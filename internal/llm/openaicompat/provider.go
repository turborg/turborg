// Package openaicompat implements an llm.Provider against any service
// that speaks the OpenAI Chat Completions API — OpenAI itself, OpenRouter,
// DeepInfra, Together, a self-hosted LiteLLM proxy, etc. The base URL and
// model are operator-supplied so a single provider routes to whichever
// backend the deployment points at.
//
// Only the stdlib net/http is used (no vendor SDK): the request/response
// shapes are small and stable, and avoiding a heavyweight dependency keeps
// the provider portable across the many compatible backends.
package openaicompat

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/llm"
)

// DefaultBaseURL is the canonical OpenAI Chat Completions endpoint root.
// Override via Settings.BaseURL to target a compatible backend.
const DefaultBaseURL = "https://api.openai.com/v1"

// DefaultModel is used when neither Settings.Model nor a per-call
// WithModel is set.
const DefaultModel = "gpt-4o-mini"

// DefaultMaxTokens caps response length absent a per-call WithMaxTokens.
const DefaultMaxTokens = 4096

// Settings configures an OpenAI-compatible provider. APIKey is required;
// the rest default sensibly.
type Settings struct {
	APIKey    string
	BaseURL   string // e.g. https://openrouter.ai/api/v1 ; empty = OpenAI
	Model     string
	MaxTokens int
	// HTTPClient lets tests inject a client pointed at httptest. Nil → a
	// client with a sane request timeout.
	HTTPClient *http.Client
}

// Provider is an OpenAI-Chat-Completions-backed llm.Provider.
type Provider struct {
	client    *http.Client
	baseURL   string
	apiKey    string
	model     string
	maxTokens int
}

// New builds a provider from Settings. Returns an error when APIKey is
// empty — fail-fast at construction so a misconfigured deployment surfaces
// before the first prompt.
func New(s Settings) (*Provider, error) {
	if s.APIKey == "" {
		return nil, fmt.Errorf("openaicompat: APIKey is empty")
	}
	base := strings.TrimRight(s.BaseURL, "/")
	if base == "" {
		base = DefaultBaseURL
	}
	model := s.Model
	if model == "" {
		model = DefaultModel
	}
	maxTokens := s.MaxTokens
	if maxTokens <= 0 {
		maxTokens = DefaultMaxTokens
	}
	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &Provider{
		client:    client,
		baseURL:   base,
		apiKey:    s.APIKey,
		model:     model,
		maxTokens: maxTokens,
	}, nil
}

func (p *Provider) Model() string { return p.model }

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	MaxTokens int           `json:"max_tokens,omitempty"`
	Stream    bool          `json:"stream,omitempty"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

func (p *Provider) buildRequest(prompt string, opts []llm.CallOption) chatRequest {
	co := llm.ApplyOptions(opts)
	model := co.Model
	if model == "" {
		model = p.model
	}
	maxTokens := co.MaxTokens
	if maxTokens <= 0 {
		maxTokens = p.maxTokens
	}
	msgs := make([]chatMessage, 0, 2)
	if co.System != "" {
		msgs = append(msgs, chatMessage{Role: "system", Content: co.System})
	}
	msgs = append(msgs, chatMessage{Role: "user", Content: prompt})
	return chatRequest{Model: model, Messages: msgs, MaxTokens: maxTokens}
}

func (p *Provider) post(ctx context.Context, body chatRequest) (*http.Response, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openaicompat marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("openaicompat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("openaicompat call: %w", err)
	}
	return resp, nil
}

// Ask sends prompt and returns the assembled (non-streamed) text response.
func (p *Provider) Ask(ctx context.Context, prompt string, opts ...llm.CallOption) (string, llm.Usage, error) {
	resp, err := p.post(ctx, p.buildRequest(prompt, opts))
	if err != nil {
		return "", llm.Usage{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	var decoded chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", llm.Usage{}, fmt.Errorf("openaicompat decode (status %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		if decoded.Error != nil && decoded.Error.Message != "" {
			return "", llm.Usage{}, fmt.Errorf("openaicompat: %s (status %d)", decoded.Error.Message, resp.StatusCode)
		}
		return "", llm.Usage{}, fmt.Errorf("openaicompat: unexpected status %d", resp.StatusCode)
	}
	if len(decoded.Choices) == 0 {
		return "", llm.Usage{}, fmt.Errorf("openaicompat: no choices in response")
	}
	var usage llm.Usage
	if decoded.Usage != nil {
		usage.InputTokens = decoded.Usage.PromptTokens
		usage.OutputTokens = decoded.Usage.CompletionTokens
	}
	return decoded.Choices[0].Message.Content, usage, nil
}

// streamChunk is one server-sent delta in a streamed completion.
type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

// Stream yields text deltas as iter.Seq2[string, error]. It uses the
// backend's SSE stream (data: lines, terminated by "data: [DONE]"). The
// error is non-nil only on the final iteration after a fault.
func (p *Provider) Stream(ctx context.Context, prompt string, opts ...llm.CallOption) iter.Seq2[string, error] {
	body := p.buildRequest(prompt, opts)
	body.Stream = true
	return func(yield func(string, error) bool) {
		resp, err := p.post(ctx, body)
		if err != nil {
			yield("", err)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			yield("", fmt.Errorf("openaicompat: unexpected status %d", resp.StatusCode))
			return
		}
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			data, ok := strings.CutPrefix(line, "data:")
			if !ok {
				continue
			}
			data = strings.TrimSpace(data)
			if data == "[DONE]" {
				return
			}
			var chunk streamChunk
			if err := json.Unmarshal([]byte(data), &chunk); err != nil {
				continue // tolerate keep-alive / non-JSON lines
			}
			for _, c := range chunk.Choices {
				if c.Delta.Content == "" {
					continue
				}
				if !yield(c.Delta.Content, nil) {
					return
				}
			}
		}
		if err := scanner.Err(); err != nil {
			yield("", fmt.Errorf("openaicompat stream: %w", err))
		}
	}
}
