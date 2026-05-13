// Package anthropic implements an llm.Provider backed by the official
// anthropic-sdk-go. Streaming + prompt caching are both wired in by
// default — the system prompt is sent as a cache_control=ephemeral
// text block so repeated calls within ~5 min benefit from the prefix
// cache.
package anthropic

import (
	"context"
	"errors"
	"fmt"
	"iter"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/turborg/turborg/internal/llm"
)

// DefaultModel matches the Python implementation's default. Override
// via Settings.Model or per-call WithModel.
const DefaultModel = "claude-sonnet-4-6"

// DefaultMaxTokens matches the Python max_tokens=4096 default.
const DefaultMaxTokens = 4096

// Settings configures an Anthropic provider. APIKey is required;
// everything else has a sensible default.
type Settings struct {
	APIKey            string
	Model             string
	MaxTokens         int
	SystemPrompt      string
	CacheSystemPrompt bool
	BaseURL           string // for tests; empty = SDK default
}

// Provider is an Anthropic-backed llm.Provider.
type Provider struct {
	client            *sdk.Client
	model             string
	maxTokens         int
	systemPrompt      string
	cacheSystemPrompt bool
}

// New builds a provider from Settings. Returns an error when APIKey is
// empty — fail-fast at construction so misconfigured deployments
// surface before the first prompt.
func New(s Settings) (*Provider, error) {
	if s.APIKey == "" {
		return nil, errors.New("anthropic: APIKey is empty")
	}
	model := s.Model
	if model == "" {
		model = DefaultModel
	}
	maxTokens := s.MaxTokens
	if maxTokens <= 0 {
		maxTokens = DefaultMaxTokens
	}

	opts := []option.RequestOption{option.WithAPIKey(s.APIKey)}
	if s.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(s.BaseURL))
	}
	client := sdk.NewClient(opts...)

	return &Provider{
		client:            &client,
		model:             model,
		maxTokens:         maxTokens,
		systemPrompt:      s.SystemPrompt,
		cacheSystemPrompt: s.CacheSystemPrompt,
	}, nil
}

func (p *Provider) Model() string { return p.model }

// Ask sends prompt to Claude and returns the assembled text response.
// Streams under the hood so large outputs don't time out the request.
func (p *Provider) Ask(ctx context.Context, prompt string, opts ...llm.CallOption) (string, error) {
	params := p.buildParams(prompt, opts)
	stream := p.client.Messages.NewStreaming(ctx, params)

	var message sdk.Message
	for stream.Next() {
		if err := message.Accumulate(stream.Current()); err != nil {
			return "", fmt.Errorf("anthropic accumulate: %w", err)
		}
	}
	if err := stream.Err(); err != nil {
		return "", fmt.Errorf("anthropic stream: %w", err)
	}
	return joinText(message), nil
}

// Stream yields text deltas as iter.Seq2[string, error]. The error is
// non-nil only on the final iteration after a fault; clean completion
// yields no extra (chunk, nil) pair after the last delta.
func (p *Provider) Stream(ctx context.Context, prompt string, opts ...llm.CallOption) iter.Seq2[string, error] {
	params := p.buildParams(prompt, opts)
	return func(yield func(string, error) bool) {
		stream := p.client.Messages.NewStreaming(ctx, params)
		for stream.Next() {
			event := stream.Current()
			delta := event.AsContentBlockDelta()
			if delta.Delta.Type != "text_delta" {
				continue
			}
			chunk := delta.Delta.AsTextDelta().Text
			if chunk == "" {
				continue
			}
			if !yield(chunk, nil) {
				return
			}
		}
		if err := stream.Err(); err != nil {
			yield("", fmt.Errorf("anthropic stream: %w", err))
		}
	}
}

func (p *Provider) buildParams(prompt string, opts []llm.CallOption) sdk.MessageNewParams {
	co := llm.ApplyOptions(opts)

	model := co.Model
	if model == "" {
		model = p.model
	}
	maxTokens := co.MaxTokens
	if maxTokens <= 0 {
		maxTokens = p.maxTokens
	}
	system := co.System
	if system == "" {
		system = p.systemPrompt
	}

	params := sdk.MessageNewParams{
		Model:     sdk.Model(model),
		MaxTokens: int64(maxTokens),
		Messages: []sdk.MessageParam{
			sdk.NewUserMessage(sdk.NewTextBlock(prompt)),
		},
	}
	if system != "" {
		params.System = SystemBlocks(system, p.cacheSystemPrompt)
	}
	return params
}

// joinText concatenates the text of every text-type content block in a
// completed Message.
func joinText(m sdk.Message) string {
	var out string
	for _, block := range m.Content {
		if block.Type == "text" {
			out += block.Text
		}
	}
	return out
}
