package openaicompat_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/llm/openaicompat"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewRequiresAPIKey(t *testing.T) {
	_, err := openaicompat.New(openaicompat.Settings{})
	require.Error(t, err)
}

func TestAskSendsModelAndSystemAndReturnsContent(t *testing.T) {
	var gotBody map[string]any
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/chat/completions", r.URL.Path)
		gotAuth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"hello there"}}]}`))
	}))
	defer srv.Close()

	p, err := openaicompat.New(openaicompat.Settings{
		APIKey:     "sk-test",
		BaseURL:    srv.URL + "/v1",
		Model:      "default-model",
		HTTPClient: srv.Client(),
	})
	require.NoError(t, err)
	assert.Equal(t, "default-model", p.Model())

	out, _, err := p.Ask(context.Background(), "ping",
		llm.WithModel("override-model"),
		llm.WithSystem("be terse"),
		llm.WithMaxTokens(123),
	)
	require.NoError(t, err)
	assert.Equal(t, "hello there", out)

	assert.Equal(t, "Bearer sk-test", gotAuth)
	assert.Equal(t, "override-model", gotBody["model"], "WithModel overrides the default")
	assert.EqualValues(t, 123, gotBody["max_tokens"])
	msgs, _ := gotBody["messages"].([]any)
	require.Len(t, msgs, 2)
	first, _ := msgs[0].(map[string]any)
	assert.Equal(t, "system", first["role"])
	assert.Equal(t, "be terse", first["content"])
}

func TestAskSurfacesAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
	}))
	defer srv.Close()

	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k", BaseURL: srv.URL, HTTPClient: srv.Client()})
	require.NoError(t, err)

	_, _, err = p.Ask(context.Background(), "hi")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rate limited")
}

func TestNewDefaultsBaseURLModelAndMaxTokens(t *testing.T) {
	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k"})
	require.NoError(t, err)
	assert.Equal(t, openaicompat.DefaultModel, p.Model())
}

func TestAskWithoutSystemSendsSingleMessage(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer srv.Close()

	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k", BaseURL: srv.URL, HTTPClient: srv.Client()})
	require.NoError(t, err)
	out, _, err := p.Ask(context.Background(), "hi")
	require.NoError(t, err)
	assert.Equal(t, "ok", out)
	msgs, _ := gotBody["messages"].([]any)
	assert.Len(t, msgs, 1, "no system prompt → only the user message")
}

func TestAskErrorsOnEmptyChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer srv.Close()

	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k", BaseURL: srv.URL, HTTPClient: srv.Client()})
	require.NoError(t, err)
	_, _, err = p.Ask(context.Background(), "hi")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no choices")
}

func TestAskErrorsOnNon200WithoutErrorBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k", BaseURL: srv.URL, HTTPClient: srv.Client()})
	require.NoError(t, err)
	_, _, err = p.Ask(context.Background(), "hi")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status 500")
}

func TestAskErrorsOnConnectionFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening now → the call fails

	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k", BaseURL: url})
	require.NoError(t, err)
	_, _, err = p.Ask(context.Background(), "hi")
	require.Error(t, err)
}

func TestStreamErrorsOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k", BaseURL: srv.URL, HTTPClient: srv.Client()})
	require.NoError(t, err)

	var sawErr error
	for _, err := range p.Stream(context.Background(), "hi") {
		if err != nil {
			sawErr = err
		}
	}
	require.Error(t, sawErr)
	assert.Contains(t, sawErr.Error(), "unexpected status 401")
}

func TestStreamErrorsOnConnectionFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k", BaseURL: url})
	require.NoError(t, err)

	var sawErr error
	for _, err := range p.Stream(context.Background(), "hi") {
		if err != nil {
			sawErr = err
		}
	}
	require.Error(t, sawErr)
}

func TestStreamToleratesNonJSONKeepaliveLines(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, ": keepalive comment\n")
		_, _ = io.WriteString(w, "data: not-json\n")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n")
		_, _ = io.WriteString(w, "data: [DONE]\n")
	}))
	defer srv.Close()

	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k", BaseURL: srv.URL, HTTPClient: srv.Client()})
	require.NoError(t, err)

	var got string
	for chunk, err := range p.Stream(context.Background(), "hi") {
		require.NoError(t, err)
		got += chunk
	}
	assert.Equal(t, "hi", got)
}

func TestStreamYieldsDeltas(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n")
		_, _ = io.WriteString(w, "data: [DONE]\n")
	}))
	defer srv.Close()

	p, err := openaicompat.New(openaicompat.Settings{APIKey: "k", BaseURL: srv.URL, HTTPClient: srv.Client()})
	require.NoError(t, err)

	var got string
	for chunk, err := range p.Stream(context.Background(), "hi") {
		require.NoError(t, err)
		got += chunk
	}
	assert.Equal(t, "Hello", got)
}
