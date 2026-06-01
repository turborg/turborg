package irc

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// allowAll is a permissive IP guard used by fetch-path tests so they can
// target a loopback httptest server (which isBlockedIP would, correctly,
// refuse).
func allowAll(net.IP) bool { return false }

func TestIsBlockedIP(t *testing.T) {
	cases := []struct {
		ip      string
		blocked bool
	}{
		{"127.0.0.1", true},             // loopback v4
		{"::1", true},                   // loopback v6
		{"10.0.0.5", true},              // RFC1918
		{"172.16.4.4", true},            // RFC1918
		{"192.168.1.1", true},           // RFC1918
		{"169.254.10.10", true},         // link-local
		{"fe80::1", true},               // link-local v6
		{"fc00::1", true},               // unique-local v6
		{"0.0.0.0", true},               // unspecified
		{"::", true},                    // unspecified v6
		{"224.0.0.1", true},             // multicast
		{"::ffff:127.0.0.1", true},      // v4-mapped loopback must not slip through
		{"169.254.169.254", true},       // cloud metadata endpoint (link-local)
		{"100.64.0.1", true},            // CGNAT (RFC 6598)
		{"198.18.0.1", true},            // benchmarking range
		{"192.0.2.1", true},             // TEST-NET-1
		{"203.0.113.5", true},           // TEST-NET-3
		{"240.0.0.1", true},             // reserved / Class E
		{"64:ff9b::a9fe:a9fe", true},    // NAT64-embedded 169.254.169.254
		{"8.8.8.8", false},              // public
		{"1.1.1.1", false},              // public
		{"2606:4700:4700::1111", false}, // public v6
	}
	for _, tc := range cases {
		ip := net.ParseIP(tc.ip)
		require.NotNil(t, ip, "parse %s", tc.ip)
		assert.Equalf(t, tc.blocked, isBlockedIP(ip), "isBlockedIP(%s)", tc.ip)
	}
	assert.True(t, isBlockedIP(nil), "nil IP must be treated as blocked")
}

func TestFetchURLRejectsNonHTTPScheme(t *testing.T) {
	for _, raw := range []string{"ftp://example.com/x", "file:///etc/passwd", "gopher://x", "  "} {
		_, err := fetchURLForTLDR(context.Background(), raw)
		require.Error(t, err, raw)
	}
}

func TestFetchURLBlocksLoopback(t *testing.T) {
	// The real guard (isBlockedIP) must refuse a loopback target — this is
	// the SSRF protection exercised end-to-end against a local server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<p>secret</p>"))
	}))
	defer srv.Close()

	_, err := fetchURLForTLDR(context.Background(), srv.URL)
	require.Error(t, err)
	assert.ErrorIs(t, err, errBlockedAddress)
}

func TestFetchURLSuccessHTML(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<html><head><style>x{}</style></head><body><h1>Title</h1><script>evil()</script><p>Hello   world</p></body></html>"))
	}))
	defer srv.Close()

	got, err := fetchURL(context.Background(), srv.URL, allowAll)
	require.NoError(t, err)
	// Script/style stripped, tags removed, whitespace collapsed.
	assert.Equal(t, "Title Hello world", got)
	assert.NotContains(t, got, "evil")
	assert.NotContains(t, got, "x{}")
}

func TestFetchURLSuccessPlainText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("just some text"))
	}))
	defer srv.Close()

	got, err := fetchURL(context.Background(), srv.URL, allowAll)
	require.NoError(t, err)
	assert.Equal(t, "just some text", got)
}

func TestFetchURLRejectsUnsupportedContentType(t *testing.T) {
	for _, ct := range []string{"application/json", "image/png"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", ct)
			_, _ = w.Write([]byte("{}"))
		}))
		_, err := fetchURL(context.Background(), srv.URL, allowAll)
		srv.Close()
		require.Errorf(t, err, "content-type %q", ct)
		assert.Contains(t, err.Error(), "text/html")
	}
}

func TestFetchURLRejectsNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := fetchURL(context.Background(), srv.URL, allowAll)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
}

func TestFetchURLBodyCap(t *testing.T) {
	// Serve more than the cap; the reader must stop at tbFetchMaxBodyBytes.
	big := strings.Repeat("a", tbFetchMaxBodyBytes+5000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(big))
	}))
	defer srv.Close()

	got, err := fetchURL(context.Background(), srv.URL, allowAll)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(got), tbFetchMaxBodyBytes)
}

func TestFetchURLRejectsEmptyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("   <html></html>   "))
	}))
	defer srv.Close()

	_, err := fetchURL(context.Background(), srv.URL, allowAll)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no readable")
}

func TestFetchURLRedirectToBadSchemeRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "ftp://evil.example/x", http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	_, err := fetchURL(context.Background(), srv.URL+"/start", allowAll)
	require.Error(t, err)
}

func TestExtractMetadata(t *testing.T) {
	htmlDoc := `<html><head>
		<title>Raw Title</title>
		<meta property="og:title" content="OG Title &amp; More">
		<meta name="description" content='Plain   desc'>
		<meta content="OG Desc" property="og:description">
	</head><body>x</body></html>`
	md := extractMetadata(htmlDoc)
	// og:title overrides the <title> tag; entities decoded; whitespace collapsed.
	assert.Equal(t, "OG Title & More", md.title)
	// og:description wins over name=description, regardless of attribute order.
	assert.Equal(t, "OG Desc", md.description)
}

func TestExtractMetadataFallsBackToTitleTag(t *testing.T) {
	md := extractMetadata(`<head><title>Only Title</title></head>`)
	assert.Equal(t, "Only Title", md.title)
	assert.Empty(t, md.description)
}

func TestExtractMetadataTwitterCard(t *testing.T) {
	md := extractMetadata(`<meta name="twitter:title" content="Tw"><meta name="twitter:description" content="TwDesc">`)
	assert.Equal(t, "Tw", md.title)
	assert.Equal(t, "TwDesc", md.description)
}

func TestBuildHTMLText(t *testing.T) {
	// Metadata only (script-rendered body, like a SPA): still produces text.
	got := buildHTMLText(`<head><meta property="og:title" content="T"><meta property="og:description" content="D"></head><body><script>app()</script></body>`)
	assert.Contains(t, got, "Title: T")
	assert.Contains(t, got, "Description: D")
	// Body only (no metadata): just the prose.
	assert.Equal(t, "hello world", buildHTMLText(`<body><p>hello world</p></body>`))
	// Nothing extractable.
	assert.Equal(t, "", buildHTMLText(`<html><head></head><body></body></html>`))
}

func TestFetchURLSPAUsesMetadata(t *testing.T) {
	// Simulates a YouTube-style page: visible content lives in <script>, but
	// OpenGraph tags carry the title/description. The fetch must surface them
	// instead of failing with "no readable text content".
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><head>
			<meta property="og:title" content="Cool Video">
			<meta property="og:description" content="A summary of the video.">
		</head><body><script>var ytInitialData = {};</script></body></html>`))
	}))
	defer srv.Close()

	got, err := fetchURL(context.Background(), srv.URL, allowAll)
	require.NoError(t, err)
	assert.Contains(t, got, "Cool Video")
	assert.Contains(t, got, "A summary of the video.")
	assert.NotContains(t, got, "ytInitialData") // script body still stripped
}

func TestStripHTMLToText(t *testing.T) {
	assert.Equal(t, "a b c", stripHTMLToText("<div> a <b>b</b>\n  c </div>"))
	assert.Equal(t, "keep", stripHTMLToText("<script>drop()</script>keep"))
	assert.Equal(t, "before", stripHTMLToText("before<script>unclosed")) // unclosed block dropped to EOF
	assert.Equal(t, "x y", stripHTMLToText("x<style>.a{color:red}</style> y"))
}

func TestNeutralizeContentDelimiters(t *testing.T) {
	assert.Equal(t, "a [content] b", neutralizeContentDelimiters("a </content> b"))
	assert.Equal(t, "[content]x[content]", neutralizeContentDelimiters("<content>x</content>"))
	// Loose whitespace / case variants are still caught.
	assert.Equal(t, "[content]", neutralizeContentDelimiters("< / CONTENT >"))
	assert.Equal(t, "no tags here", neutralizeContentDelimiters("no tags here"))
}

func TestClampContent(t *testing.T) {
	assert.Equal(t, "", clampContent("anything", 0))
	assert.Equal(t, "abc", clampContent("abc", 10))
	assert.True(t, strings.HasPrefix(clampContent("abcdef", 3), "abc"))
	assert.Contains(t, clampContent("abcdef", 3), "truncated")
}

func TestFetchURLInvalidURL(t *testing.T) {
	_, err := fetchURLForTLDR(context.Background(), "http://")
	require.Error(t, err)
	_, err = fetchURLForTLDR(context.Background(), "://nohost")
	require.Error(t, err)
}
