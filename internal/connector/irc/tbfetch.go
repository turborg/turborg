package irc

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	// tbFetchTimeout bounds the whole fetch — connect, TLS, headers, and
	// body read all share this deadline.
	tbFetchTimeout = 10 * time.Second
	// tbFetchMaxBodyBytes caps how many bytes we read off the wire. The
	// reader is hard-limited, so a server that streams forever (or lies in
	// Content-Length) can't exhaust memory.
	tbFetchMaxBodyBytes = 512 * 1024 // 512 KiB
	// tbFetchMaxRedirects bounds redirect chains; each hop is re-validated
	// by the dialer and the scheme check below.
	tbFetchMaxRedirects = 5
)

// errBlockedAddress is the sentinel returned when a URL resolves to an
// address the fetcher must not connect to. Kept distinct so callers can
// render one safe, non-leaky message regardless of which range tripped it,
// and so it survives the wrapping http.Client.Do applies to dial errors.
var errBlockedAddress = errors.New("destination address is not permitted")

// blockedCIDRs are ranges the URL fetcher must refuse that Go's standard
// IP classifiers (IsPrivate/IsLoopback/…) do not cover. Parsed once at init.
//
//   - 100.64.0.0/10   carrier-grade NAT (RFC 6598) — routes to internal infra
//     in many cloud/NAT topologies
//   - 192.0.0.0/24    IETF protocol assignments (RFC 6890)
//   - 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24  TEST-NET-1/2/3
//   - 198.18.0.0/15   benchmarking (RFC 2544)
//   - 192.88.99.0/24  6to4 relay anycast (deprecated)
//   - 240.0.0.0/4     reserved / former Class E
//   - 64:ff9b::/96    NAT64 well-known prefix (RFC 6052) — embeds an IPv4
//     target the v4 rules below would otherwise never see
var blockedCIDRs = func() []*net.IPNet {
	raw := []string{
		"100.64.0.0/10",
		"192.0.0.0/24",
		"192.0.2.0/24",
		"198.51.100.0/24",
		"203.0.113.0/24",
		"198.18.0.0/15",
		"192.88.99.0/24",
		"240.0.0.0/4",
		"64:ff9b::/96",
	}
	out := make([]*net.IPNet, 0, len(raw))
	for _, c := range raw {
		if _, n, err := net.ParseCIDR(c); err == nil {
			out = append(out, n)
		}
	}
	return out
}()

// isBlockedIP reports whether ip falls in a range the URL fetcher must
// refuse to connect to. This is the heart of the SSRF guard: it is applied
// to every IP returned by DNS, and the connection is then pinned to a vetted
// IP (see safeDialContext), so a hostname that re-resolves to a fresh
// address between the check and the dial — DNS rebinding — can't smuggle an
// internal target past it.
//
// Covered: loopback (127/8, ::1), RFC1918 + unique-local (10/8, 172.16/12,
// 192.168/16, fc00::/7), link-local unicast/multicast (169.254/16, fe80::/10)
// — which includes the cloud metadata endpoint 169.254.169.254 — any
// multicast, interface-local multicast, the unspecified address (0.0.0.0, ::),
// and the extra reserved/CGNAT/NAT64 ranges in blockedCIDRs. A nil IP is
// treated as blocked.
func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	// Check the explicit CIDRs against the address as given, before any v4
	// normalization, so the IPv6 NAT64 prefix is matched on its v6 form.
	for _, n := range blockedCIDRs {
		if n.Contains(ip) {
			return true
		}
	}
	// Normalize IPv4-in-IPv6 so e.g. ::ffff:127.0.0.1 is judged by the
	// IPv4 rules rather than slipping through as a "global" v6 address.
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified()
}

// contentTagPattern matches the <content> / </content> fence (and loose
// whitespace variants) used to wrap untrusted page text in the LLM prompt.
var contentTagPattern = regexp.MustCompile(`(?i)<\s*/?\s*content\s*>`)

// neutralizeContentDelimiters defangs any <content>/</content> markers found
// inside fetched text, so a page can't forge the fence and "break out" of the
// data section of the prompt — a delimiter-based prompt-injection attack. For
// text/html these are already gone (stripHTMLToText drops all tags), but
// text/plain pages pass through verbatim, so this closes that path uniformly.
func neutralizeContentDelimiters(s string) string {
	return contentTagPattern.ReplaceAllString(s, "[content]")
}

// safeDialContext builds a DialContext that resolves the target host,
// rejects the connection if *any* resolved IP is blocked, and then dials a
// vetted IP literal directly. Dialing the literal — rather than handing the
// hostname back to the OS resolver — is what closes the DNS-rebinding
// window: the address we vetted is byte-for-byte the address we connect to.
func safeDialContext(dialer *net.Dialer, blocked func(net.IP) bool) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, errBlockedAddress
		}
		for _, ipa := range ips {
			if blocked(ipa.IP) {
				return nil, errBlockedAddress
			}
		}
		// Every resolved IP vetted clean — pin the connection to the first.
		return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}
}

// fetchURLForTLDR retrieves rawURL over an SSRF-guarded HTTP client and
// returns the response body as text. It enforces, in order: http/https only;
// post-resolution IP vetting on every connection (including redirect hops);
// a 10s overall timeout; a text/html-or-text/plain Content-Type; and a
// 512 KiB body cap. Errors are phrased for the requesting user and never
// echo internal addressing detail beyond the generic "not permitted" notice.
func fetchURLForTLDR(ctx context.Context, rawURL string) (string, error) {
	return fetchURL(ctx, rawURL, isBlockedIP)
}

// fetchURL is the testable core of fetchURLForTLDR. blocked is the per-IP
// guard applied after DNS resolution; production passes isBlockedIP, tests
// pass a permissive guard so they can exercise the fetch path against a
// loopback test server (which the real guard would, correctly, refuse).
func fetchURL(ctx context.Context, rawURL string, blocked func(net.IP) bool) (string, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" {
		return "", errors.New("invalid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("only http and https URLs are supported")
	}

	dialer := &net.Dialer{Timeout: tbFetchTimeout}
	transport := &http.Transport{
		DialContext:           safeDialContext(dialer, blocked),
		TLSHandshakeTimeout:   tbFetchTimeout,
		ResponseHeaderTimeout: tbFetchTimeout,
		ExpectContinueTimeout: time.Second,
		DisableKeepAlives:     true,
		// No proxy: a configured proxy would terminate the connection
		// itself, bypassing the per-IP dial guard entirely.
		Proxy: nil,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   tbFetchTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= tbFetchMaxRedirects {
				return errors.New("too many redirects")
			}
			// Re-validate the scheme on every hop. The IP guard re-runs
			// for free because each redirect opens a fresh dial through
			// safeDialContext.
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("redirect to unsupported scheme %q", req.URL.Scheme)
			}
			return nil
		},
	}

	ctx, cancel := context.WithTimeout(ctx, tbFetchTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", errors.New("invalid URL")
	}
	req.Header.Set("User-Agent", "turborg-tldr/1.0")
	req.Header.Set("Accept", "text/html, text/plain")

	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, errBlockedAddress) {
			return "", errBlockedAddress
		}
		return "", errors.New("could not fetch URL")
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("fetch returned HTTP %d", resp.StatusCode)
	}

	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mediaType != "text/html" && mediaType != "text/plain" {
		return "", errors.New("only text/html and text/plain pages can be summarized")
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, tbFetchMaxBodyBytes))
	if err != nil {
		return "", errors.New("could not read response body")
	}

	text := string(body)
	if mediaType == "text/html" {
		text = buildHTMLText(text)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return "", errors.New("page had no readable text content")
	}
	return text, nil
}

// pageMetadata holds the human-meaningful header fields lifted from an HTML
// document's <head>: a title and a description. Either may be empty.
type pageMetadata struct {
	title       string
	description string
}

var (
	titleTagPattern = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
	metaTagPattern  = regexp.MustCompile(`(?is)<meta\b[^>]*>`)
	// attrPattern pulls name="value" / name='value' / name=value attribute
	// pairs out of a single tag, independent of their order.
	attrPattern = regexp.MustCompile(`(?is)([a-z][a-z0-9:_-]*)\s*=\s*("([^"]*)"|'([^']*)'|([^\s"'>]+))`)
)

// extractMetadata reads the <title> and the OpenGraph / Twitter-card / plain
// <meta name="description"> fields from HTML. Preference order: og: > twitter:
// > the bare tag. All regexes are RE2 (linear-time) and run against the
// already size-capped body, so there's no backtracking-DoS exposure.
func extractMetadata(htmlStr string) pageMetadata {
	var md pageMetadata
	if m := titleTagPattern.FindStringSubmatch(htmlStr); m != nil {
		md.title = cleanMeta(m[1])
	}

	var ogTitle, twTitle, ogDesc, twDesc, nameDesc string
	for _, tag := range metaTagPattern.FindAllString(htmlStr, -1) {
		key, content := metaKeyContent(tag)
		switch key {
		case "og:title":
			ogTitle = content
		case "twitter:title":
			twTitle = content
		case "og:description":
			ogDesc = content
		case "twitter:description":
			twDesc = content
		case "description":
			nameDesc = content
		}
	}
	if t := firstNonEmpty(ogTitle, twTitle); t != "" {
		md.title = t // a card title beats the raw <title> when present
	}
	md.description = firstNonEmpty(ogDesc, twDesc, nameDesc)
	return md
}

// metaKeyContent parses one <meta> tag into its identifying key (property=
// preferred over name=, lowercased) and its content= value.
func metaKeyContent(tag string) (key, content string) {
	var prop, name, ct string
	for _, a := range attrPattern.FindAllStringSubmatch(tag, -1) {
		val := firstNonEmpty(a[3], a[4], a[5])
		switch strings.ToLower(a[1]) {
		case "property":
			prop = strings.ToLower(val)
		case "name":
			name = strings.ToLower(val)
		case "content":
			ct = val
		}
	}
	return firstNonEmpty(prop, name), cleanMeta(ct)
}

// cleanMeta unescapes HTML entities and collapses whitespace in a metadata
// value. html.UnescapeString is stdlib and never executes anything.
func cleanMeta(s string) string {
	return collapseWhitespace(html.UnescapeString(s))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// buildHTMLText renders an HTML document to summarizable text: a short header
// carrying the page's title/description metadata, followed by the stripped
// body prose. The header is what lets SPA/JS pages (YouTube, X, Reddit, …)
// whose visible text is script-rendered still yield something to summarize —
// their og:/twitter: tags survive in the initial HTML even when the body does
// not. Returns "" only when neither metadata nor body prose is present.
func buildHTMLText(htmlStr string) string {
	md := extractMetadata(htmlStr)
	body := stripHTMLToText(htmlStr)

	var b strings.Builder
	if md.title != "" {
		fmt.Fprintf(&b, "Title: %s\n", md.title)
	}
	if md.description != "" {
		fmt.Fprintf(&b, "Description: %s\n", md.description)
	}
	if body != "" {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(body)
	}
	return b.String()
}

// stripHTMLToText produces a rough plain-text rendering of HTML: it drops the
// contents of <script> and <style> blocks, replaces every remaining tag with
// a space, and collapses runs of whitespace. It is deliberately simple and is
// NOT a sanitizer — its only job is to cut markup noise (and token cost)
// before the content reaches the LLM. The text is still treated strictly as
// untrusted data by the TL;DR prompt, never as instructions.
func stripHTMLToText(s string) string {
	s = removeHTMLBlock(s, "script")
	s = removeHTMLBlock(s, "style")

	var b strings.Builder
	b.Grow(len(s))
	inTag := false
	for _, r := range s {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			if inTag {
				inTag = false
				b.WriteByte(' ')
			}
		case !inTag:
			b.WriteRune(r)
		}
	}
	return collapseWhitespace(b.String())
}

// removeHTMLBlock deletes <tag ...> ... </tag> spans (case-insensitive),
// including the open and close tags. An unclosed block is truncated to the
// open tag — everything from the open tag to end-of-string is dropped, which
// is the safe choice for script/style noise.
func removeHTMLBlock(s, tag string) string {
	lower := strings.ToLower(s)
	open := "<" + tag
	closeTag := "</" + tag + ">"
	var b strings.Builder
	for {
		i := strings.Index(lower, open)
		if i < 0 {
			b.WriteString(s)
			break
		}
		b.WriteString(s[:i])
		rest := lower[i:]
		j := strings.Index(rest, closeTag)
		if j < 0 {
			// No closing tag — drop the remainder entirely.
			break
		}
		cut := i + j + len(closeTag)
		s = s[cut:]
		lower = lower[cut:]
	}
	return b.String()
}

// collapseWhitespace trims and squeezes any run of whitespace down to a
// single space, so HTML indentation doesn't bloat the token count.
func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// clampContent truncates s to at most n runes, appending an ellipsis marker
// when it cuts. Bounds the LLM input regardless of page size.
func clampContent(s string, n int) string {
	if n <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + " …[truncated]"
}
