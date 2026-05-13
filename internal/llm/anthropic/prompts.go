package anthropic

import (
	sdk "github.com/anthropics/anthropic-sdk-go"
)

// SystemBlocks wraps text as the structured "system" argument expected
// by the Anthropic Messages API. When cache is true (the default), the
// block carries cache_control={"type": "ephemeral"} so the system
// prefix is cached across calls (~5 minute TTL).
//
// Cache-control is a *prefix-match* mechanism on Anthropic's side: any
// byte change anywhere in the prefix invalidates everything after it.
// The system prompt is the single biggest cacheable prefix in a typical
// turborg call, so build it deterministically and cache aggressively.
//
// Disable caching only for short, volatile prompts where the cache
// write would never amortize.
func SystemBlocks(text string, cache bool) []sdk.TextBlockParam {
	block := sdk.TextBlockParam{Text: text}
	if cache {
		block.CacheControl = sdk.NewCacheControlEphemeralParam()
	}
	return []sdk.TextBlockParam{block}
}
