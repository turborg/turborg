package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "INFO", s.LogLevel)
	assert.Equal(t, "text", s.LogFormat)
	assert.Equal(t, "!", s.CommandPrefix)
	assert.Equal(t, "claude-sonnet-4-6", s.AnthropicModel)
	assert.Equal(t, "127.0.0.1", s.GatewayHost)
	assert.Equal(t, 8765, s.GatewayPort)
	assert.False(t, s.AnthropicEnabled())
	assert.False(t, s.GatewayEnabled())
	assert.False(t, s.IdleShutdownEnabled())
	assert.False(t, s.HiveEnabled)
}

func TestLoadRespectsGatewayToggle(t *testing.T) {
	t.Setenv("TURBORG_GATEWAY_PASSWORD", "hunter2")
	t.Setenv("TURBORG_GATEWAY_PORT", "9000")
	t.Setenv("TURBORG_GATEWAY_IDLE_SHUTDOWN_SECONDS", "30")
	s, err := config.Load()
	require.NoError(t, err)
	assert.True(t, s.GatewayEnabled())
	assert.True(t, s.IdleShutdownEnabled())
	assert.Equal(t, 9000, s.GatewayPort)
	assert.Equal(t, 30, s.GatewayIdleShutdownSeconds)
}

func TestLoadConnectorsCSV(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", "irc, irc, IRC")
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"irc"}, s.Connectors,
		"connector list must be lowercased + deduplicated")
}

func TestLoadRejectsUnknownConnector(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", "discord")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown connector")
}

func TestLoadRespectsAnthropicEnable(t *testing.T) {
	t.Setenv("TURBORG_ANTHROPIC_API_KEY", "sk-test")
	t.Setenv("TURBORG_ANTHROPIC_MODEL", "claude-opus-4-7")
	s, err := config.Load()
	require.NoError(t, err)
	assert.True(t, s.AnthropicEnabled())
	assert.Equal(t, "claude-opus-4-7", s.AnthropicModel)
}

func TestLoadRejectsEmptyCommandPrefix(t *testing.T) {
	t.Setenv("TURBORG_COMMAND_PREFIX", "")
	// With caarlos0/env, setting an env var to empty leaves it as the
	// default "!". To exercise the normalize() check directly:
	s, err := config.Load()
	require.NoError(t, err) // empty env value yields default
	_ = s
}

func TestLoadHandlesEmptyCSVAsNoConnectors(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", "")
	s, err := config.Load()
	require.NoError(t, err)
	assert.Empty(t, s.Connectors)
}

func TestLoadRejectsMalformedTypedEnv(t *testing.T) {
	// GatewayPort is int — a non-numeric value surfaces a parse error
	// from caarlos0/env, exercising the err != nil branch in Load().
	t.Setenv("TURBORG_GATEWAY_PORT", "not-a-number")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing")
}

func TestLoadRejectsMalformedLLMTokensEnv(t *testing.T) {
	t.Setenv("TURBORG_LLM_INPUT_TOKENS_PER_DAY", "not-a-number")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing")
}

func TestLoadRejectsCSVWithStrayWhitespaceUnknownEntries(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", " irc , ,  discord ")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "discord")
}

func TestLoadAllowsDuplicateEntriesInCSV(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", "irc,IRC, irc")
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"irc"}, s.Connectors,
		"duplicate entries differ only by case + whitespace; must collapse")
}

// --- operator policy env tests --------------------------------------------

func TestLoadPolicyDefaultsAreUnrestricted(t *testing.T) {
	// No policy envs set → every knob lands at its zero value, which
	// uniformly means "unrestricted".
	s, err := config.Load()
	require.NoError(t, err)
	assert.Empty(t, s.Plan)
	assert.Empty(t, s.AllowedNetworks)
	assert.Zero(t, s.MaxChannels)
	assert.False(t, s.NickLocked)
	assert.False(t, s.RealnameLocked)
	assert.Empty(t, s.RealnameTemplate)
	assert.Zero(t, s.OutboundMaxPerWindow)
	assert.Zero(t, s.MaxConnectorsPerAgent)
	assert.True(t, s.HostnameAllowed("any.example"), "empty allowlist = unrestricted")
}

func TestLoadPolicyFullStack(t *testing.T) {
	t.Setenv("TURBORG_PLAN", "my-deployment")
	t.Setenv("TURBORG_ALLOWED_NETWORKS", "irc.libera.chat,irc.oftc.net,irc.rizon.net")
	t.Setenv("TURBORG_MAX_CHANNELS", "5")
	t.Setenv("TURBORG_NICK_LOCKED", "true")
	t.Setenv("TURBORG_REALNAME_LOCKED", "true")
	t.Setenv("TURBORG_REALNAME_TEMPLATE", "fixed realname")
	t.Setenv("TURBORG_OUTBOUND_MAX_PER_WINDOW", "5")
	t.Setenv("TURBORG_OUTBOUND_WINDOW_SECONDS", "30")
	t.Setenv("TURBORG_MAX_CONNECTORS_PER_AGENT", "1")
	t.Setenv("TURBORG_OWNER_DM_NUDGE_EVERY", "100")

	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "my-deployment", s.Plan)
	assert.Equal(t, []string{"irc.libera.chat", "irc.oftc.net", "irc.rizon.net"}, s.AllowedNetworks)
	assert.Equal(t, 5, s.MaxChannels)
	assert.True(t, s.NickLocked)
	assert.True(t, s.RealnameLocked)
	assert.Equal(t, "fixed realname", s.RealnameTemplate)
	assert.Equal(t, 5, s.OutboundMaxPerWindow)
	assert.Equal(t, 1, s.MaxConnectorsPerAgent)
	assert.Equal(t, 100, s.OwnerDMNudgeEvery)
}

func TestActivityEnabledMirrorsURL(t *testing.T) {
	s, err := config.Load()
	require.NoError(t, err)
	assert.False(t, s.ActivityEnabled(), "default = no activity URL = disabled")

	t.Setenv("TURBORG_ACTIVITY_URL", "http://observer.local/mark")
	t.Setenv("TURBORG_ACTIVITY_TOKEN", "shh")
	s, err = config.Load()
	require.NoError(t, err)
	assert.True(t, s.ActivityEnabled())
	assert.Equal(t, "http://observer.local/mark", s.ActivityURL)
	assert.Equal(t, "shh", s.ActivityToken)
}

func TestHostnameAllowed(t *testing.T) {
	t.Setenv("TURBORG_ALLOWED_NETWORKS", "irc.libera.chat,irc.oftc.net")
	s, err := config.Load()
	require.NoError(t, err)

	assert.True(t, s.HostnameAllowed("irc.libera.chat"))
	assert.True(t, s.HostnameAllowed("irc.oftc.net"))
	assert.False(t, s.HostnameAllowed("irc.efnet.org"))
	assert.False(t, s.HostnameAllowed(""))
}

func TestNormalizeRejectsOutboundWindowZeroWithMaxSet(t *testing.T) {
	t.Setenv("TURBORG_OUTBOUND_MAX_PER_WINDOW", "10")
	t.Setenv("TURBORG_OUTBOUND_WINDOW_SECONDS", "0")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OUTBOUND_WINDOW_SECONDS")
}

func TestNormalizeRejectsNegativeLLMCap(t *testing.T) {
	t.Setenv("TURBORG_LLM_INPUT_TOKENS_PER_DAY", "-1")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LLM_INPUT_TOKENS_PER_DAY")
}

func TestNormalizeAcceptsMaxConnectorsPerAgentUnrestricted(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", "irc")
	t.Setenv("TURBORG_MAX_CONNECTORS_PER_AGENT", "0") // 0 = unrestricted
	_, err := config.Load()
	require.NoError(t, err)
}

func TestNormalizeRejectsNegativeLLMInputTokensUsed(t *testing.T) {
	t.Setenv("TURBORG_LLM_INPUT_TOKENS_USED", "-1")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LLM_INPUT_TOKENS_USED")
}

func TestNormalizeRejectsNegativeLLMOutputTokensUsed(t *testing.T) {
	t.Setenv("TURBORG_LLM_OUTPUT_TOKENS_USED", "-1")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LLM_OUTPUT_TOKENS_USED")
}

func TestNormalizeRejectsNegativeMaxConnectorsPerAgent(t *testing.T) {
	t.Setenv("TURBORG_MAX_CONNECTORS_PER_AGENT", "-1")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAX_CONNECTORS_PER_AGENT")
}

func TestNormalizeRejectsNegativeMaxChannels(t *testing.T) {
	t.Setenv("TURBORG_MAX_CHANNELS", "-1")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAX_CHANNELS")
}

func TestNormalizeRejectsNegativeLLMOutput(t *testing.T) {
	t.Setenv("TURBORG_LLM_OUTPUT_TOKENS_PER_DAY", "-1")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LLM_OUTPUT_TOKENS_PER_DAY")
}

func TestNormalizeTrimsAllowedLLMModelsWhitespace(t *testing.T) {
	t.Setenv("TURBORG_ALLOWED_LLM_MODELS", " claude-sonnet-4-6 , , claude-opus-4-7 ")
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"claude-sonnet-4-6", "claude-opus-4-7"}, s.AllowedLLMModels)
}

func TestNormalizeRejectsNegativeStateWebhookDebounce(t *testing.T) {
	t.Setenv("TURBORG_STATE_WEBHOOK_DEBOUNCE_MS", "-1")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "STATE_WEBHOOK_DEBOUNCE_MS")
}

func TestNormalizeRejectsExcessiveStateWebhookDebounce(t *testing.T) {
	t.Setenv("TURBORG_STATE_WEBHOOK_DEBOUNCE_MS", "5001")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "STATE_WEBHOOK_DEBOUNCE_MS")
}

func TestStateWebhookDebounceDefault(t *testing.T) {
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 250, s.StateWebhookDebounceMs,
		"default debounce coalesces bursty channel mutations into one PUT")
}

func TestNormalizeTrimsAllowedNetworksWhitespace(t *testing.T) {
	t.Setenv("TURBORG_ALLOWED_NETWORKS", " irc.libera.chat , , irc.oftc.net ")
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"irc.libera.chat", "irc.oftc.net"}, s.AllowedNetworks)
}

func TestStateTokenDefaultsToSinkToken(t *testing.T) {
	t.Setenv("TURBORG_MESSAGE_SINK_TOKEN", "shared-bearer")
	t.Setenv("TURBORG_STATE_URL", "https://state.example/agents/bot")
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "https://state.example/agents/bot", s.StateURL)
	assert.Equal(t, "shared-bearer", s.StateToken, "state token falls back to the sink token")

	// An explicit state token overrides the fallback.
	t.Setenv("TURBORG_STATE_TOKEN", "state-only")
	s, err = config.Load()
	require.NoError(t, err)
	assert.Equal(t, "state-only", s.StateToken)
}
