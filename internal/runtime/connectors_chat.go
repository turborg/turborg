package runtime

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/turborg/turborg/internal/activity"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/budgetrefresh"
	"github.com/turborg/turborg/internal/commandrefresh"
	"github.com/turborg/turborg/internal/config"
	"github.com/turborg/turborg/internal/connector/discord"
	"github.com/turborg/turborg/internal/connector/slack"
	"github.com/turborg/turborg/internal/connector/telegram"
	"github.com/turborg/turborg/internal/flow"
	"github.com/turborg/turborg/internal/flowrefresh"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/skill"
)

// chatConnector is the closed set of dial-out chat-platform connectors the
// single-instance runtime can boot as the sole (primary) connector of a
// dedicated container.
var chatConnector = map[string]bool{
	"discord":  true,
	"telegram": true,
	"slack":    true,
}

// singlePrimaryChatConnector reports the connector name when TURBORG_CONNECTORS
// names exactly one chat-platform connector and nothing else — the dedicated
// (single-instance) shape for a Discord / Telegram / Slack turborg. IRC (and any
// mix with IRC) stays on the IRC-primary path in Build.
func singlePrimaryChatConnector(names []string) (string, bool) {
	if len(names) != 1 {
		return "", false
	}
	if chatConnector[names[0]] {
		return names[0], true
	}
	return "", false
}

// RequiresIRCSettings reports whether the CLI must load TURBORG_IRC_* settings
// for this connector set. A dedicated chat-platform container (a single
// discord/telegram/slack connector) has no IRC config, so the CLI skips the
// hard-required IRC env; every other shape (the IRC quickstart default, or any
// set containing IRC) needs them.
func RequiresIRCSettings(s *config.Settings) bool {
	_, chatOnly := singlePrimaryChatConnector(s.Connectors)
	return !chatOnly
}

// buildChatOnly wires an agent whose sole connector is a dial-out chat platform
// (discord/telegram/slack). It mirrors the connector-agnostic half of Build —
// LLM provider + budget, message store, skill state, owner guard, the data-
// driven skill/flow set, and the live refreshers — via runtime.WireCore, so a
// dedicated Discord/Telegram/Slack container gets the identical builtins,
// persistence, skills, and hot-reload the IRC path does. There is no bouncer,
// gateway, or IRC state-push (those are IRC-specific).
func buildChatOnly(name string, s *config.Settings, log *slog.Logger) (*Built, error) {
	if log == nil {
		log = slog.Default()
	}

	provider, err := buildLLM(s)
	if err != nil {
		return nil, err
	}
	cmds, err := parseCommands(s.Commands)
	if err != nil {
		return nil, err
	}
	flows, err := parseFlows(s.Flows)
	if err != nil {
		return nil, err
	}

	a := agent.NewWithPrefix(log, s.CommandPrefix)
	notifier := activity.New(s.ActivityURL, s.ActivityToken, log)

	store, sink := buildMessageStore(s, log)
	_ = sink // lifecycle parity; closed with the agent
	skillStore := buildSkillStore(s, log)

	var activityHook func(string)
	if notifier.Enabled() {
		activityHook = notifier.Hook
	}

	provider = BuildBudgetedProvider(a, provider, s.LLMInputTokensPerDay, s.LLMOutputTokensPerDay, s.LLMInputTokensUsed, s.LLMOutputTokensUsed, log)

	conn, actor, botNick, platform, err := buildChatConnector(name, log, a)
	if err != nil {
		return nil, err
	}

	owner := GuardParams{
		OwnerMode:            s.OwnerMode,
		OwnerNick:            s.OwnerNick,
		OwnerAccount:         s.OwnerAccount,
		OwnerHostmask:        s.OwnerHostmask,
		IgnoredNicks:         s.IgnoredNicks,
		BotNick:              botNick(),
		CommandMaxPerWindow:  s.CommandMaxPerWindow,
		CommandWindowSeconds: s.CommandWindowSeconds,
	}

	cp := CommonParams{
		CustomCommandsMax: s.CustomCommandsMax,
		Commands:          cmds,
		Flows:             flows,
		Platform:          platform,
		Owner:             owner,
		LLM:               provider,
		LLMInputCap:       s.LLMInputTokensPerDay,
		LLMOutputCap:      s.LLMOutputTokensPerDay,
		LLMInputUsed:      s.LLMInputTokensUsed,
		LLMOutputUsed:     s.LLMOutputTokensUsed,
		ActivityHook:      activityHook,
		Store:             store,
		SkillStore:        skillStore,
	}
	wiring := WireCore(a, conn, actor, botNick, provider, cp, log)

	built := &Built{
		Agent:     a,
		LLM:       provider,
		Activity:  notifier,
		Scheduler: wiring.Scheduler,
	}

	if bp, ok := provider.(*llm.BudgetedProvider); ok {
		built.BudgetRefresh = budgetrefresh.New(
			s.LLMBudgetURL, s.LLMBudgetToken, s.LLMBudgetRefreshSeconds,
			time.Now(), bp.Budget(), log,
		)
	}
	built.CommandRefresh = commandrefresh.New(
		s.CommandsURL, s.CommandsToken, s.CommandsRefreshSeconds,
		func(skills []skill.Skill) { ApplySkills(a, wiring, skills, provider, owner, platform, log) },
		log,
	)
	built.FlowRefresh = flowrefresh.New(
		s.FlowsURL, s.FlowsToken, s.FlowsRefreshSeconds,
		func(fl []flow.Flow) { ApplyFlows(wiring, fl) },
		log,
	)

	log.Info("runtime features (chat)",
		"connector", name,
		"llm", provider != nil,
		"commands", len(cmds),
		"flows", len(flows),
	)
	return built, nil
}

// buildChatConnector constructs the named chat-platform connector from its
// TURBORG_<PLATFORM>_* env, returning the connector, its Actor, a bot-nick
// resolver, and the {platform} template label. It never opens a network
// connection (that happens in Start, driven by the agent's Run).
func buildChatConnector(name string, log *slog.Logger, a *agent.Agent) (agent.Connector, agent.Actor, func() string, string, error) {
	switch name {
	case "discord":
		cfg, err := discord.LoadSettings()
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("runtime: loading TURBORG_DISCORD_* settings: %w", err)
		}
		if err := cfg.Validate(); err != nil {
			return nil, nil, nil, "", err
		}
		conn := discord.New(cfg, log, a.Events)
		return conn, discord.NewActor(conn), conn.BotName, "Discord", nil
	case "telegram":
		cfg, err := telegram.LoadSettings()
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("runtime: loading TURBORG_TELEGRAM_* settings: %w", err)
		}
		if err := cfg.Validate(); err != nil {
			return nil, nil, nil, "", err
		}
		conn := telegram.New(cfg, log, a.Events)
		return conn, telegram.NewActor(conn), conn.BotName, "Telegram", nil
	case "slack":
		cfg, err := slack.LoadSettings()
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("runtime: loading TURBORG_SLACK_* settings: %w", err)
		}
		if err := cfg.Validate(); err != nil {
			return nil, nil, nil, "", err
		}
		conn := slack.New(cfg, log, a.Events)
		return conn, slack.NewActor(conn), conn.BotName, "Slack", nil
	default:
		return nil, nil, nil, "", fmt.Errorf("runtime: unsupported chat connector %q", name)
	}
}
