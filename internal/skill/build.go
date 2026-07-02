package skill

import (
	"log/slog"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/commands"
	"github.com/turborg/turborg/internal/llm"
)

// GuardFactory builds the per-command guard for a command-kind skill. A nil
// factory or a nil return means "no per-command guard" (the registry-wide
// guard still runs).
type GuardFactory func(Skill) agent.CommandGuard

// Build turns the command-kind skills in the batch into a ReplaceDynamic
// batch of agent commands, producing the SAME handlers as the historical
// command builder (it delegates to it per skill, preserving every
// placeholder, the LLM system-prompt fallback, and the budget-exhausted
// reply). Non-command skills are ignored here — they belong to the Engine.
// platform/owner seed the {platform}/{owner} template placeholders.
func Build(skills []Skill, provider llm.Provider, guardFor GuardFactory, platform, owner string, log *slog.Logger) []agent.DynamicCommand {
	if log == nil {
		log = slog.Default()
	}
	out := make([]agent.DynamicCommand, 0, len(skills))
	for _, s := range skills {
		if !s.IsCommand() {
			continue
		}
		sk := s
		built := commands.Build(
			[]commands.Definition{sk.ToDefinition()},
			provider,
			func(commands.Definition) agent.CommandGuard {
				if guardFor != nil {
					return guardFor(sk)
				}
				return nil
			},
			platform, owner, log,
		)
		out = append(out, built...)
	}
	return out
}
