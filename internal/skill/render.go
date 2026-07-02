package skill

import (
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Helper-placeholder patterns mirror the command builder's: each captures the
// comma-separated argument list (or the bound, for random) and never spans a
// closing brace.
var (
	choiceRe  = regexp.MustCompile(`\{choice:([^}]*)\}`)
	randomRe  = regexp.MustCompile(`\{random:([^}]*)\}`)
	shuffleRe = regexp.MustCompile(`\{shuffle:([^}]*)\}`)
)

// renderFields carries the per-fire values an engine skill's template can
// reference. They are populated from the event/message that triggered the
// skill; unset fields render empty.
type renderFields struct {
	user     string // the acting nick (sender / joiner / setter / offender)
	channel  string
	text     string // the message text (match triggers)
	target   string // the affected nick (kick victim, nick-change subject)
	reason   string
	topic    string
	modes    string
	oldNick  string
	newNick  string
	platform string
	owner    string
	count    int // per-sender flood count, when computed
}

// render substitutes the supported placeholders in a template. The dynamic
// helpers ({choice}/{random}/{shuffle}) are expanded first, over the
// author-supplied template only, so a value substituted afterwards (e.g.
// {text}) can never inject a helper.
func render(tmpl string, f renderFields) string {
	tmpl = expandHelpers(tmpl)
	now := time.Now().UTC()
	return strings.NewReplacer(
		"{user}", f.user,
		"{nick}", f.user,
		"{channel}", f.channel,
		"{room}", f.channel,
		"{text}", f.text,
		"{message}", f.text,
		"{target}", f.target,
		"{reason}", f.reason,
		"{topic}", f.topic,
		"{modes}", f.modes,
		"{old}", f.oldNick,
		"{new}", f.newNick,
		"{platform}", f.platform,
		"{network}", f.platform,
		"{owner}", f.owner,
		"{count}", strconv.Itoa(f.count),
		"{date}", now.Format("2006-01-02"),
		"{time}", now.Format("15:04:05"),
		"{datetime}", now.Format("2006-01-02 15:04:05 UTC"),
	).Replace(tmpl)
}

// expandHelpers evaluates the random dynamic-placeholder helpers in a
// template. A {random:N} with a missing or non-positive bound is left literal
// so a typo is visible rather than silent.
func expandHelpers(tmpl string) string {
	tmpl = choiceRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		opts := splitOptions(choiceRe.FindStringSubmatch(m)[1])
		if len(opts) == 0 {
			return ""
		}
		return opts[rand.Intn(len(opts))]
	})
	tmpl = shuffleRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		opts := splitOptions(shuffleRe.FindStringSubmatch(m)[1])
		rand.Shuffle(len(opts), func(i, j int) { opts[i], opts[j] = opts[j], opts[i] })
		return strings.Join(opts, ",")
	})
	tmpl = randomRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		n, err := strconv.Atoi(strings.TrimSpace(randomRe.FindStringSubmatch(m)[1]))
		if err != nil || n < 1 {
			return m
		}
		return strconv.Itoa(rand.Intn(n) + 1)
	})
	return tmpl
}

// splitOptions splits a comma-separated helper argument into trimmed,
// non-empty options.
func splitOptions(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
