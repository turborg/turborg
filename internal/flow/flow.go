// Package flow is turborg's declarative node-graph engine: an operator wires
// activity nodes (set / if / switch / say / notice / effect / llm / webhook /
// getvar / setvar / stop) into a directed graph, and the engine walks that
// graph when the flow's trigger fires. It is the modular, composable layer
// above single-shot skills — a flow can branch, transform a data bag, call the
// LLM, act on the network through the connector-agnostic Actor, and call out to
// external endpoints, all without code.
//
// The model is intentionally small and JSON-serializable so a builder UI can
// author flows and introspect the node catalog. It is declarative only: nodes
// come from a fixed, vetted registry; there is no sandboxed or arbitrary code
// execution. Execution is bounded (a step ceiling) so a malformed or cyclic
// graph can never run away.
package flow

import "github.com/turborg/turborg/internal/skill"

// Flow is one node graph plus the trigger that starts it. Trigger reuses the
// skill trigger model (command / event / match / schedule) so flows and skills
// share one trigger vocabulary.
type Flow struct {
	Name    string        `json:"name"`
	Trigger skill.Trigger `json:"trigger"`
	Nodes   []Node        `json:"nodes"`
	Edges   []Edge        `json:"edges"`
	// Category is optional display-only metadata (e.g. a builder-UI grouping
	// label). The engine ignores it entirely; it round-trips through JSON so an
	// authoring UI can carry it, but it never affects execution.
	Category string `json:"category,omitempty"`
}

// Node is one activity in a flow: a registered Type plus free-form Config the
// node's handler interprets. ID is unique within the flow and is what edges
// reference.
type Node struct {
	ID     string         `json:"id"`
	Type   string         `json:"type"`
	Config map[string]any `json:"config,omitempty"`
}

// Edge connects one node's output port to another node's input. Port selects a
// branch on a multi-output node ("" for the default/single output; "true" /
// "false" for an if node; a case label for a switch node). An edge whose From
// is empty (or "start"/"trigger") is an entry edge.
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Port string `json:"port,omitempty"`
}

// triggerKind returns the flow's trigger kind, defaulting to command.
func (f Flow) triggerKind() skill.TriggerKind {
	if f.Trigger.Kind == "" {
		return skill.KindCommand
	}
	return f.Trigger.Kind
}

// entryNodes returns the IDs of nodes that no edge points to — the graph's
// starting points. Explicit entry edges (From empty/"start"/"trigger") also
// seed execution; their targets are returned too.
func (f Flow) entryNodes() []string {
	hasIncoming := map[string]bool{}
	var explicit []string
	for _, e := range f.Edges {
		if isEntryFrom(e.From) {
			explicit = append(explicit, e.To)
			continue
		}
		hasIncoming[e.To] = true
	}
	seen := map[string]bool{}
	var out []string
	add := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, id := range explicit {
		add(id)
	}
	for _, n := range f.Nodes {
		if !hasIncoming[n.ID] {
			add(n.ID)
		}
	}
	return out
}

// successors returns the target node IDs reachable from node `from` via output
// `port`. The default port ("") also follows edges authored without a port.
func (f Flow) successors(from, port string) []string {
	var out []string
	for _, e := range f.Edges {
		if e.From != from {
			continue
		}
		if e.Port == port {
			out = append(out, e.To)
		}
	}
	return out
}

func isEntryFrom(from string) bool {
	switch from {
	case "", "start", "trigger":
		return true
	default:
		return false
	}
}
