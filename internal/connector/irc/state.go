package irc

import (
	"strings"
	"sync"
)

// memberPrefixes is the set of mode-prefix characters that may precede a
// nickname in RPL_NAMREPLY (353). Order doesn't matter — we just check
// containment.
const memberPrefixes = "@+%&~"

// ChannelInfo is per-channel state cached from observed server messages.
// Mirrors Python core/connectors/irc/state.py:ChannelInfo. The bouncer
// replays this to new clients on auth so they pick up the bot's view of
// the network instead of starting blank.
type ChannelInfo struct {
	// Name is the original-cased channel ("#XSHELLZ-Test2"). Lookups are
	// case-insensitive but display preserves the form the server used.
	Name string

	Topic        string
	TopicSet     bool
	TopicSetBy   string
	TopicSetAt   int64
	Members      map[string]string // nick → mode prefix ("", "@", "+", "%", "&", "~")
	NamesComplete bool
}

// ChannelState tracks every channel the bot has joined. Safe for
// concurrent use by the IRC reader (writer) and the bouncer (reader).
type ChannelState struct {
	mu       sync.RWMutex
	channels map[string]*ChannelInfo // lowercased name → info
	order    []string                // insertion order, for JoinedChannels()
}

func NewChannelState() *ChannelState {
	return &ChannelState{channels: map[string]*ChannelInfo{}}
}

// Get returns a snapshot copy of the named channel's info, or nil if the
// bot isn't in it. The copy is intentional: callers (e.g. the bouncer)
// must not mutate live state.
func (s *ChannelState) Get(channel string) *ChannelInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.channels[strings.ToLower(channel)]
	if !ok {
		return nil
	}
	return cloneChannelInfo(info)
}

// JoinedChannels returns snapshot copies of every channel currently
// joined, in insertion order.
func (s *ChannelState) JoinedChannels() []*ChannelInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*ChannelInfo, 0, len(s.order))
	for _, key := range s.order {
		if info, ok := s.channels[key]; ok {
			out = append(out, cloneChannelInfo(info))
		}
	}
	return out
}

// OnSelfJoin records that the bot joined a channel. Idempotent.
func (s *ChannelState) OnSelfJoin(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.ToLower(channel)
	if _, ok := s.channels[key]; ok {
		return
	}
	s.channels[key] = &ChannelInfo{
		Name:    channel,
		Members: map[string]string{},
	}
	s.order = append(s.order, key)
}

func (s *ChannelState) OnSelfPart(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.ToLower(channel)
	delete(s.channels, key)
	for i, k := range s.order {
		if k == key {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// SetTopic updates the topic text. Use ClearTopic to record an empty
// topic (RPL_NOTOPIC) — passing an empty string here is treated the same.
func (s *ChannelState) SetTopic(channel, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.channels[strings.ToLower(channel)]
	if !ok {
		return
	}
	info.Topic = topic
	info.TopicSet = true
}

func (s *ChannelState) ClearTopic(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.channels[strings.ToLower(channel)]
	if !ok {
		return
	}
	info.Topic = ""
	info.TopicSet = true
}

// SetTopicMeta records who set the topic and when. Maps to Python's
// on_topic(channel, set_by=..., set_at=...) when no topic= kwarg is
// supplied (RPL_TOPICWHOTIME / 333).
func (s *ChannelState) SetTopicMeta(channel, setBy string, setAt int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.channels[strings.ToLower(channel)]
	if !ok {
		return
	}
	if setBy != "" {
		info.TopicSetBy = setBy
	}
	if setAt > 0 {
		info.TopicSetAt = setAt
	}
}

// OnNamesReply appends one RPL_NAMREPLY chunk to a channel's member list.
// Multiple chunks may arrive before RPL_ENDOFNAMES; callers don't need to
// coordinate.
func (s *ChannelState) OnNamesReply(channel string, members []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := strings.ToLower(channel)
	info, ok := s.channels[key]
	if !ok {
		info = &ChannelInfo{Name: channel, Members: map[string]string{}}
		s.channels[key] = info
		s.order = append(s.order, key)
	}
	for _, entry := range members {
		prefix, nick := splitMemberPrefix(entry)
		if nick != "" {
			info.Members[nick] = prefix
		}
	}
}

func (s *ChannelState) OnNamesEnd(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.channels[strings.ToLower(channel)]; ok {
		info.NamesComplete = true
	}
}

func (s *ChannelState) OnMemberJoin(channel, nick string) {
	if nick == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.channels[strings.ToLower(channel)]; ok {
		if _, exists := info.Members[nick]; !exists {
			info.Members[nick] = ""
		}
	}
}

func (s *ChannelState) OnMemberPart(channel, nick string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.channels[strings.ToLower(channel)]; ok {
		delete(info.Members, nick)
	}
}

func (s *ChannelState) OnMemberKick(channel, nick string) {
	s.OnMemberPart(channel, nick)
}

// OnMemberQuit removes a nick from every channel — IRC QUIT applies
// network-wide, not per-channel.
func (s *ChannelState) OnMemberQuit(nick string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, info := range s.channels {
		delete(info.Members, nick)
	}
}

// OnNickChange propagates a NICK change across every channel that knew
// the old nick.
func (s *ChannelState) OnNickChange(oldNick, newNick string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, info := range s.channels {
		if prefix, ok := info.Members[oldNick]; ok {
			info.Members[newNick] = prefix
			delete(info.Members, oldNick)
		}
	}
}

func splitMemberPrefix(member string) (prefix, nick string) {
	if member != "" && strings.ContainsRune(memberPrefixes, rune(member[0])) {
		return string(member[0]), member[1:]
	}
	return "", member
}

func cloneChannelInfo(in *ChannelInfo) *ChannelInfo {
	members := make(map[string]string, len(in.Members))
	for k, v := range in.Members {
		members[k] = v
	}
	return &ChannelInfo{
		Name:          in.Name,
		Topic:         in.Topic,
		TopicSet:      in.TopicSet,
		TopicSetBy:    in.TopicSetBy,
		TopicSetAt:    in.TopicSetAt,
		Members:       members,
		NamesComplete: in.NamesComplete,
	}
}
