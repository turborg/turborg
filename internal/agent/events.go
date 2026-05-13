package agent

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// EventType mirrors the Python core/events.py StrEnum. Values match
// byte-for-byte so external consumers (xshellz orchestrator, observability
// stack) see the same event names regardless of which implementation is
// running.
type EventType string

const (
	EventBoot     EventType = "BOOT"
	EventReady    EventType = "READY"
	EventShutdown EventType = "SHUTDOWN"

	EventMessage     EventType = "MESSAGE"
	EventMessageSent EventType = "MESSAGE_SENT"
	EventCommand     EventType = "COMMAND"

	EventUserJoin       EventType = "USER_JOIN"
	EventUserLeave      EventType = "USER_LEAVE"
	EventUserKicked     EventType = "USER_KICKED"
	EventJoinFailed     EventType = "JOIN_FAILED"
	EventUserNickChange EventType = "USER_NICK_CHANGE"
	EventTopicChanged   EventType = "TOPIC_CHANGED"
	EventChannelNames   EventType = "CHANNEL_NAMES"
	EventServerNotice   EventType = "SERVER_NOTICE"
	EventModeChanged    EventType = "MODE_CHANGED"
	EventWhoisResult    EventType = "WHOIS_RESULT"
	EventListResult     EventType = "LIST_RESULT"
	EventWhoResult      EventType = "WHO_RESULT"
	EventError          EventType = "ERROR"
	EventRaw            EventType = "RAW"
)

type Event struct {
	Type   EventType
	Time   time.Time
	Fields map[string]any
}

type EventHandler func(ctx context.Context, ev *Event)

// EventBus is an in-process pub/sub. Publish blocks until every subscribed
// handler returns, matching the Python EventBus contract built on
// asyncio.gather(..., return_exceptions=True): handlers run concurrently,
// a panic in one is logged but never propagates to siblings or the
// publisher.
type EventBus struct {
	mu       sync.RWMutex
	handlers map[EventType][]EventHandler
	log      *slog.Logger
}

func NewEventBus(log *slog.Logger) *EventBus {
	if log == nil {
		log = slog.Default()
	}
	return &EventBus{
		handlers: map[EventType][]EventHandler{},
		log:      log,
	}
}

func (b *EventBus) Subscribe(t EventType, h EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[t] = append(b.handlers[t], h)
}

func (b *EventBus) Publish(ctx context.Context, ev *Event) {
	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}
	b.mu.RLock()
	handlers := append([]EventHandler{}, b.handlers[ev.Type]...)
	b.mu.RUnlock()
	if len(handlers) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, h := range handlers {
		h := h
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					b.log.Error("event handler panic", "type", ev.Type, "panic", r)
				}
			}()
			h(ctx, ev)
		}()
	}
	wg.Wait()
}
