package agent_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/turborg/turborg/internal/agent"
)

func TestEventBusPublishWithNoSubscribers(t *testing.T) {
	bus := agent.NewEventBus(nil)
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventBoot})
}

func TestEventBusDeliversToAllHandlers(t *testing.T) {
	bus := agent.NewEventBus(nil)
	var a, b atomic.Int32
	bus.Subscribe(agent.EventMessage, func(_ context.Context, _ *agent.Event) { a.Add(1) })
	bus.Subscribe(agent.EventMessage, func(_ context.Context, _ *agent.Event) { b.Add(1) })

	bus.Publish(context.Background(), &agent.Event{Type: agent.EventMessage})

	assert.Equal(t, int32(1), a.Load())
	assert.Equal(t, int32(1), b.Load())
}

func TestEventBusIsolatesHandlerPanic(t *testing.T) {
	bus := agent.NewEventBus(nil)
	var survivor atomic.Int32

	bus.Subscribe(agent.EventMessage, func(_ context.Context, _ *agent.Event) {
		panic("boom")
	})
	bus.Subscribe(agent.EventMessage, func(_ context.Context, _ *agent.Event) {
		survivor.Add(1)
	})

	bus.Publish(context.Background(), &agent.Event{Type: agent.EventMessage})

	assert.Equal(t, int32(1), survivor.Load(),
		"panic in one handler must not block siblings")
}

func TestEventBusStampsTimeWhenZero(t *testing.T) {
	bus := agent.NewEventBus(nil)
	var captured time.Time

	bus.Subscribe(agent.EventMessage, func(_ context.Context, ev *agent.Event) {
		captured = ev.Time
	})
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventMessage})

	assert.False(t, captured.IsZero(), "Publish must stamp Time if caller left it zero")
}

func TestEventBusFiltersByType(t *testing.T) {
	bus := agent.NewEventBus(nil)
	var msgCount, shutdownCount atomic.Int32

	bus.Subscribe(agent.EventMessage, func(_ context.Context, _ *agent.Event) { msgCount.Add(1) })
	bus.Subscribe(agent.EventShutdown, func(_ context.Context, _ *agent.Event) { shutdownCount.Add(1) })

	bus.Publish(context.Background(), &agent.Event{Type: agent.EventMessage})
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventShutdown})
	bus.Publish(context.Background(), &agent.Event{Type: agent.EventMessage})

	assert.Equal(t, int32(2), msgCount.Load())
	assert.Equal(t, int32(1), shutdownCount.Load())
}
