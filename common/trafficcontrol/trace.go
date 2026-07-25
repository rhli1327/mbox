package trafficcontrol

import (
	"context"
	"sync"

	"github.com/sagernet/sing-box/adapter"
)

type routeTraceContextKey struct{}

// RouteTrace records the outbound path selected for one routed flow.
//
// The router creates the trace with the rule-selected (or final) outbound.
// Each outbound group then appends the concrete selection it dispatches to.
// This makes the final leaf network-specific and avoids reconstructing history
// later from OutboundGroup.Now().
type RouteTrace struct {
	access             sync.RWMutex
	routeTag           string
	groupPath          []string
	actualOutboundTag  string
	actualOutboundType string
	resolved           bool
}

type RouteTraceSnapshot struct {
	RouteTag           string
	GroupPath          []string
	ActualOutboundTag  string
	ActualOutboundType string
	Resolved           bool
}

func NewRouteTrace(routeTag string, routeType string, routeIsGroup bool) *RouteTrace {
	trace := &RouteTrace{
		routeTag: routeTag,
	}
	if routeIsGroup {
		if routeTag != "" {
			trace.groupPath = []string{routeTag}
		}
	} else {
		trace.actualOutboundTag = routeTag
		trace.actualOutboundType = routeType
		trace.resolved = true
	}
	return trace
}

func NewRouteTraceForOutbound(outbound adapter.Outbound) *RouteTrace {
	if outbound == nil {
		return new(RouteTrace)
	}
	_, isGroup := outbound.(adapter.OutboundGroup)
	return NewRouteTrace(outbound.Tag(), outbound.Type(), isGroup)
}

func ContextWithRouteTrace(ctx context.Context, outbound adapter.Outbound) context.Context {
	return context.WithValue(ctx, routeTraceContextKey{}, NewRouteTraceForOutbound(outbound))
}

func ContextWithExistingRouteTrace(ctx context.Context, trace *RouteTrace) context.Context {
	if trace == nil {
		return ctx
	}
	return context.WithValue(ctx, routeTraceContextKey{}, trace)
}

func RouteTraceFromContext(ctx context.Context) *RouteTrace {
	trace, _ := ctx.Value(routeTraceContextKey{}).(*RouteTrace)
	return trace
}

func RecordOutboundSelection(ctx context.Context, parent adapter.Outbound, selected adapter.Outbound) {
	trace := RouteTraceFromContext(ctx)
	if trace == nil || parent == nil || selected == nil {
		return
	}
	_, selectedIsGroup := selected.(adapter.OutboundGroup)
	trace.RecordSelection(parent.Tag(), selected.Tag(), selected.Type(), selectedIsGroup)
}

func (t *RouteTrace) RecordSelection(parentTag string, selectedTag string, selectedType string, selectedIsGroup bool) {
	if t == nil {
		return
	}
	t.access.Lock()
	defer t.access.Unlock()
	t.appendGroup(parentTag)
	if selectedIsGroup {
		t.actualOutboundTag = ""
		t.actualOutboundType = ""
		t.resolved = false
		t.appendGroup(selectedTag)
		return
	}
	t.actualOutboundTag = selectedTag
	t.actualOutboundType = selectedType
	t.resolved = true
}

func (t *RouteTrace) appendGroup(tag string) {
	if tag == "" {
		return
	}
	if len(t.groupPath) > 0 && t.groupPath[len(t.groupPath)-1] == tag {
		return
	}
	t.groupPath = append(t.groupPath, tag)
}

func (t *RouteTrace) Snapshot() RouteTraceSnapshot {
	if t == nil {
		return RouteTraceSnapshot{}
	}
	t.access.RLock()
	defer t.access.RUnlock()
	return RouteTraceSnapshot{
		RouteTag:           t.routeTag,
		GroupPath:          append([]string(nil), t.groupPath...),
		ActualOutboundTag:  t.actualOutboundTag,
		ActualOutboundType: t.actualOutboundType,
		Resolved:           t.resolved,
	}
}
