package trafficcontrol

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/compatible"
	"github.com/sagernet/sing/common/cleanup"
	"github.com/sagernet/sing/common/observable"
	"github.com/sagernet/sing/common/x/list"

	"github.com/gofrs/uuid/v5"
)

type ConnectionEventType int

const (
	ConnectionEventNew ConnectionEventType = iota
	ConnectionEventClosed
)

type ConnectionEvent struct {
	Type     ConnectionEventType
	ID       uuid.UUID
	Metadata *TrackerMetadata
	ClosedAt time.Time
}

const closedConnectionsLimit = 1000

var (
	_ adapter.ConnectionTracker = (*Manager)(nil)
	_ adapter.LifecycleService  = (*Manager)(nil)
)

type Manager struct {
	outbound      adapter.OutboundManager
	deltaRecorder DeltaRecorder
	uploadTotal   atomic.Int64
	downloadTotal atomic.Int64

	lifecycleAccess sync.Mutex
	lifecycleCond   *sync.Cond
	closing         bool
	liveTrackers    int
	closeOnce       sync.Once
	closeErr        error

	connections             compatible.Map[uuid.UUID, Tracker]
	closedConnectionsAccess sync.Mutex
	closedConnections       list.List[TrackerMetadata]

	eventSubscriber *observable.Subscriber[ConnectionEvent]
	eventObserver   *observable.Observer[ConnectionEvent]
	cleaner         *cleanup.Cleaner

	historyDone     chan struct{}
	historyStopOnce sync.Once
	historyWait     sync.WaitGroup
}

type DeltaRecorder interface {
	RecordDelta(metadata *TrackerMetadata, uplink int64, downlink int64, newConnection bool)
}

func NewManager(outbound adapter.OutboundManager, recorders ...DeltaRecorder) *Manager {
	manager := &Manager{
		outbound:        outbound,
		eventSubscriber: observable.NewSubscriber[ConnectionEvent](256),
		historyDone:     make(chan struct{}),
	}
	if len(recorders) > 0 {
		manager.deltaRecorder = recorders[0]
	}
	manager.lifecycleCond = sync.NewCond(&manager.lifecycleAccess)
	return manager
}

func (m *Manager) Name() string {
	return "traffic manager"
}

func (m *Manager) Start(stage adapter.StartStage) error {
	switch stage {
	case adapter.StartStateInitialize:
		m.eventObserver = observable.NewObserver(m.eventSubscriber, 64)
		m.cleaner = cleanup.Add(m.Clear)
	case adapter.StartStateStart:
		if m.deltaRecorder != nil {
			m.historyWait.Add(1)
			go m.loopHistory()
		}
	}
	return nil
}

func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.closeErr = m.close()
	})
	return m.closeErr
}

func (m *Manager) close() error {
	m.lifecycleAccess.Lock()
	m.closing = true
	var trackers []Tracker
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		trackers = append(trackers, tracker)
		return true
	})
	m.lifecycleAccess.Unlock()

	m.historyStopOnce.Do(func() {
		close(m.historyDone)
	})
	m.historyWait.Wait()

	// Tracker.Close is a synchronous drain boundary: connection trackers wait
	// for all in-flight counting callbacks before they call leave. Calling
	// leave again also seals trackers whose Close implementation does not.
	for _, tracker := range trackers {
		_ = tracker.Close()
		m.leave(tracker)
	}
	m.lifecycleAccess.Lock()
	for m.liveTrackers > 0 {
		m.lifecycleCond.Wait()
	}
	m.lifecycleAccess.Unlock()

	if m.cleaner != nil {
		m.cleaner.Close()
	}
	if m.eventObserver != nil {
		return m.eventObserver.Close()
	}
	return nil
}

func (m *Manager) SubscribeEvents() (observable.Subscription[ConnectionEvent], <-chan struct{}, error) {
	return m.eventObserver.Subscribe()
}

func (m *Manager) UnSubscribeEvents(subscription observable.Subscription[ConnectionEvent]) {
	m.eventObserver.UnSubscribe(subscription)
}

func (m *Manager) join(tracker Tracker) bool {
	m.lifecycleAccess.Lock()
	defer m.lifecycleAccess.Unlock()
	if m.closing {
		return false
	}
	metadata := tracker.Metadata()
	m.connections.Store(metadata.ID, tracker)
	m.liveTrackers++
	m.eventSubscriber.Emit(ConnectionEvent{
		Type:     ConnectionEventNew,
		ID:       metadata.ID,
		Metadata: metadata,
	})
	return true
}

func (m *Manager) leave(tracker Tracker) {
	metadata := tracker.Metadata()
	m.lifecycleAccess.Lock()
	_, loaded := m.connections.LoadAndDelete(metadata.ID)
	if !loaded {
		m.lifecycleAccess.Unlock()
		return
	}
	m.lifecycleAccess.Unlock()
	defer func() {
		m.lifecycleAccess.Lock()
		m.liveTrackers--
		m.lifecycleCond.Broadcast()
		m.lifecycleAccess.Unlock()
	}()

	m.recordHistory(tracker, true)
	closedAt := time.Now()
	metadata.ClosedAt = closedAt
	metadataCopy := *metadata
	m.closedConnectionsAccess.Lock()
	if m.closedConnections.Len() >= closedConnectionsLimit {
		m.closedConnections.PopFront()
	}
	m.closedConnections.PushBack(metadataCopy)
	m.closedConnectionsAccess.Unlock()
	m.eventSubscriber.Emit(ConnectionEvent{
		Type:     ConnectionEventClosed,
		ID:       metadata.ID,
		Metadata: &metadataCopy,
		ClosedAt: closedAt,
	})
}

func (m *Manager) loopHistory() {
	defer m.historyWait.Done()
	ticker := time.NewTicker(historyFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.sampleHistory(false)
		case <-m.historyDone:
			return
		}
	}
}

func (m *Manager) sampleHistory(final bool) {
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		m.recordHistory(tracker, final)
		return true
	})
}

func (m *Manager) recordHistory(tracker Tracker, final bool) {
	if m.deltaRecorder == nil {
		return
	}
	historyTracker, loaded := tracker.(interface{ RecordHistory(final bool) })
	if loaded {
		historyTracker.RecordHistory(final)
	}
}

func (m *Manager) Total() (uplinkTotal int64, downlinkTotal int64) {
	return m.uploadTotal.Load(), m.downloadTotal.Load()
}

func (m *Manager) ConnectionsLen() int {
	return m.connections.Len()
}

func (m *Manager) Connections() []*TrackerMetadata {
	var connections []*TrackerMetadata
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		connections = append(connections, tracker.Metadata())
		return true
	})
	return connections
}

func (m *Manager) ClosedConnections() []*TrackerMetadata {
	m.closedConnectionsAccess.Lock()
	values := m.closedConnections.Array()
	m.closedConnectionsAccess.Unlock()
	if len(values) == 0 {
		return nil
	}
	connections := make([]*TrackerMetadata, len(values))
	for i := range values {
		connections[i] = &values[i]
	}
	return connections
}

func (m *Manager) Connection(id uuid.UUID) Tracker {
	connection, loaded := m.connections.Load(id)
	if !loaded {
		return nil
	}
	return connection
}

func (m *Manager) CloseAllConnections() {
	m.connections.Range(func(_ uuid.UUID, tracker Tracker) bool {
		tracker.Close()
		return true
	})
}

func (m *Manager) Clear() {
	m.closedConnectionsAccess.Lock()
	defer m.closedConnectionsAccess.Unlock()
	m.closedConnections.Init()
}
