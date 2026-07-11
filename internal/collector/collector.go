package collector

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"traffic-go/internal/config"
	"traffic-go/internal/model"
	"traffic-go/internal/store"
)

type Runner interface {
	Start(context.Context) error
	ActiveProcesses() []model.ProcessListItem
	ActiveStats() model.ActiveStats
	ReadyStatus() model.CollectorReadiness
	Diagnostics() model.CollectorDiagnostics
}

type Service struct {
	cfg                     config.Config
	store                   *store.Store
	logger                  *log.Logger
	resolver                *processResolver
	currentMinute           time.Time
	buckets                 map[model.UsageKey]*bucketState
	forwardBuckets          map[model.ForwardUsageKey]*bucketState
	interfaceBuckets        map[string]model.InterfaceUsageDelta
	usageFlowOwner          map[string]model.UsageKey
	fwdFlowOwner            map[string]model.ForwardUsageKey
	frozenFlowSequence      uint64
	runtimeMu               sync.RWMutex
	snapshots               map[string]model.FlowSnapshot
	snapshotReady           bool
	processHints            map[string]processHint
	localIPs                map[string]struct{}
	lastIPRefresh           time.Time
	interfaceCounters       map[string]interfaceCounter
	socketIndex             socketIndex
	lastSockRefresh         time.Time
	socketReady             bool
	warnedNoAcct            bool
	lastTickMetrics         tickMetrics
	lastSuccessfulTick      time.Time
	consecutiveTickFailures int64
}

type bucketState struct {
	bytesUp   int64
	bytesDown int64
	pktsUp    int64
	pktsDown  int64
	flowCount int64
	perFlow   map[string]flowContribution
}

type tickMetrics struct {
	tickDuration       time.Duration
	conntrackRead      time.Duration
	socketIndexLookup  time.Duration
	processResolve     time.Duration
	rawFlowCount       int
	observedFlowCount  int
	socketIndexEntries int
}

const defaultProcessHintTTL = 90 * time.Second
const shutdownFlushTimeout = 5 * time.Second
const defaultFlowGraceTTL = 15 * time.Second
const defaultSocketIndexTTL = 10 * time.Second

type processHint struct {
	process model.ProcessInfo
	expires time.Time
}

func New(cfg config.Config, trafficStore *store.Store, logger *log.Logger) Runner {
	if cfg.MockData {
		return &mockCollector{
			cfg:    cfg,
			store:  trafficStore,
			logger: logger,
		}
	}
	if runtime.GOOS != "linux" {
		// Offline inspection on non-Linux hosts should remain read-only. Writing
		// synthetic records into a copied production database makes the analysis
		// misleading, so mock traffic is now opt-in via mock_data only.
		return &noopCollector{}
	}

	if _, err := os.Stat(cfg.ConntrackPath); err != nil {
		return &failingCollector{err: fmt.Errorf("conntrack path %q unavailable: %w", cfg.ConntrackPath, err)}
	}

	return &Service{
		cfg:              cfg,
		store:            trafficStore,
		logger:           logger,
		resolver:         newProcessResolver(cfg.ProcFS),
		buckets:          make(map[model.UsageKey]*bucketState),
		forwardBuckets:   make(map[model.ForwardUsageKey]*bucketState),
		interfaceBuckets: make(map[string]model.InterfaceUsageDelta),
		usageFlowOwner:   make(map[string]model.UsageKey),
		fwdFlowOwner:     make(map[string]model.ForwardUsageKey),
		snapshots:        make(map[string]model.FlowSnapshot),
		processHints:     make(map[string]processHint),
	}
}

type failingCollector struct {
	err error
}

func (f *failingCollector) Start(context.Context) error {
	return f.err
}

func (f *failingCollector) ActiveProcesses() []model.ProcessListItem {
	return nil
}

func (f *failingCollector) ActiveStats() model.ActiveStats {
	return model.ActiveStats{}
}

func (f *failingCollector) ReadyStatus() model.CollectorReadiness {
	return model.CollectorReadiness{}
}

func (f *failingCollector) Diagnostics() model.CollectorDiagnostics {
	return model.CollectorDiagnostics{
		AttributionCounts: make(map[string]int64),
	}
}

func (s *Service) Start(ctx context.Context) error {
	s.currentMinute = time.Now().UTC().Truncate(time.Minute)
	if err := s.refreshLocalIPs(); err != nil {
		s.logger.Printf("refresh local IPs: %v", err)
	}
	// Schedule the next collection only after the current one finishes. A ticker
	// retains a pending tick while collection is slow, which makes an overloaded
	// single-core host run ticks back-to-back with no recovery window.
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
			if err := s.flushCurrentBuckets(flushCtx); err != nil {
				s.logger.Printf("flush buckets on shutdown failed: %v", err)
			}
			cancel()
			return nil
		case <-timer.C:
			tickAt := time.Now().UTC()
			if err := s.runTick(ctx, tickAt); err != nil {
				s.recordTickOutcome(time.Now().UTC(), err)
				s.logger.Printf("collector tick failed: %v", err)
			} else {
				s.recordTickOutcome(time.Now().UTC(), nil)
			}
			timer.Reset(s.cfg.TickInterval)
		}
	}
}

func (s *Service) recordTickOutcome(at time.Time, err error) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	if err != nil {
		s.consecutiveTickFailures++
		return
	}
	s.lastSuccessfulTick = at.UTC()
	s.consecutiveTickFailures = 0
}

func (s *Service) runTick(ctx context.Context, now time.Time) error {
	tickStarted := time.Now()
	metrics := tickMetrics{}
	defer func() {
		metrics.tickDuration = time.Since(tickStarted)
		s.recordTickMetrics(metrics)
	}()

	minute := now.Truncate(time.Minute)
	if !minute.Equal(s.currentMinute) {
		if err := s.flushCurrentBuckets(ctx); err != nil {
			return err
		}
		s.currentMinute = minute
	}
	if err := s.collectInterfaceDeltas(); err != nil {
		s.logger.Printf("collect interface counters: %v", err)
	}

	if now.Sub(s.lastIPRefreshTime()) >= 30*time.Second || s.localIPCount() == 0 {
		if err := s.refreshLocalIPs(); err != nil {
			s.logger.Printf("refresh local IPs: %v", err)
		}
	}

	conntrackStarted := time.Now()
	flows, err := readConntrackObservations(s.cfg.ConntrackPath)
	metrics.conntrackRead = time.Since(conntrackStarted)
	if err != nil {
		return err
	}
	metrics.rawFlowCount = len(flows)
	if len(flows) > 0 && !s.warnedNoAcct && !hasConntrackObservationAccounting(flows) {
		s.logger.Printf("conntrack accounting appears disabled; enable `sysctl -w net.netfilter.nf_conntrack_acct=1` to collect byte and packet counters")
		s.warnedNoAcct = true
	}
	socketStarted := time.Now()
	socketIndex, err := s.socketIndexSnapshot(now)
	metrics.socketIndexLookup = time.Since(socketStarted)
	if err != nil {
		return err
	}
	metrics.socketIndexEntries = len(socketIndex.ByTuple) + len(socketIndex.ByLocal)
	s.pruneProcessHints(now)

	inodesNeeded := make(map[uint64]struct{})
	localIPs := s.localIPSnapshot()
	observed := normalizeConntrackObservations(flows, localIPs, socketIndex)
	metrics.observedFlowCount = len(observed)
	for _, flow := range observed {
		if flow.Classified.Inode > 0 {
			inodesNeeded[flow.Classified.Inode] = struct{}{}
		}
	}

	resolveStarted := time.Now()
	processes := s.resolver.Resolve(ctx, inodesNeeded)
	metrics.processResolve = time.Since(resolveStarted)
	// Build one mutable next-state map. Stale entries are retained only when the
	// same identity was observed again, allowing a confirmed conntrack lineage to
	// resume from its last counters after a long read outage without keeping every
	// vanished flow indefinitely.
	nextSnapshots, baselineReady, expiredOwnership := s.snapshotsForTick(now, observed)
	for flowID := range expiredOwnership {
		// Once a snapshot ages out, its contribution belongs to a completed
		// generation even if the tuple is absent on this tick. Freezing now is
		// essential: otherwise a later tuple reuse could inherit its owner.
		s.releaseFlowOwnership(flowID)
	}

	for _, flow := range observed {
		classified := flow.Classified
		process := processes[classified.Inode]
		if process.PID <= 0 {
			if hinted, ok := s.lookupProcessHint(classified, now); ok {
				process = hinted
				classified.MatchedByHint = true
			}
		}

		flowID := string(flow.Identity)
		prev, exists := nextSnapshots[flowID]
		lineageContinues := observedLineageContinues(prev, flow.Flow.CTID, flow.Flow.SyntheticCTID, flow.RawIDs)
		snapshot, delta, forwardDelta, countFlow, resetOwners := s.updateSnapshot(now, flow.Flow, classified, process, prev, exists, baselineReady, lineageContinues)
		nextSnapshots[flowID] = snapshot
		if snapshot.Direction != model.DirectionForward && snapshot.PID > 0 {
			s.rememberProcessHint(snapshot, now)
		}
		if resetOwners {
			s.releaseFlowOwnership(flowID)
		}

		if classified.Direction == model.DirectionForward {
			if forwardDelta != nil || countFlow {
				deltaValue := deltaPair{}
				if forwardDelta != nil {
					deltaValue = *forwardDelta
				}
				s.addForwardUsage(flowID, classified, deltaValue, countFlow)
			}
			continue
		}
		if delta != nil || countFlow {
			deltaValue := deltaPair{}
			if delta != nil {
				deltaValue = *delta
			}
			s.addUsage(flowID, snapshot, deltaValue, countFlow)
		}
	}
	s.replaceSnapshots(nextSnapshots)
	return nil
}

func (s *Service) recordTickMetrics(metrics tickMetrics) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.lastTickMetrics = metrics
}

func (s *Service) collectInterfaceDeltas() error {
	counters, err := ReadInterfaceCounters(s.cfg.ProcFS)
	if err != nil {
		return err
	}
	eligible := s.eligibleInterfaces()
	if s.interfaceCounters == nil {
		s.interfaceCounters = make(map[string]interfaceCounter, len(counters))
	}
	if s.interfaceBuckets == nil {
		s.interfaceBuckets = make(map[string]model.InterfaceUsageDelta)
	}
	for _, counter := range counters {
		if !isEligibleInterface(counter.Name, eligible) {
			continue
		}
		if previous, ok := s.interfaceCounters[counter.Name]; ok {
			rxBytes, rxOK := representableCounterDelta(counter.RxBytes, previous.RxBytes)
			txBytes, txOK := representableCounterDelta(counter.TxBytes, previous.TxBytes)
			if !rxOK || !txOK {
				// SQLite stores signed 64-bit counters. Treat an unrepresentable
				// kernel delta as a corrupt sample, advance the baseline below, and
				// resume with the next representable increment.
				s.interfaceCounters[counter.Name] = counter
				continue
			}
			delta := model.InterfaceUsageDelta{
				RxBytes: rxBytes,
				TxBytes: txBytes,
			}
			if delta.RxBytes != 0 || delta.TxBytes != 0 {
				current := s.interfaceBuckets[counter.Name]
				current.RxBytes += delta.RxBytes
				current.TxBytes += delta.TxBytes
				s.interfaceBuckets[counter.Name] = current
			}
		}
		s.interfaceCounters[counter.Name] = counter
	}
	return nil
}

func eligibleInterfaces() map[string]struct{} {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	result := make(map[string]struct{}, len(interfaces))
	for _, iface := range interfaces {
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}
		result[iface.Name] = struct{}{}
	}
	return result
}

func (s *Service) eligibleInterfaces() map[string]struct{} {
	if len(s.cfg.NetworkInterfaces) == 0 {
		return eligibleInterfaces()
	}
	result := make(map[string]struct{}, len(s.cfg.NetworkInterfaces))
	for _, name := range s.cfg.NetworkInterfaces {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		result[name] = struct{}{}
	}
	return result
}

func isEligibleInterface(name string, eligible map[string]struct{}) bool {
	if name == "" || name == "lo" {
		return false
	}
	if eligible == nil {
		return true
	}
	_, ok := eligible[name]
	return ok
}

func (s *Service) socketIndexSnapshot(now time.Time) (socketIndex, error) {
	s.runtimeMu.RLock()
	if ttl := s.socketIndexTTL(); s.socketReady && ttl > 0 && now.Sub(s.lastSockRefresh) < ttl {
		index := s.socketIndex
		s.runtimeMu.RUnlock()
		return index, nil
	}
	socketReady := s.socketReady
	cachedIndex := s.socketIndex
	s.runtimeMu.RUnlock()

	index, err := ReadSocketIndex(s.cfg.ProcFS)
	if err != nil {
		if socketReady {
			return cachedIndex, nil
		}
		return socketIndex{}, err
	}

	s.runtimeMu.Lock()
	s.socketIndex = index
	s.lastSockRefresh = now
	s.socketReady = true
	s.runtimeMu.Unlock()
	return index, nil
}

func (s *Service) socketIndexTTL() time.Duration {
	if s.cfg.SocketIndexInterval > 0 {
		return s.cfg.SocketIndexInterval
	}
	return defaultSocketIndexTTL
}

func (s *Service) carryForwardSnapshots(prev map[string]model.FlowSnapshot, now time.Time) map[string]model.FlowSnapshot {
	if len(prev) == 0 {
		return make(map[string]model.FlowSnapshot)
	}

	next := make(map[string]model.FlowSnapshot, len(prev))
	for key, snapshot := range prev {
		if now.Sub(snapshot.LastSeen) > defaultFlowGraceTTL {
			continue
		}
		next[key] = snapshot
	}
	return next
}

func (s *Service) snapshotsForTick(now time.Time, observed []observedFlow) (map[string]model.FlowSnapshot, bool, map[string]struct{}) {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()

	next := make(map[string]model.FlowSnapshot, len(s.snapshots))
	expiredOwnership := make(map[string]struct{})
	for flowID, snapshot := range s.snapshots {
		if now.Sub(snapshot.LastSeen) <= defaultFlowGraceTTL {
			next[flowID] = snapshot
		} else {
			expiredOwnership[flowID] = struct{}{}
		}
	}
	for _, flow := range observed {
		flowID := string(flow.Identity)
		if _, exists := next[flowID]; exists {
			continue
		}
		if snapshot, exists := s.snapshots[flowID]; exists {
			if (!snapshot.SyntheticCTID && !flow.Flow.SyntheticCTID) || syntheticCountersContinue(snapshot, flow.Flow) {
				next[flowID] = snapshot
				delete(expiredOwnership, flowID)
			} else {
				expiredOwnership[flowID] = struct{}{}
			}
		}
	}
	return next, s.snapshotReady, expiredOwnership
}

func syntheticCountersContinue(previous model.FlowSnapshot, current model.ConntrackFlow) bool {
	if !previous.SyntheticCTID || !current.SyntheticCTID || !current.HasAccounting || previous.CTID == 0 || previous.CTID != current.CTID {
		return false
	}
	return current.OrigBytes >= previous.LastOrig &&
		current.ReplyBytes >= previous.LastReply &&
		current.OrigPkts >= previous.LastOPkts &&
		current.ReplyPkts >= previous.LastRPkts
}

func hasConntrackObservationAccounting(flows []conntrackObservation) bool {
	for _, flow := range flows {
		if flow.Flow.HasAccounting {
			return true
		}
	}
	return false
}

func (s *Service) updateSnapshot(
	now time.Time,
	flow model.ConntrackFlow,
	classified classifiedFlow,
	process model.ProcessInfo,
	prev model.FlowSnapshot,
	exists bool,
	baselineReady bool,
	lineageContinues bool,
) (model.FlowSnapshot, *deltaPair, *deltaPair, bool, bool) {
	attribution := model.AttributionUnknown
	pid := 0
	comm := ""
	exe := ""
	if classified.Direction != model.DirectionForward {
		if process.PID > 0 {
			switch {
			case process.Ambiguous:
				attribution = model.AttributionHeuristic
			case classified.MatchedByHint:
				attribution = model.AttributionGuess
			case classified.MatchedByLocal:
				attribution = model.AttributionHeuristic
			case flow.Proto == "udp" && !classified.Connected:
				attribution = model.AttributionHeuristic
			default:
				attribution = model.AttributionExact
			}
			pid = process.PID
			comm = process.Comm
			exe = process.Exe
		} else if exists && prev.PID > 0 {
			sameTuple := prev.Proto == flow.Proto &&
				prev.Direction == classified.Direction &&
				prev.LocalIP == classified.LocalIP &&
				prev.LocalPort == classified.LocalPort &&
				prev.RemoteIP == classified.RemoteIP &&
				prev.RemotePort == classified.RemotePort
			if sameTuple {
				attribution = model.AttributionGuess
				pid = prev.PID
				comm = prev.Comm
				exe = prev.Exe
			}
		}
	}

	localIP := classified.LocalIP
	remoteIP := classified.RemoteIP
	localPort := classified.LocalPort
	remotePort := classified.RemotePort
	if classified.Direction == model.DirectionForward {
		localIP = ""
		remoteIP = ""
		localPort = 0
		remotePort = 0
	}

	snapshot := model.FlowSnapshot{
		CTID:          flow.CTID,
		SyntheticCTID: flow.SyntheticCTID,
		Proto:         flow.Proto,
		LocalIP:       localIP,
		LocalPort:     localPort,
		RemoteIP:      remoteIP,
		RemotePort:    remotePort,
		Direction:     classified.Direction,
		PID:           pid,
		Comm:          comm,
		Exe:           exe,
		Attribution:   attribution,
		StartedAt:     now,
		BaselineOrig:  flow.OrigBytes,
		BaselineReply: flow.ReplyBytes,
		LastOrig:      flow.OrigBytes,
		LastReply:     flow.ReplyBytes,
		BaselineOPkts: flow.OrigPkts,
		BaselineRPkts: flow.ReplyPkts,
		LastOPkts:     flow.OrigPkts,
		LastRPkts:     flow.ReplyPkts,
		Counted:       false,
		LastSeen:      now,
	}

	if !exists {
		if !baselineReady {
			snapshot.Counted = true
			return snapshot, nil, nil, false, false
		}
		if flow.SyntheticCTID {
			// Tuple-derived IDs cannot distinguish a connection that appeared after
			// startup from a long-lived connection missed by the first baseline
			// read. Seed its counters and count the flow, but never charge the full
			// kernel lifetime as traffic. Subsequent ticks use monotonic deltas, and
			// an observed rollback below still proves a genuinely new lineage.
			snapshot.Counted = true
			return snapshot, nil, nil, true, false
		}
		snapshot.Counted = true
		delta, representable := representableFlowDelta(classified.Direction, flow.OrigBytes, flow.ReplyBytes, flow.OrigPkts, flow.ReplyPkts)
		if !representable {
			return snapshot, nil, nil, true, false
		}
		if classified.Direction == model.DirectionForward {
			return snapshot, nil, &delta, true, false
		}
		return snapshot, &delta, nil, true, false
	}

	// Older kernels may omit id=. Their tuple-derived fallback cannot prove
	// lineage across tuple reuse; a counter rollback is positive evidence that a
	// new connection replaced the previous one, even inside the short grace TTL.
	if flow.SyntheticCTID && lineageContinues && (flow.OrigBytes < prev.LastOrig || flow.ReplyBytes < prev.LastReply || flow.OrigPkts < prev.LastOPkts || flow.ReplyPkts < prev.LastRPkts) {
		lineageContinues = false
	}
	newLineage := exists && !lineageContinues
	if newLineage || snapshotTupleChanged(prev, snapshot) || flow.OrigBytes < prev.LastOrig || flow.ReplyBytes < prev.LastReply {
		if lineageContinues {
			// Reclassification or transient counter anomalies on the same conntrack
			// lineage should not double-count the flow, but the caller still needs
			// a zero-delta update so an already-counted flow can migrate to the new
			// bucket when direction/tuple attribution becomes clearer mid-minute.
			snapshot.Counted = prev.Counted
			zero := &deltaPair{}
			if classified.Direction == model.DirectionForward {
				return snapshot, nil, zero, false, false
			}
			return snapshot, zero, nil, false, false
		}
		snapshot.Counted = true
		if !baselineReady {
			return snapshot, nil, nil, false, true
		}
		delta, representable := representableFlowDelta(classified.Direction, flow.OrigBytes, flow.ReplyBytes, flow.OrigPkts, flow.ReplyPkts)
		if !representable {
			return snapshot, nil, nil, true, true
		}
		if classified.Direction == model.DirectionForward {
			return snapshot, nil, &delta, true, true
		}
		return snapshot, &delta, nil, true, true
	}

	snapshot.StartedAt = prev.StartedAt
	snapshot.BaselineOrig = prev.BaselineOrig
	snapshot.BaselineReply = prev.BaselineReply
	snapshot.BaselineOPkts = prev.BaselineOPkts
	snapshot.BaselineRPkts = prev.BaselineRPkts
	snapshot.Counted = prev.Counted

	origDelta := clampDelta(flow.OrigBytes, prev.LastOrig)
	replyDelta := clampDelta(flow.ReplyBytes, prev.LastReply)
	origPktDelta := clampDelta(flow.OrigPkts, prev.LastOPkts)
	replyPktDelta := clampDelta(flow.ReplyPkts, prev.LastRPkts)
	delta, representable := representableFlowDelta(classified.Direction, origDelta, replyDelta, origPktDelta, replyPktDelta)
	if !representable {
		return snapshot, nil, nil, false, false
	}

	if classified.Direction == model.DirectionForward {
		return snapshot, nil, &delta, false, false
	}
	return snapshot, &delta, nil, false, false
}

type deltaPair struct {
	upBytes   int64
	downBytes int64
	upPkts    int64
	downPkts  int64
}

func (d deltaPair) isZero() bool {
	return d.upBytes == 0 && d.downBytes == 0 && d.upPkts == 0 && d.downPkts == 0
}

type flowContribution struct {
	delta     deltaPair
	flowCount int64
}

func (c flowContribution) isZero() bool {
	return c.delta.isZero() && c.flowCount == 0
}

func clampDelta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

func representableCounterDelta(current, previous uint64) (int64, bool) {
	return representableUint64(clampDelta(current, previous))
}

func representableFlowDelta(direction model.Direction, origBytes, replyBytes, origPkts, replyPkts uint64) (deltaPair, bool) {
	origBytesValue, origBytesOK := representableUint64(origBytes)
	replyBytesValue, replyBytesOK := representableUint64(replyBytes)
	origPktsValue, origPktsOK := representableUint64(origPkts)
	replyPktsValue, replyPktsOK := representableUint64(replyPkts)
	if !origBytesOK || !replyBytesOK || !origPktsOK || !replyPktsOK {
		return deltaPair{}, false
	}
	if direction == model.DirectionIn {
		return deltaPair{
			upBytes:   replyBytesValue,
			downBytes: origBytesValue,
			upPkts:    replyPktsValue,
			downPkts:  origPktsValue,
		}, true
	}
	return deltaPair{
		upBytes:   origBytesValue,
		downBytes: replyBytesValue,
		upPkts:    origPktsValue,
		downPkts:  replyPktsValue,
	}, true
}

func representableUint64(value uint64) (int64, bool) {
	if value > uint64(math.MaxInt64) {
		return 0, false
	}
	return int64(value), true
}

func snapshotTupleChanged(prev, next model.FlowSnapshot) bool {
	return prev.Proto != next.Proto ||
		prev.Direction != next.Direction ||
		prev.LocalIP != next.LocalIP ||
		prev.LocalPort != next.LocalPort ||
		prev.RemoteIP != next.RemoteIP ||
		prev.RemotePort != next.RemotePort
}

func observedLineageContinues(prev model.FlowSnapshot, selectedCTID uint64, selectedSynthetic bool, rawIDs map[uint64]struct{}) bool {
	if prev.CTID == 0 || len(rawIDs) == 0 {
		return false
	}
	if prev.SyntheticCTID != selectedSynthetic {
		return false
	}
	if selectedCTID != 0 && selectedCTID != prev.CTID {
		return false
	}
	_, ok := rawIDs[prev.CTID]
	return ok
}

func (s *Service) addUsage(flowID string, snapshot model.FlowSnapshot, delta deltaPair, countFlow bool) {
	s.ensureFlowOwners()

	key := model.UsageKey{
		MinuteTS:    s.currentMinute.Unix(),
		Proto:       snapshot.Proto,
		Direction:   snapshot.Direction,
		PID:         snapshot.PID,
		Comm:        snapshot.Comm,
		Exe:         snapshot.Exe,
		LocalPort:   snapshot.LocalPort,
		RemoteIP:    snapshot.RemoteIP,
		RemotePort:  snapshot.RemotePort,
		Attribution: snapshot.Attribution,
	}

	state := s.buckets[key]
	ensureState := func() *bucketState {
		if state == nil {
			state = newBucketState()
			s.buckets[key] = state
		}
		return state
	}

	if previousForwardKey, ok := s.fwdFlowOwner[flowID]; ok {
		if previousForwardState := s.forwardBuckets[previousForwardKey]; previousForwardState != nil {
			ensureState().applyContribution(flowID, previousForwardState.detachFlow(flowID))
			if previousForwardState.empty() {
				delete(s.forwardBuckets, previousForwardKey)
			}
		}
		delete(s.fwdFlowOwner, flowID)
	}
	if previousUsageKey, ok := s.usageFlowOwner[flowID]; ok && previousUsageKey != key {
		// Reclassification within the same minute must migrate the flow's existing
		// bytes/packets and one-time flow_count to the new bucket instead of
		// counting it again.
		if previousUsageState := s.buckets[previousUsageKey]; previousUsageState != nil {
			ensureState().applyContribution(flowID, previousUsageState.detachFlow(flowID))
			if previousUsageState.empty() {
				delete(s.buckets, previousUsageKey)
			}
		}
	}

	contribution := flowContribution{delta: delta}
	if countFlow {
		contribution.flowCount = 1
	}
	if !contribution.isZero() {
		ensureState().applyContribution(flowID, contribution)
	}
	s.usageFlowOwner[flowID] = key
}

func (s *Service) releaseFlowOwnership(flowID string) {
	s.ensureFlowOwners()
	usageKey, ownsUsage := s.usageFlowOwner[flowID]
	forwardKey, ownsForward := s.fwdFlowOwner[flowID]
	if ownsUsage || ownsForward {
		s.frozenFlowSequence++
		frozenFlowID := fmt.Sprintf("%s\x00closed:%d", flowID, s.frozenFlowSequence)
		if ownsUsage {
			s.buckets[usageKey].freezeFlow(flowID, frozenFlowID)
		}
		if ownsForward {
			s.forwardBuckets[forwardKey].freezeFlow(flowID, frozenFlowID)
		}
	}
	delete(s.usageFlowOwner, flowID)
	delete(s.fwdFlowOwner, flowID)
}

func (s *Service) addForwardUsage(flowID string, classified classifiedFlow, delta deltaPair, countFlow bool) {
	s.ensureFlowOwners()

	key := model.ForwardUsageKey{
		MinuteTS:  s.currentMinute.Unix(),
		Proto:     classified.Proto,
		OrigSrcIP: classified.OrigSrcIP,
		OrigDstIP: classified.OrigDstIP,
		OrigSPort: classified.OrigSrcPort,
		OrigDPort: classified.OrigDstPort,
	}

	state := s.forwardBuckets[key]
	ensureState := func() *bucketState {
		if state == nil {
			state = newBucketState()
			s.forwardBuckets[key] = state
		}
		return state
	}

	if previousUsageKey, ok := s.usageFlowOwner[flowID]; ok {
		if previousUsageState := s.buckets[previousUsageKey]; previousUsageState != nil {
			ensureState().applyContribution(flowID, previousUsageState.detachFlow(flowID))
			if previousUsageState.empty() {
				delete(s.buckets, previousUsageKey)
			}
		}
		delete(s.usageFlowOwner, flowID)
	}
	if previousForwardKey, ok := s.fwdFlowOwner[flowID]; ok && previousForwardKey != key {
		if previousForwardState := s.forwardBuckets[previousForwardKey]; previousForwardState != nil {
			ensureState().applyContribution(flowID, previousForwardState.detachFlow(flowID))
			if previousForwardState.empty() {
				delete(s.forwardBuckets, previousForwardKey)
			}
		}
	}

	contribution := flowContribution{delta: delta}
	if countFlow {
		contribution.flowCount = 1
	}
	if !contribution.isZero() {
		ensureState().applyContribution(flowID, contribution)
	}
	s.fwdFlowOwner[flowID] = key
}

func (s *Service) flushCurrentBuckets(ctx context.Context) error {
	s.ensureFlowOwners()

	if len(s.buckets) == 0 && len(s.forwardBuckets) == 0 && len(s.interfaceBuckets) == 0 {
		s.usageFlowOwner = make(map[string]model.UsageKey)
		s.fwdFlowOwner = make(map[string]model.ForwardUsageKey)
		return nil
	}

	usage := make(map[model.UsageKey]model.UsageDelta, len(s.buckets))
	for key, state := range s.buckets {
		if state == nil || state.empty() {
			continue
		}
		usage[key] = model.UsageDelta{
			BytesUp:   state.bytesUp,
			BytesDown: state.bytesDown,
			PktsUp:    state.pktsUp,
			PktsDown:  state.pktsDown,
			FlowCount: state.flowCount,
		}
	}

	forward := make(map[model.ForwardUsageKey]model.UsageDelta, len(s.forwardBuckets))
	for key, state := range s.forwardBuckets {
		if state == nil || state.empty() {
			continue
		}
		forward[key] = model.UsageDelta{
			BytesUp:   state.bytesUp,
			BytesDown: state.bytesDown,
			PktsUp:    state.pktsUp,
			PktsDown:  state.pktsDown,
			FlowCount: state.flowCount,
		}
	}

	if err := s.store.FlushMinuteBatch(ctx, s.currentMinute.Unix(), usage, forward, s.interfaceBuckets); err != nil {
		return err
	}
	s.buckets = make(map[model.UsageKey]*bucketState)
	s.forwardBuckets = make(map[model.ForwardUsageKey]*bucketState)
	s.interfaceBuckets = make(map[string]model.InterfaceUsageDelta)
	s.usageFlowOwner = make(map[string]model.UsageKey)
	s.fwdFlowOwner = make(map[string]model.ForwardUsageKey)
	return nil
}

func (s *Service) ensureFlowOwners() {
	if s.usageFlowOwner == nil {
		s.usageFlowOwner = make(map[string]model.UsageKey)
	}
	if s.fwdFlowOwner == nil {
		s.fwdFlowOwner = make(map[string]model.ForwardUsageKey)
	}
}

func newBucketState() *bucketState {
	return &bucketState{
		perFlow: make(map[string]flowContribution),
	}
}

func (b *bucketState) applyContribution(flowID string, contribution flowContribution) {
	if b == nil || contribution.isZero() {
		return
	}
	if b.perFlow == nil {
		b.perFlow = make(map[string]flowContribution)
	}

	// Keep per-flow contributions so we can move an already-counted flow between
	// buckets when attribution or direction becomes clearer later in the minute.
	current := b.perFlow[flowID]
	current.delta.upBytes += contribution.delta.upBytes
	current.delta.downBytes += contribution.delta.downBytes
	current.delta.upPkts += contribution.delta.upPkts
	current.delta.downPkts += contribution.delta.downPkts
	current.flowCount += contribution.flowCount
	b.perFlow[flowID] = current

	b.bytesUp += contribution.delta.upBytes
	b.bytesDown += contribution.delta.downBytes
	b.pktsUp += contribution.delta.upPkts
	b.pktsDown += contribution.delta.downPkts
	b.flowCount += contribution.flowCount
}

func (b *bucketState) detachFlow(flowID string) flowContribution {
	if b == nil {
		return flowContribution{}
	}
	if b.perFlow == nil {
		return flowContribution{}
	}

	contribution := b.perFlow[flowID]
	delete(b.perFlow, flowID)

	b.bytesUp -= contribution.delta.upBytes
	b.bytesDown -= contribution.delta.downBytes
	b.pktsUp -= contribution.delta.upPkts
	b.pktsDown -= contribution.delta.downPkts
	b.flowCount -= contribution.flowCount
	return contribution
}

// freezeFlow gives a completed connection generation an immutable internal key.
// The aggregate totals stay unchanged, while a later connection that reuses the
// same tuple can still be reclassified without moving the completed generation.
func (b *bucketState) freezeFlow(flowID, frozenFlowID string) {
	if b == nil || b.perFlow == nil {
		return
	}
	contribution, exists := b.perFlow[flowID]
	if !exists {
		return
	}
	delete(b.perFlow, flowID)
	b.perFlow[frozenFlowID] = contribution
}

func (b *bucketState) empty() bool {
	if b == nil {
		return true
	}
	return len(b.perFlow) == 0 && b.bytesUp == 0 && b.bytesDown == 0 && b.pktsUp == 0 && b.pktsDown == 0 && b.flowCount == 0
}

func (s *Service) refreshLocalIPs() error {
	interfaces, err := net.Interfaces()
	if err != nil {
		return err
	}
	localIPs := make(map[string]struct{})
	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				continue
			}
			localIPs[ip.String()] = struct{}{}
		}
	}
	s.runtimeMu.Lock()
	s.localIPs = localIPs
	s.lastIPRefresh = time.Now().UTC()
	s.runtimeMu.Unlock()
	return nil
}

func (s *Service) localIPSnapshot() map[string]struct{} {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()

	copied := make(map[string]struct{}, len(s.localIPs))
	for ip := range s.localIPs {
		copied[ip] = struct{}{}
	}
	return copied
}

func (s *Service) localIPCount() int {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return len(s.localIPs)
}

func (s *Service) lastIPRefreshTime() time.Time {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return s.lastIPRefresh
}

func (s *Service) ActiveProcesses() []model.ProcessListItem {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()

	byPID := make(map[int]model.ProcessListItem)
	for _, snapshot := range s.snapshots {
		if snapshot.PID <= 0 {
			continue
		}
		byPID[snapshot.PID] = model.ProcessListItem{
			PID:  snapshot.PID,
			Comm: snapshot.Comm,
			Exe:  snapshot.Exe,
		}
	}

	processes := make([]model.ProcessListItem, 0, len(byPID))
	for _, item := range byPID {
		processes = append(processes, item)
	}
	sort.Slice(processes, func(i, j int) bool {
		if processes[i].Comm == processes[j].Comm {
			return processes[i].PID < processes[j].PID
		}
		return processes[i].Comm < processes[j].Comm
	})
	return processes
}

func (s *Service) ActiveStats() model.ActiveStats {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()

	byPID := make(map[int]struct{})
	for _, snapshot := range s.snapshots {
		if snapshot.PID > 0 {
			byPID[snapshot.PID] = struct{}{}
		}
	}
	return model.ActiveStats{
		Connections: int64(len(s.snapshots)),
		Processes:   int64(len(byPID)),
	}
}

func (s *Service) Diagnostics() model.CollectorDiagnostics {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()

	byPID := make(map[int]struct{})
	attributionCounts := map[string]int64{
		string(model.AttributionExact):     0,
		string(model.AttributionHeuristic): 0,
		string(model.AttributionGuess):     0,
		string(model.AttributionUnknown):   0,
	}
	var unresolvedLocal int64
	for _, snapshot := range s.snapshots {
		if snapshot.PID > 0 {
			byPID[snapshot.PID] = struct{}{}
		}
		if snapshot.Direction != model.DirectionForward {
			attribution := string(snapshot.Attribution)
			if attribution == "" {
				attribution = string(model.AttributionUnknown)
			}
			attributionCounts[attribution]++
			if snapshot.PID <= 0 {
				unresolvedLocal++
			}
		}
	}

	now := time.Now().UTC()
	var lastSuccessfulTickTS *int64
	var lastSuccessfulTickAge *int64
	collectionFresh := collectionIsFresh(now, s.lastSuccessfulTick, s.cfg.TickInterval)
	if !s.lastSuccessfulTick.IsZero() {
		ts := s.lastSuccessfulTick.Unix()
		ageDuration := now.Sub(s.lastSuccessfulTick)
		if ageDuration < 0 {
			ageDuration = 0
		}
		age := int64(ageDuration.Seconds())
		lastSuccessfulTickTS = &ts
		lastSuccessfulTickAge = &age
	}
	var socketAge *int64
	if s.socketReady && !s.lastSockRefresh.IsZero() {
		age := int64(now.Sub(s.lastSockRefresh).Seconds())
		socketAge = &age
	}
	var localIPAge *int64
	if !s.lastIPRefresh.IsZero() {
		age := int64(now.Sub(s.lastIPRefresh).Seconds())
		localIPAge = &age
	}

	return model.CollectorDiagnostics{
		ActiveConnections:          int64(len(s.snapshots)),
		ActiveProcesses:            int64(len(byPID)),
		UnresolvedLocalConnections: unresolvedLocal,
		AttributionCounts:          attributionCounts,
		SnapshotReady:              s.snapshotReady,
		CollectionFresh:            collectionFresh,
		LastSuccessfulTickTS:       lastSuccessfulTickTS,
		LastSuccessfulTickAgeSecs:  lastSuccessfulTickAge,
		ConsecutiveTickFailures:    s.consecutiveTickFailures,
		SocketIndexReady:           s.socketReady,
		SocketIndexAgeSeconds:      socketAge,
		LocalIPCount:               len(s.localIPs),
		LocalIPAgeSeconds:          localIPAge,
		ProcessHintCount:           len(s.processHints),
		LastTickDurationMS:         s.lastTickMetrics.tickDuration.Milliseconds(),
		LastConntrackReadMS:        s.lastTickMetrics.conntrackRead.Milliseconds(),
		LastSocketIndexMS:          s.lastTickMetrics.socketIndexLookup.Milliseconds(),
		LastResolverMS:             s.lastTickMetrics.processResolve.Milliseconds(),
		LastRawFlowCount:           s.lastTickMetrics.rawFlowCount,
		LastObservedFlowCount:      s.lastTickMetrics.observedFlowCount,
		SocketIndexEntryCount:      s.lastTickMetrics.socketIndexEntries,
	}
}

func (s *Service) ReadyStatus() model.CollectorReadiness {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return model.CollectorReadiness{
		SnapshotReady:   s.snapshotReady,
		CollectionFresh: collectionIsFresh(time.Now().UTC(), s.lastSuccessfulTick, s.cfg.TickInterval),
	}
}

func collectionIsFresh(now, lastSuccessfulTick time.Time, tickInterval time.Duration) bool {
	if lastSuccessfulTick.IsZero() {
		return false
	}
	age := now.Sub(lastSuccessfulTick)
	if age < 0 {
		age = 0
	}
	if tickInterval <= 0 {
		tickInterval = config.Default().TickInterval
	}
	staleAfter := time.Duration(1<<63 - 1)
	if tickInterval <= staleAfter/3 {
		staleAfter = 3 * tickInterval
	}
	if staleAfter < 10*time.Second {
		staleAfter = 10 * time.Second
	}
	return age <= staleAfter
}

func (s *Service) replaceSnapshots(next map[string]model.FlowSnapshot) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.snapshots = next
	s.snapshotReady = true
}

func hintKey(proto string, direction model.Direction, localIP string, localPort int, remoteIP string, remotePort int) string {
	return fmt.Sprintf("%s|%s|%s|%d|%s|%d", proto, direction, localIP, localPort, remoteIP, remotePort)
}

func (s *Service) lookupProcessHint(classified classifiedFlow, now time.Time) (model.ProcessInfo, bool) {
	if classified.Direction == model.DirectionForward || classified.LocalPort <= 0 || classified.LocalIP == "" || classified.RemoteIP == "" || classified.RemotePort <= 0 {
		return model.ProcessInfo{}, false
	}
	key := hintKey(classified.Proto, classified.Direction, classified.LocalIP, classified.LocalPort, classified.RemoteIP, classified.RemotePort)
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	hint, ok := s.processHints[key]
	if !ok {
		return model.ProcessInfo{}, false
	}
	if !hint.expires.After(now) {
		delete(s.processHints, key)
		return model.ProcessInfo{}, false
	}
	return hint.process, true
}

func (s *Service) rememberProcessHint(snapshot model.FlowSnapshot, now time.Time) {
	if snapshot.LocalPort <= 0 || snapshot.LocalIP == "" || snapshot.PID <= 0 || snapshot.RemoteIP == "" || snapshot.RemotePort <= 0 {
		return
	}
	key := hintKey(snapshot.Proto, snapshot.Direction, snapshot.LocalIP, snapshot.LocalPort, snapshot.RemoteIP, snapshot.RemotePort)
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.processHints[key] = processHint{
		process: model.ProcessInfo{PID: snapshot.PID, Comm: snapshot.Comm, Exe: snapshot.Exe},
		expires: now.Add(defaultProcessHintTTL),
	}
}

func (s *Service) pruneProcessHints(now time.Time) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	for key, hint := range s.processHints {
		if !hint.expires.After(now) {
			delete(s.processHints, key)
		}
	}
}

func attachSocketMetadata(index socketIndex, classified classifiedFlow) (classifiedFlow, bool) {
	if classified.Direction == model.DirectionForward || !classified.Tuple.valid() {
		return classified, false
	}

	if sock, ok := index.ByTuple[classified.Tuple.key()]; ok && sock.Present {
		classified.Connected = sock.Connected
		if sock.Inode > 0 {
			classified.Inode = sock.Inode
			return classified, true
		}
	}

	if sock, ok := lookupLocalSocketFallback(index, classified); ok {
		classified.Inode = sock.Inode
		classified.Connected = sock.Connected
		classified.MatchedByLocal = true
		return classified, true
	}

	return classified, false
}

func lookupLocalSocketFallback(index socketIndex, classified classifiedFlow) (socketEntry, bool) {
	if classified.LocalPort <= 0 || classified.Proto == "" {
		return socketEntry{}, false
	}

	allowFallback := classified.Proto == "udp" || (classified.Proto == "tcp" && classified.Direction == model.DirectionIn)
	if !allowFallback {
		return socketEntry{}, false
	}

	lookup := func(localIP string) (socketEntry, bool) {
		if localIP == "" {
			return socketEntry{}, false
		}
		key := localTupleKey(classified.Proto, localIP, classified.LocalPort)
		sock, ok := index.ByLocal[key]
		if !ok || !sock.Present || sock.Inode == 0 {
			return socketEntry{}, false
		}
		return sock, true
	}

	if sock, ok := lookup(classified.LocalIP); ok {
		return sock, true
	}

	if classified.Proto == "tcp" && classified.Direction == model.DirectionIn {
		for _, anyAddr := range []string{"0.0.0.0", "::"} {
			if sock, ok := lookup(anyAddr); ok {
				return sock, true
			}
		}
	}

	return socketEntry{}, false
}
