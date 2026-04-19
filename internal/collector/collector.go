package collector

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"sort"
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
}

type Service struct {
	cfg            config.Config
	store          *store.Store
	logger         *log.Logger
	resolver       *processResolver
	currentMinute  time.Time
	buckets        map[model.UsageKey]*bucketState
	forwardBuckets map[model.ForwardUsageKey]*bucketState
	usageFlowOwner map[uint64]model.UsageKey
	fwdFlowOwner   map[uint64]model.ForwardUsageKey
	runtimeMu      sync.RWMutex
	snapshots      map[uint64]model.FlowSnapshot
	snapshotReady  bool
	processHints   map[string]processHint
	localIPs       map[string]struct{}
	lastIPRefresh  time.Time
	warnedNoAcct   bool
}

type bucketState struct {
	bytesUp   int64
	bytesDown int64
	pktsUp    int64
	pktsDown  int64
	flowCount int64
	flows     map[uint64]struct{}
	perFlow   map[uint64]flowContribution
}

const defaultProcessHintTTL = 90 * time.Second
const shutdownFlushTimeout = 5 * time.Second

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
		cfg:            cfg,
		store:          trafficStore,
		logger:         logger,
		resolver:       newProcessResolver(cfg.ProcFS),
		buckets:        make(map[model.UsageKey]*bucketState),
		forwardBuckets: make(map[model.ForwardUsageKey]*bucketState),
		usageFlowOwner: make(map[uint64]model.UsageKey),
		fwdFlowOwner:   make(map[uint64]model.ForwardUsageKey),
		snapshots:      make(map[uint64]model.FlowSnapshot),
		processHints:   make(map[string]processHint),
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

func (s *Service) Start(ctx context.Context) error {
	s.currentMinute = time.Now().UTC().Truncate(time.Minute)
	if err := s.refreshLocalIPs(); err != nil {
		s.logger.Printf("refresh local IPs: %v", err)
	}
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	for {
		if err := s.runTick(ctx, time.Now().UTC()); err != nil {
			s.logger.Printf("collector tick failed: %v", err)
		}

		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
			if err := s.flushCurrentBuckets(flushCtx); err != nil {
				s.logger.Printf("flush buckets on shutdown failed: %v", err)
			}
			cancel()
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Service) runTick(ctx context.Context, now time.Time) error {
	minute := now.Truncate(time.Minute)
	if !minute.Equal(s.currentMinute) {
		if err := s.flushCurrentBuckets(ctx); err != nil {
			return err
		}
		s.currentMinute = minute
	}

	if now.Sub(s.lastIPRefresh) >= 30*time.Second || len(s.localIPs) == 0 {
		if err := s.refreshLocalIPs(); err != nil {
			s.logger.Printf("refresh local IPs: %v", err)
		}
	}

	flows, err := ReadConntrackSnapshot(s.cfg.ConntrackPath)
	if err != nil {
		return err
	}
	if len(flows) > 0 && !s.warnedNoAcct && !hasConntrackAccounting(flows) {
		s.logger.Printf("conntrack accounting appears disabled; enable `sysctl -w net.netfilter.nf_conntrack_acct=1` to collect byte and packet counters")
		s.warnedNoAcct = true
	}
	socketIndex, err := ReadSocketIndex(s.cfg.ProcFS)
	if err != nil {
		return err
	}
	s.pruneProcessHints(now)

	inodesNeeded := make(map[uint64]struct{})
	flowMeta := make(map[uint64]classifiedFlow)
	for _, flow := range flows {
		classified := classifyFlow(flow, s.localIPs, socketIndex)
		classified, inodeResolved := attachSocketMetadata(socketIndex, classified)
		if inodeResolved && classified.Inode > 0 {
			inodesNeeded[classified.Inode] = struct{}{}
		}
		flowMeta[flow.CTID] = classified
	}

	processes := s.resolver.Resolve(ctx, inodesNeeded)
	prevSnapshots := s.snapshotCopy()
	baselineReady := s.isSnapshotReady()
	nextSnapshots := make(map[uint64]model.FlowSnapshot, len(flows))

	for _, flow := range flows {
		classified := flowMeta[flow.CTID]
		process := processes[classified.Inode]
		if process.PID <= 0 {
			if hinted, ok := s.lookupProcessHint(classified, now); ok {
				process = hinted
				classified.MatchedByHint = true
			}
		}

		prev, exists := prevSnapshots[flow.CTID]
		snapshot, delta, forwardDelta, countFlow := s.updateSnapshot(now, flow, classified, process, prev, exists, baselineReady)
		nextSnapshots[flow.CTID] = snapshot
		if snapshot.Direction != model.DirectionForward && snapshot.PID > 0 {
			s.rememberProcessHint(snapshot, now)
		}

		if classified.Direction == model.DirectionForward {
			if forwardDelta != nil || countFlow {
				deltaValue := deltaPair{}
				if forwardDelta != nil {
					deltaValue = *forwardDelta
				}
				s.addForwardUsage(snapshot.CTID, classified, deltaValue, countFlow)
			}
			continue
		}
		if delta != nil || countFlow {
			deltaValue := deltaPair{}
			if delta != nil {
				deltaValue = *delta
			}
			s.addUsage(snapshot.CTID, snapshot, deltaValue, countFlow)
		}
	}
	s.replaceSnapshots(nextSnapshots)
	return nil
}

func hasConntrackAccounting(flows []model.ConntrackFlow) bool {
	for _, flow := range flows {
		if flow.HasAccounting {
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
) (model.FlowSnapshot, *deltaPair, *deltaPair, bool) {
	attribution := model.AttributionUnknown
	pid := 0
	comm := ""
	exe := ""
	if classified.Direction != model.DirectionForward {
		if process.PID > 0 {
			switch {
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
			return snapshot, nil, nil, false
		}
		snapshot.Counted = true
		if classified.Direction == model.DirectionForward {
			return snapshot, nil, &deltaPair{
				upBytes:   int64(flow.OrigBytes),
				downBytes: int64(flow.ReplyBytes),
				upPkts:    int64(flow.OrigPkts),
				downPkts:  int64(flow.ReplyPkts),
			}, true
		}

		delta := deltaPair{}
		switch classified.Direction {
		case model.DirectionIn:
			delta.upBytes = int64(flow.ReplyBytes)
			delta.downBytes = int64(flow.OrigBytes)
			delta.upPkts = int64(flow.ReplyPkts)
			delta.downPkts = int64(flow.OrigPkts)
		default:
			delta.upBytes = int64(flow.OrigBytes)
			delta.downBytes = int64(flow.ReplyBytes)
			delta.upPkts = int64(flow.OrigPkts)
			delta.downPkts = int64(flow.ReplyPkts)
		}
		return snapshot, &delta, nil, true
	}

	if snapshotTupleChanged(prev, snapshot) || flow.OrigBytes < prev.LastOrig || flow.ReplyBytes < prev.LastReply {
		// Counter resets or tuple instability should not double-count the flow.
		snapshot.Counted = prev.Counted
		return snapshot, nil, nil, false
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

	if classified.Direction == model.DirectionForward {
		return snapshot, nil, &deltaPair{
			upBytes:   int64(origDelta),
			downBytes: int64(replyDelta),
			upPkts:    int64(origPktDelta),
			downPkts:  int64(replyPktDelta),
		}, false
	}

	delta := deltaPair{}
	switch classified.Direction {
	case model.DirectionIn:
		delta.upBytes = int64(replyDelta)
		delta.downBytes = int64(origDelta)
		delta.upPkts = int64(replyPktDelta)
		delta.downPkts = int64(origPktDelta)
	default:
		delta.upBytes = int64(origDelta)
		delta.downBytes = int64(replyDelta)
		delta.upPkts = int64(origPktDelta)
		delta.downPkts = int64(replyPktDelta)
	}
	return snapshot, &delta, nil, false
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

func snapshotTupleChanged(prev, next model.FlowSnapshot) bool {
	return prev.Proto != next.Proto ||
		prev.Direction != next.Direction ||
		prev.LocalIP != next.LocalIP ||
		prev.LocalPort != next.LocalPort ||
		prev.RemoteIP != next.RemoteIP ||
		prev.RemotePort != next.RemotePort
}

func (s *Service) addUsage(ctid uint64, snapshot model.FlowSnapshot, delta deltaPair, countFlow bool) {
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
	if state == nil {
		state = newBucketState()
		s.buckets[key] = state
	}

	if previousForwardKey, ok := s.fwdFlowOwner[ctid]; ok {
		if previousForwardState := s.forwardBuckets[previousForwardKey]; previousForwardState != nil {
			state.applyContribution(ctid, previousForwardState.detachFlow(ctid))
			if previousForwardState.empty() {
				delete(s.forwardBuckets, previousForwardKey)
			}
		}
		delete(s.fwdFlowOwner, ctid)
	}
	if previousUsageKey, ok := s.usageFlowOwner[ctid]; ok && previousUsageKey != key {
		// Reclassification within the same minute must migrate the flow's existing
		// bytes/packets and one-time flow_count to the new bucket instead of
		// counting it again.
		if previousUsageState := s.buckets[previousUsageKey]; previousUsageState != nil {
			state.applyContribution(ctid, previousUsageState.detachFlow(ctid))
			if previousUsageState.empty() {
				delete(s.buckets, previousUsageKey)
			}
		}
	}

	contribution := flowContribution{delta: delta}
	if countFlow {
		contribution.flowCount = 1
	}
	state.applyContribution(ctid, contribution)
	s.usageFlowOwner[ctid] = key
}

func (s *Service) addForwardUsage(ctid uint64, classified classifiedFlow, delta deltaPair, countFlow bool) {
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
	if state == nil {
		state = newBucketState()
		s.forwardBuckets[key] = state
	}

	if previousUsageKey, ok := s.usageFlowOwner[ctid]; ok {
		if previousUsageState := s.buckets[previousUsageKey]; previousUsageState != nil {
			state.applyContribution(ctid, previousUsageState.detachFlow(ctid))
			if previousUsageState.empty() {
				delete(s.buckets, previousUsageKey)
			}
		}
		delete(s.usageFlowOwner, ctid)
	}
	if previousForwardKey, ok := s.fwdFlowOwner[ctid]; ok && previousForwardKey != key {
		if previousForwardState := s.forwardBuckets[previousForwardKey]; previousForwardState != nil {
			state.applyContribution(ctid, previousForwardState.detachFlow(ctid))
			if previousForwardState.empty() {
				delete(s.forwardBuckets, previousForwardKey)
			}
		}
	}

	contribution := flowContribution{delta: delta}
	if countFlow {
		contribution.flowCount = 1
	}
	state.applyContribution(ctid, contribution)
	s.fwdFlowOwner[ctid] = key
}

func (s *Service) flushCurrentBuckets(ctx context.Context) error {
	s.ensureFlowOwners()

	if len(s.buckets) == 0 && len(s.forwardBuckets) == 0 {
		s.usageFlowOwner = make(map[uint64]model.UsageKey)
		s.fwdFlowOwner = make(map[uint64]model.ForwardUsageKey)
		return nil
	}

	usage := make(map[model.UsageKey]model.UsageDelta, len(s.buckets))
	for key, state := range s.buckets {
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
		forward[key] = model.UsageDelta{
			BytesUp:   state.bytesUp,
			BytesDown: state.bytesDown,
			PktsUp:    state.pktsUp,
			PktsDown:  state.pktsDown,
			FlowCount: state.flowCount,
		}
	}

	if err := s.store.FlushMinute(ctx, s.currentMinute.Unix(), usage, forward); err != nil {
		return err
	}
	s.buckets = make(map[model.UsageKey]*bucketState)
	s.forwardBuckets = make(map[model.ForwardUsageKey]*bucketState)
	s.usageFlowOwner = make(map[uint64]model.UsageKey)
	s.fwdFlowOwner = make(map[uint64]model.ForwardUsageKey)
	return nil
}

func (s *Service) ensureFlowOwners() {
	if s.usageFlowOwner == nil {
		s.usageFlowOwner = make(map[uint64]model.UsageKey)
	}
	if s.fwdFlowOwner == nil {
		s.fwdFlowOwner = make(map[uint64]model.ForwardUsageKey)
	}
}

func newBucketState() *bucketState {
	return &bucketState{
		flows:   make(map[uint64]struct{}),
		perFlow: make(map[uint64]flowContribution),
	}
}

func (b *bucketState) applyContribution(ctid uint64, contribution flowContribution) {
	if b == nil || contribution.isZero() {
		return
	}
	if b.flows == nil {
		b.flows = make(map[uint64]struct{})
	}
	if b.perFlow == nil {
		b.perFlow = make(map[uint64]flowContribution)
	}

	// Keep per-flow contributions so we can move an already-counted flow between
	// buckets when attribution or direction becomes clearer later in the minute.
	current := b.perFlow[ctid]
	current.delta.upBytes += contribution.delta.upBytes
	current.delta.downBytes += contribution.delta.downBytes
	current.delta.upPkts += contribution.delta.upPkts
	current.delta.downPkts += contribution.delta.downPkts
	current.flowCount += contribution.flowCount
	b.perFlow[ctid] = current

	b.bytesUp += contribution.delta.upBytes
	b.bytesDown += contribution.delta.downBytes
	b.pktsUp += contribution.delta.upPkts
	b.pktsDown += contribution.delta.downPkts
	b.flowCount += contribution.flowCount
	b.flows[ctid] = struct{}{}
}

func (b *bucketState) detachFlow(ctid uint64) flowContribution {
	if b == nil {
		return flowContribution{}
	}
	if b.perFlow == nil {
		delete(b.flows, ctid)
		return flowContribution{}
	}

	contribution := b.perFlow[ctid]
	delete(b.perFlow, ctid)
	delete(b.flows, ctid)

	b.bytesUp -= contribution.delta.upBytes
	b.bytesDown -= contribution.delta.downBytes
	b.pktsUp -= contribution.delta.upPkts
	b.pktsDown -= contribution.delta.downPkts
	b.flowCount -= contribution.flowCount
	return contribution
}

func (b *bucketState) empty() bool {
	if b == nil {
		return true
	}
	return len(b.flows) == 0 && len(b.perFlow) == 0 && b.bytesUp == 0 && b.bytesDown == 0 && b.pktsUp == 0 && b.pktsDown == 0 && b.flowCount == 0
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
	s.localIPs = localIPs
	s.lastIPRefresh = time.Now().UTC()
	return nil
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

func (s *Service) snapshotCopy() map[uint64]model.FlowSnapshot {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()

	copied := make(map[uint64]model.FlowSnapshot, len(s.snapshots))
	for ctid, snapshot := range s.snapshots {
		copied[ctid] = snapshot
	}
	return copied
}

func (s *Service) isSnapshotReady() bool {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return s.snapshotReady
}

func (s *Service) replaceSnapshots(next map[uint64]model.FlowSnapshot) {
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
	s.processHints[key] = processHint{
		process: model.ProcessInfo{PID: snapshot.PID, Comm: snapshot.Comm, Exe: snapshot.Exe},
		expires: now.Add(defaultProcessHintTTL),
	}
}

func (s *Service) pruneProcessHints(now time.Time) {
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
