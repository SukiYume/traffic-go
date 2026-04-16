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
	runtimeMu      sync.RWMutex
	snapshots      map[uint64]model.FlowSnapshot
	localIPs       map[string]struct{}
	lastIPRefresh  time.Time
	warnedNoAcct   bool
}

type bucketState struct {
	bytesUp   int64
	bytesDown int64
	pktsUp    int64
	pktsDown  int64
	flows     map[uint64]struct{}
}

func New(cfg config.Config, trafficStore *store.Store, logger *log.Logger) Runner {
	if cfg.MockData || runtime.GOOS != "linux" {
		return &mockCollector{
			cfg:    cfg,
			store:  trafficStore,
			logger: logger,
		}
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
		snapshots:      make(map[uint64]model.FlowSnapshot),
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
			if err := s.flushCurrentBuckets(ctx); err != nil {
				s.logger.Printf("flush buckets on shutdown failed: %v", err)
			}
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

	inodesNeeded := make(map[uint64]struct{})
	flowMeta := make(map[uint64]classifiedFlow)
	for _, flow := range flows {
		classified := classifyFlow(flow, s.localIPs)
		if classified.Direction != model.DirectionForward && classified.Tuple.valid() {
			if sock, ok := socketIndex[classified.Tuple.key()]; ok {
				classified.Inode = sock.Inode
				classified.Connected = sock.Connected
				inodesNeeded[sock.Inode] = struct{}{}
			}
		}
		flowMeta[flow.CTID] = classified
	}

	processes := s.resolver.Resolve(ctx, inodesNeeded)
	prevSnapshots := s.snapshotCopy()
	nextSnapshots := make(map[uint64]model.FlowSnapshot, len(flows))

	for _, flow := range flows {
		classified := flowMeta[flow.CTID]
		prev, exists := prevSnapshots[flow.CTID]
		snapshot, delta, forwardDelta := s.updateSnapshot(now, flow, classified, processes[classified.Inode], prev, exists)
		nextSnapshots[flow.CTID] = snapshot

		if classified.Direction == model.DirectionForward {
			if forwardDelta != nil {
				s.addForwardUsage(snapshot.CTID, classified, *forwardDelta)
			}
			continue
		}
		if delta != nil {
			s.addUsage(snapshot.CTID, snapshot, *delta)
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
) (model.FlowSnapshot, *deltaPair, *deltaPair) {
	attribution := model.AttributionUnknown
	pid := 0
	comm := ""
	exe := ""
	if classified.Direction != model.DirectionForward {
		if flow.Proto == "udp" && !classified.Connected {
			attribution = model.AttributionUnknown
		} else if process.PID > 0 {
			attribution = model.AttributionExact
			pid = process.PID
			comm = process.Comm
			exe = process.Exe
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
		LastSeen:      now,
	}

	if !exists || snapshotTupleChanged(prev, snapshot) || flow.OrigBytes < prev.LastOrig || flow.ReplyBytes < prev.LastReply {
		// baseline: first observation / counter reset -> record current as baseline, drop delta.
		return snapshot, nil, nil
	}

	snapshot.StartedAt = prev.StartedAt
	snapshot.BaselineOrig = prev.BaselineOrig
	snapshot.BaselineReply = prev.BaselineReply
	snapshot.BaselineOPkts = prev.BaselineOPkts
	snapshot.BaselineRPkts = prev.BaselineRPkts

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
		}
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
	return snapshot, &delta, nil
}

type deltaPair struct {
	upBytes   int64
	downBytes int64
	upPkts    int64
	downPkts  int64
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

func (s *Service) addUsage(ctid uint64, snapshot model.FlowSnapshot, delta deltaPair) {
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
		state = &bucketState{flows: make(map[uint64]struct{})}
		s.buckets[key] = state
	}
	state.bytesUp += delta.upBytes
	state.bytesDown += delta.downBytes
	state.pktsUp += delta.upPkts
	state.pktsDown += delta.downPkts
	state.flows[ctid] = struct{}{}
}

func (s *Service) addForwardUsage(ctid uint64, classified classifiedFlow, delta deltaPair) {
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
		state = &bucketState{flows: make(map[uint64]struct{})}
		s.forwardBuckets[key] = state
	}
	state.bytesUp += delta.upBytes
	state.bytesDown += delta.downBytes
	state.pktsUp += delta.upPkts
	state.pktsDown += delta.downPkts
	state.flows[ctid] = struct{}{}
}

func (s *Service) flushCurrentBuckets(ctx context.Context) error {
	if len(s.buckets) == 0 && len(s.forwardBuckets) == 0 {
		return nil
	}

	usage := make(map[model.UsageKey]model.UsageDelta, len(s.buckets))
	for key, state := range s.buckets {
		usage[key] = model.UsageDelta{
			BytesUp:   state.bytesUp,
			BytesDown: state.bytesDown,
			PktsUp:    state.pktsUp,
			PktsDown:  state.pktsDown,
			FlowCount: int64(len(state.flows)),
		}
	}

	forward := make(map[model.ForwardUsageKey]model.UsageDelta, len(s.forwardBuckets))
	for key, state := range s.forwardBuckets {
		forward[key] = model.UsageDelta{
			BytesUp:   state.bytesUp,
			BytesDown: state.bytesDown,
			PktsUp:    state.pktsUp,
			PktsDown:  state.pktsDown,
			FlowCount: int64(len(state.flows)),
		}
	}

	if err := s.store.FlushMinute(ctx, s.currentMinute.Unix(), usage, forward); err != nil {
		return err
	}
	s.buckets = make(map[model.UsageKey]*bucketState)
	s.forwardBuckets = make(map[model.ForwardUsageKey]*bucketState)
	return nil
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

func (s *Service) replaceSnapshots(next map[uint64]model.FlowSnapshot) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.snapshots = next
}
