package collector

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"traffic-go/internal/model"
)

const (
	defaultPositiveCacheTTL = 30 * time.Second
	defaultNegativeCacheTTL = 10 * time.Second
	defaultScanCooldown     = 5 * time.Second
)

type processResolver struct {
	procFS        string
	now           func() time.Time
	scan          func(context.Context) (map[uint64]model.ProcessInfo, bool)
	pidSockets    func(int) (map[uint64]struct{}, bool)
	positiveTTL   time.Duration
	negativeTTL   time.Duration
	scanCooldown  time.Duration
	lastFullScan  time.Time
	lastMissScan  time.Time
	cache         map[uint64]model.ProcessInfo
	negativeCache map[uint64]time.Time
}

func newProcessResolver(procFS string) *processResolver {
	return &processResolver{
		procFS:        procFS,
		now:           time.Now,
		positiveTTL:   defaultPositiveCacheTTL,
		negativeTTL:   defaultNegativeCacheTTL,
		scanCooldown:  defaultScanCooldown,
		cache:         make(map[uint64]model.ProcessInfo),
		negativeCache: make(map[uint64]time.Time),
	}
}

func (r *processResolver) Resolve(ctx context.Context, requested map[uint64]struct{}) map[uint64]model.ProcessInfo {
	now := r.now().UTC()
	r.purgeExpiredNegativeEntries(now)

	resolved := make(map[uint64]model.ProcessInfo, len(requested))
	if len(requested) == 0 {
		return resolved
	}
	scanDue := r.fullScanDue(now)
	missing := make(map[uint64]struct{})
	ownedSockets := make(map[int]map[uint64]struct{})
	ownedSocketsLoaded := make(map[int]bool)
	for inode := range requested {
		if !scanDue {
			if cached, ok := r.cache[inode]; ok {
				// Confirm the cached PID still owns the socket inode before we
				// trust a stale positive-cache hit. Linux can recycle socket
				// inodes quickly, and using the old mapping would misattribute
				// short-lived connections to the wrong process.
				if r.cachedOwnershipValid(ownedSockets, ownedSocketsLoaded, cached.PID, inode) {
					resolved[inode] = cached
					continue
				}
				delete(r.cache, inode)
			}
		}
		if expiry, ok := r.negativeCache[inode]; ok && expiry.After(now) {
			continue
		}
		missing[inode] = struct{}{}
	}

	if len(missing) == 0 && !scanDue {
		return resolved
	}
	if len(missing) > 0 && !scanDue && !r.missScanDue(now) {
		return resolved
	}

	scanFn := r.scan
	if scanFn == nil {
		scanFn = r.scanProcSockets
	}
	// Any previously unseen inode still forces one full /proc scan even if the
	// positive cache TTL has not elapsed yet. The negative cache prevents sockets
	// that cannot be resolved from paying this full-scan cost on every tick.
	scanned, scanOK := scanFn(ctx)
	if scanOK {
		r.cache = scanned
		r.lastFullScan = now
		r.lastMissScan = now
		for inode := range scanned {
			delete(r.negativeCache, inode)
		}
	}

	for inode := range missing {
		if cached, ok := r.cache[inode]; ok {
			resolved[inode] = cached
			continue
		}
		if !scanOK {
			continue
		}
		if expiry, ok := r.negativeCache[inode]; ok && expiry.After(now) {
			continue
		}
		r.negativeCache[inode] = now.Add(r.negativeTTL)
	}

	return resolved
}

func (r *processResolver) fullScanDue(now time.Time) bool {
	return r.lastFullScan.IsZero() || now.Sub(r.lastFullScan) >= r.positiveTTL
}

func (r *processResolver) missScanDue(now time.Time) bool {
	return r.lastMissScan.IsZero() || now.Sub(r.lastMissScan) >= r.scanCooldown
}

func (r *processResolver) cachedOwnershipValid(ownedSockets map[int]map[uint64]struct{}, loaded map[int]bool, pid int, inode uint64) bool {
	if pid <= 0 {
		return false
	}
	if !loaded[pid] {
		loadSockets := r.pidSockets
		if loadSockets == nil {
			loadSockets = r.readProcessSocketInodes
		}
		sockets, ok := loadSockets(pid)
		loaded[pid] = true
		if !ok {
			return false
		}
		ownedSockets[pid] = sockets
	}
	sockets, ok := ownedSockets[pid]
	if !ok {
		return false
	}
	_, exists := sockets[inode]
	return exists
}

func (r *processResolver) purgeExpiredNegativeEntries(now time.Time) {
	for inode, expiry := range r.negativeCache {
		if !expiry.After(now) {
			delete(r.negativeCache, inode)
		}
	}
}

func (r *processResolver) readProcessSocketInodes(pid int) (map[uint64]struct{}, bool) {
	if pid <= 0 {
		return nil, false
	}
	return readSocketInodesFromFDDir(filepath.Join(r.procFS, strconv.Itoa(pid), "fd"))
}

func (r *processResolver) scanProcSockets(ctx context.Context) (map[uint64]model.ProcessInfo, bool) {
	entries, err := os.ReadDir(r.procFS)
	if err != nil {
		return nil, false
	}

	cache := make(map[uint64]model.ProcessInfo)
	for _, entry := range entries {
		if ctx.Err() != nil {
			return nil, false
		}
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}

		fdDir := filepath.Join(r.procFS, entry.Name(), "fd")
		inodes, ok := readSocketInodesFromFDDir(fdDir)
		if !ok || len(inodes) == 0 {
			continue
		}

		var (
			comm   string
			exe    string
			loaded bool
		)

		for inode := range inodes {
			if !loaded {
				comm = readProcessComm(r.procFS, entry.Name())
				exe = readProcessExe(r.procFS, entry.Name())
				loaded = true
			}
			cache[inode] = model.ProcessInfo{
				PID:  pid,
				Comm: comm,
				Exe:  exe,
			}
		}
	}

	return cache, true
}

func readSocketInodesFromFDDir(fdDir string) (map[uint64]struct{}, bool) {
	fdEntries, err := os.ReadDir(fdDir)
	if err != nil {
		return nil, false
	}
	inodes := make(map[uint64]struct{})
	for _, fdEntry := range fdEntries {
		target, err := os.Readlink(filepath.Join(fdDir, fdEntry.Name()))
		if err != nil {
			continue
		}
		inode, ok := parseSocketInode(target)
		if !ok {
			continue
		}
		inodes[inode] = struct{}{}
	}
	return inodes, true
}

func parseSocketInode(target string) (uint64, bool) {
	if !strings.HasPrefix(target, "socket:[") || !strings.HasSuffix(target, "]") {
		return 0, false
	}
	inodeValue := strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")
	inode, err := strconv.ParseUint(inodeValue, 10, 64)
	if err != nil {
		return 0, false
	}
	return inode, true
}

func readProcessComm(procFS, pid string) string {
	commBytes, err := os.ReadFile(filepath.Join(procFS, pid, "comm"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(commBytes))
}

func readProcessExe(procFS, pid string) string {
	exe, err := os.Readlink(filepath.Join(procFS, pid, "exe"))
	if err != nil {
		return ""
	}
	return exe
}
