package collector

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type tuple struct {
	Proto      string
	LocalIP    string
	LocalPort  int
	RemoteIP   string
	RemotePort int
}

func (t tuple) valid() bool {
	return t.Proto != "" && t.LocalIP != "" && t.LocalPort > 0
}

func (t tuple) key() string {
	return fmt.Sprintf("%s|%s|%d|%s|%d", t.Proto, t.LocalIP, t.LocalPort, t.RemoteIP, t.RemotePort)
}

type socketEntry struct {
	Inode     uint64
	Connected bool
	Present   bool
}

type socketIndex struct {
	ByTuple map[string]socketEntry
	ByLocal map[string]socketEntry
}

func localTupleKey(proto string, localIP string, localPort int) string {
	return fmt.Sprintf("%s|%s|%d", proto, localIP, localPort)
}

func ReadSocketIndex(procFS string) (socketIndex, error) {
	index := socketIndex{
		ByTuple: make(map[string]socketEntry),
		ByLocal: make(map[string]socketEntry),
	}
	roots := socketFileRoots(procFS)
	for _, root := range roots {
		if err := readSocketRoot(index, root); err != nil {
			return socketIndex{}, err
		}
	}
	return index, nil
}

func socketFileRoots(procFS string) []string {
	roots := make([]string, 0, 8)
	seen := make(map[string]struct{})

	addRoot := func(root string, key string) {
		root = filepath.Clean(root)
		if root == "." || root == "" {
			return
		}
		if key == "" {
			key = root
		}
		if _, ok := seen[key]; ok {
			return
		}
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			return
		}
		seen[key] = struct{}{}
		roots = append(roots, root)
	}

	baseNSKey := ""
	if target, err := os.Readlink(filepath.Join(procFS, "self", "ns", "net")); err == nil && strings.TrimSpace(target) != "" {
		baseNSKey = target
	}
	addRoot(filepath.Join(procFS, "net"), baseNSKey)

	entries, err := os.ReadDir(procFS)
	if err != nil {
		return roots
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		netRoot := filepath.Join(procFS, entry.Name(), "net")
		nsKey := ""
		if target, err := os.Readlink(filepath.Join(procFS, entry.Name(), "ns", "net")); err == nil && strings.TrimSpace(target) != "" {
			nsKey = target
		}
		addRoot(netRoot, nsKey)
	}
	return roots
}

func readSocketRoot(index socketIndex, root string) error {
	files := []struct {
		name  string
		proto string
	}{
		{"tcp", "tcp"},
		{"tcp6", "tcp"},
		{"udp", "udp"},
		{"udp6", "udp"},
	}

	for _, item := range files {
		entries, err := readSocketFile(filepath.Join(root, item.name), item.proto)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, entry := range entries {
			recordSocketTuple(index.ByTuple, entry.Tuple.key(), socketEntry{
				Inode:     entry.Inode,
				Connected: entry.Connected,
				Present:   true,
			})
			if entry.Connected {
				continue
			}
			recordSocketTuple(index.ByLocal, localTupleKey(entry.Tuple.Proto, entry.Tuple.LocalIP, entry.Tuple.LocalPort), socketEntry{
				Inode:     entry.Inode,
				Connected: false,
				Present:   true,
			})
		}
	}
	return nil
}

func recordSocketTuple(index map[string]socketEntry, key string, entry socketEntry) {
	existing, ok := index[key]
	switch {
	case !ok:
		index[key] = entry
	case existing.Inode == entry.Inode:
		existing.Present = true
		existing.Connected = existing.Connected || entry.Connected
		index[key] = existing
	default:
		index[key] = socketEntry{
			Inode:     0,
			Connected: existing.Connected || entry.Connected,
			Present:   true,
		}
	}
}

type parsedSocketEntry struct {
	Tuple     tuple
	Inode     uint64
	Connected bool
}

func readSocketFile(path string, proto string) ([]parsedSocketEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	first := true
	entries := make([]parsedSocketEntry, 0, 256)
	for scanner.Scan() {
		if first {
			first = false
			continue
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) < 10 {
			continue
		}
		localIP, localPort, err := decodeProcEndpoint(fields[1])
		if err != nil {
			continue
		}
		remoteIP, remotePort, err := decodeProcEndpoint(fields[2])
		if err != nil {
			continue
		}
		inode, err := strconv.ParseUint(fields[9], 10, 64)
		if err != nil {
			continue
		}
		entries = append(entries, parsedSocketEntry{
			Tuple: tuple{
				Proto:      proto,
				LocalIP:    localIP,
				LocalPort:  localPort,
				RemoteIP:   remoteIP,
				RemotePort: remotePort,
			},
			Inode:     inode,
			Connected: !(remoteIP == "0.0.0.0" && remotePort == 0) && !(remoteIP == "::" && remotePort == 0),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan socket file %q: %w", path, err)
	}
	return entries, nil
}

func decodeProcEndpoint(value string) (string, int, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid proc endpoint %q", value)
	}
	ipHex, portHex := parts[0], parts[1]
	portValue, err := strconv.ParseUint(portHex, 16, 32)
	if err != nil {
		return "", 0, err
	}

	switch len(ipHex) {
	case 8:
		ip, err := decodeIPv4(ipHex)
		return ip, int(portValue), err
	case 32:
		ip, err := decodeIPv6(ipHex)
		return ip, int(portValue), err
	default:
		return "", 0, fmt.Errorf("unsupported ip hex %q", ipHex)
	}
}

func decodeIPv4(ipHex string) (string, error) {
	raw, err := hex.DecodeString(ipHex)
	if err != nil {
		return "", err
	}
	if len(raw) != 4 {
		return "", fmt.Errorf("unexpected ipv4 length")
	}
	return net.IPv4(raw[3], raw[2], raw[1], raw[0]).String(), nil
}

func decodeIPv6(ipHex string) (string, error) {
	raw, err := hex.DecodeString(ipHex)
	if err != nil {
		return "", err
	}
	if len(raw) != 16 {
		return "", fmt.Errorf("unexpected ipv6 length")
	}
	normalized := make([]byte, 16)
	for i := 0; i < 16; i += 4 {
		normalized[i] = raw[i+3]
		normalized[i+1] = raw[i+2]
		normalized[i+2] = raw[i+1]
		normalized[i+3] = raw[i]
	}
	return net.IP(normalized).String(), nil
}
