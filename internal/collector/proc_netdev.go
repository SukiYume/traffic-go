package collector

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type interfaceCounter struct {
	Name    string
	RxBytes uint64
	TxBytes uint64
}

func ReadInterfaceCounters(procFS string) ([]interfaceCounter, error) {
	path := filepath.Join(procFS, "net", "dev")
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	counters := make([]interfaceCounter, 0, 4)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		name := strings.TrimSpace(parts[0])
		fields := strings.Fields(parts[1])
		if name == "" || len(fields) < 16 {
			continue
		}
		rxBytes, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %s rx bytes: %w", name, err)
		}
		txBytes, err := strconv.ParseUint(fields[8], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %s tx bytes: %w", name, err)
		}
		counters = append(counters, interfaceCounter{
			Name:    name,
			RxBytes: rxBytes,
			TxBytes: txBytes,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan %q: %w", path, err)
	}
	return counters, nil
}
