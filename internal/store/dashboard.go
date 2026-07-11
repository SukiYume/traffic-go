package store

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"traffic-go/internal/model"
)

const (
	dashboardProcessLimit = 5
	dashboardRemoteLimit  = 5
	dashboardPortLimit    = 10
)

type dashboardPointKey struct {
	bucketTS  int64
	direction model.Direction
}

type dashboardRemoteKey struct {
	direction model.Direction
	remoteIP  string
}

func (s *Store) dashboardSummaryParts(start, end time.Time, bucket time.Duration) []summaryPart {
	if bucket < 24*time.Hour {
		return s.usageSummaryParts(start, end)
	}

	startUnix := start.UTC().Unix()
	endUnix := end.UTC().Unix()
	if endUnix <= startUnix || startUnix != floorUnix(startUnix, 86400) {
		return s.usageSummaryParts(start, end)
	}

	dayEnd := minInt64(floorUnix(endUnix, 86400), s.now().UTC().Truncate(24*time.Hour).Unix())
	if dayEnd <= startUnix {
		return s.usageSummaryParts(start, end)
	}

	parts := []summaryPart{{
		info:  usageSourceInfo(DataSourceDay),
		start: startUnix,
		end:   dayEnd,
	}}
	if dayEnd < endUnix {
		parts = append(parts, s.usageSummaryParts(time.Unix(dayEnd, 0).UTC(), end)...)
	}
	return parts
}

// QueryDashboardSummary scans each non-overlapping retention segment once and
// derives every usage-backed dashboard panel from that shared stream. Calendar
// month views use complete daily rows plus the current-day hour/minute tail.
func (s *Store) QueryDashboardSummary(ctx context.Context, start, end time.Time, bucket time.Duration) (model.DashboardSnapshot, error) {
	parts := s.dashboardSummaryParts(start, end, bucket)
	dataSource := usageSummaryDataLabel(parts)
	bucketSeconds := int64(bucket / time.Second)
	if bucketSeconds <= 0 {
		bucketSeconds = 60
	}

	points := make(map[dashboardPointKey]model.TimeseriesPoint)
	processes := make(map[string]model.ProcessSummary)
	remotes := make(map[dashboardRemoteKey]model.RemoteSummary)
	ports := make(map[int]model.TopEntry)
	snapshot := model.DashboardSnapshot{DataSource: dataSource}

	for _, part := range parts {
		rows, err := s.queryDB().QueryContext(ctx, fmt.Sprintf(`
SELECT %[1]s, direction, comm, local_port, remote_ip,
       bytes_up, bytes_down, flow_count
FROM %[2]s
WHERE %[1]s >= ? AND %[1]s < ?
`, part.info.TimeCol, part.info.Table), part.start, part.end)
		if err != nil {
			return model.DashboardSnapshot{}, fmt.Errorf("query dashboard summary source %s: %w", part.info.Table, err)
		}

		for rows.Next() {
			var (
				timestamp int64
				direction model.Direction
				comm      string
				localPort int
				remoteIP  string
				bytesUp   int64
				bytesDown int64
				flowCount int64
			)
			if err := rows.Scan(&timestamp, &direction, &comm, &localPort, &remoteIP, &bytesUp, &bytesDown, &flowCount); err != nil {
				rows.Close()
				return model.DashboardSnapshot{}, fmt.Errorf("scan dashboard summary source %s: %w", part.info.Table, err)
			}

			snapshot.BytesUp += bytesUp
			snapshot.BytesDown += bytesDown
			snapshot.FlowCount += flowCount

			pointKey := dashboardPointKey{bucketTS: floorUnix(timestamp, bucketSeconds), direction: direction}
			point := points[pointKey]
			point.BucketTS = pointKey.bucketTS
			point.Group = string(direction)
			point.BytesUp += bytesUp
			point.BytesDown += bytesDown
			point.FlowCount += flowCount
			point.DataSource = dataSource
			points[pointKey] = point

			process := processes[comm]
			process.Comm = comm
			process.BytesUp += bytesUp
			process.BytesDown += bytesDown
			process.FlowCount += flowCount
			process.DataSource = dataSource
			processes[comm] = process

			remoteKey := dashboardRemoteKey{direction: direction, remoteIP: remoteIP}
			remote := remotes[remoteKey]
			remote.Direction = direction
			remote.RemoteIP = remoteIP
			remote.BytesUp += bytesUp
			remote.BytesDown += bytesDown
			remote.FlowCount += flowCount
			remote.DataSource = dataSource
			remotes[remoteKey] = remote

			port := ports[localPort]
			port.Key = strconv.Itoa(localPort)
			port.BytesUp += bytesUp
			port.BytesDown += bytesDown
			port.FlowCount += flowCount
			port.DataSource = dataSource
			ports[localPort] = port
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return model.DashboardSnapshot{}, fmt.Errorf("iterate dashboard summary source %s: %w", part.info.Table, err)
		}
		rows.Close()
	}

	snapshot.Timeseries = make([]model.TimeseriesPoint, 0, len(points))
	for _, point := range points {
		snapshot.Timeseries = append(snapshot.Timeseries, point)
	}
	sort.Slice(snapshot.Timeseries, func(i, j int) bool {
		if snapshot.Timeseries[i].BucketTS != snapshot.Timeseries[j].BucketTS {
			return snapshot.Timeseries[i].BucketTS < snapshot.Timeseries[j].BucketTS
		}
		return snapshot.Timeseries[i].Group < snapshot.Timeseries[j].Group
	})

	snapshot.TopProcesses = make([]model.ProcessSummary, 0, len(processes))
	for _, process := range processes {
		snapshot.TopProcesses = append(snapshot.TopProcesses, process)
	}
	sort.Slice(snapshot.TopProcesses, func(i, j int) bool {
		left := snapshot.TopProcesses[i].BytesUp + snapshot.TopProcesses[i].BytesDown
		right := snapshot.TopProcesses[j].BytesUp + snapshot.TopProcesses[j].BytesDown
		if left != right {
			return left > right
		}
		return snapshot.TopProcesses[i].Comm < snapshot.TopProcesses[j].Comm
	})
	snapshot.TopProcesses = limitDashboardRows(snapshot.TopProcesses, dashboardProcessLimit)

	for _, remote := range remotes {
		switch remote.Direction {
		case model.DirectionIn:
			snapshot.TopInboundRemotes = append(snapshot.TopInboundRemotes, remote)
		case model.DirectionOut:
			snapshot.TopOutboundRemotes = append(snapshot.TopOutboundRemotes, remote)
		}
	}
	sortDashboardRemotes(snapshot.TopInboundRemotes)
	sortDashboardRemotes(snapshot.TopOutboundRemotes)
	snapshot.TopInboundRemotes = limitDashboardRows(snapshot.TopInboundRemotes, dashboardRemoteLimit)
	snapshot.TopOutboundRemotes = limitDashboardRows(snapshot.TopOutboundRemotes, dashboardRemoteLimit)

	snapshot.TopPorts = make([]model.TopEntry, 0, len(ports))
	for _, port := range ports {
		snapshot.TopPorts = append(snapshot.TopPorts, port)
	}
	sort.Slice(snapshot.TopPorts, func(i, j int) bool {
		left := snapshot.TopPorts[i].BytesUp + snapshot.TopPorts[i].BytesDown
		right := snapshot.TopPorts[j].BytesUp + snapshot.TopPorts[j].BytesDown
		if left != right {
			return left > right
		}
		return snapshot.TopPorts[i].Key < snapshot.TopPorts[j].Key
	})
	snapshot.TopPorts = limitDashboardRows(snapshot.TopPorts, dashboardPortLimit)

	return snapshot, nil
}

func sortDashboardRemotes(rows []model.RemoteSummary) {
	sort.Slice(rows, func(i, j int) bool {
		left := rows[i].BytesUp + rows[i].BytesDown
		right := rows[j].BytesUp + rows[j].BytesDown
		if left != right {
			return left > right
		}
		return rows[i].RemoteIP < rows[j].RemoteIP
	})
}

func limitDashboardRows[T any](rows []T, limit int) []T {
	if limit <= 0 || len(rows) <= limit {
		return rows
	}
	return rows[:limit]
}
