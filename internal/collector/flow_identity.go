package collector

import (
	"fmt"
	"sort"

	"traffic-go/internal/model"
)

type flowIdentity string

type observedFlow struct {
	Identity   flowIdentity
	Flow       model.ConntrackFlow
	Classified classifiedFlow
	RawIDs     map[uint64]struct{}
}

func normalizeObservedFlows(flows []model.ConntrackFlow, localIPs map[string]struct{}, sockets socketIndex) []observedFlow {
	if len(flows) == 0 {
		return nil
	}

	grouped := make(map[flowIdentity]*observedFlow, len(flows))
	for _, raw := range flows {
		classified := classifyFlow(raw, localIPs, sockets)
		classified, _ = attachSocketMetadata(sockets, classified)

		identity := buildFlowIdentity(raw, classified)
		current := grouped[identity]
		if current == nil {
			grouped[identity] = &observedFlow{
				Identity:   identity,
				Flow:       raw,
				Classified: classified,
				RawIDs:     map[uint64]struct{}{raw.CTID: {}},
			}
			continue
		}

		current.RawIDs[raw.CTID] = struct{}{}
		if preferObservedCandidate(raw, classified, current.Flow, current.Classified) {
			current.Flow.CTID = raw.CTID
			current.Classified = classified
		}
		current.Flow.HasAccounting = current.Flow.HasAccounting || raw.HasAccounting
		current.Flow.OrigBytes = maxUint64(current.Flow.OrigBytes, raw.OrigBytes)
		current.Flow.ReplyBytes = maxUint64(current.Flow.ReplyBytes, raw.ReplyBytes)
		current.Flow.OrigPkts = maxUint64(current.Flow.OrigPkts, raw.OrigPkts)
		current.Flow.ReplyPkts = maxUint64(current.Flow.ReplyPkts, raw.ReplyPkts)
	}

	result := make([]observedFlow, 0, len(grouped))
	for _, flow := range grouped {
		result = append(result, *flow)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Identity < result[j].Identity
	})
	return result
}

func buildFlowIdentity(raw model.ConntrackFlow, classified classifiedFlow) flowIdentity {
	if classified.Direction == model.DirectionForward {
		return flowIdentity(fmt.Sprintf(
			"forward|%s|%s|%d|%s|%d",
			classified.Proto,
			classified.OrigSrcIP,
			classified.OrigSrcPort,
			classified.OrigDstIP,
			classified.OrigDstPort,
		))
	}

	return flowIdentity(fmt.Sprintf(
		"local|%s|%s|%s|%d|%s|%d|%s",
		raw.Family,
		classified.Proto,
		classified.Direction,
		classified.LocalPort,
		classified.RemoteIP,
		classified.RemotePort,
		classified.LocalIP,
	))
}

func preferObservedCandidate(next model.ConntrackFlow, nextClassified classifiedFlow, current model.ConntrackFlow, currentClassified classifiedFlow) bool {
	nextScore := observedCandidateScore(next, nextClassified)
	currentScore := observedCandidateScore(current, currentClassified)
	if nextScore != currentScore {
		return nextScore > currentScore
	}
	if next.OrigBytes != current.OrigBytes {
		return next.OrigBytes > current.OrigBytes
	}
	if next.ReplyBytes != current.ReplyBytes {
		return next.ReplyBytes > current.ReplyBytes
	}
	return next.CTID < current.CTID
}

func observedCandidateScore(flow model.ConntrackFlow, classified classifiedFlow) int {
	score := 0
	if flow.HasAccounting {
		score += 8
	}
	if classified.Inode > 0 {
		score += 4
	}
	if classified.Connected {
		score += 2
	}
	if !classified.MatchedByLocal {
		score++
	}
	return score
}

func maxUint64(left, right uint64) uint64 {
	if right > left {
		return right
	}
	return left
}
