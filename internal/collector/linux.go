package collector

import "traffic-go/internal/model"

type classifiedFlow struct {
	Proto          string
	Direction      model.Direction
	LocalIP        string
	LocalPort      int
	RemoteIP       string
	RemotePort     int
	Inode          uint64
	Connected      bool
	MatchedByLocal bool
	MatchedByHint  bool
	Tuple          tuple
	OrigSrcIP      string
	OrigDstIP      string
	OrigSrcPort    int
	OrigDstPort    int
}

func classifyFlow(flow model.ConntrackFlow, localIPs map[string]struct{}, sockets socketIndex) classifiedFlow {
	outTuple := tuple{
		Proto:      flow.Proto,
		LocalIP:    flow.OrigSrcIP,
		LocalPort:  flow.OrigSrcPort,
		RemoteIP:   flow.OrigDstIP,
		RemotePort: flow.OrigDstPort,
	}
	inTuple := tuple{
		Proto:      flow.Proto,
		LocalIP:    flow.OrigDstIP,
		LocalPort:  flow.OrigDstPort,
		RemoteIP:   flow.OrigSrcIP,
		RemotePort: flow.OrigSrcPort,
	}
	replyTuple := tuple{
		Proto:      flow.Proto,
		LocalIP:    flow.ReplySrcIP,
		LocalPort:  flow.ReplySrcPort,
		RemoteIP:   flow.ReplyDstIP,
		RemotePort: flow.ReplyDstPort,
	}

	if _, ok := localIPs[flow.OrigSrcIP]; ok {
		return classifyAsLocal(flow.Proto, model.DirectionOut, outTuple, socketEntry{})
	}
	if sock, ok := sockets.ByTuple[replyTuple.key()]; ok && sock.Present {
		// DNAT/REDIRECT traffic may already target a local address in the
		// original tuple while the real process is only visible on the
		// translated reply tuple. Prefer that socket before falling back to
		// the raw destination IP shortcut so attribution follows the actual
		// local endpoint.
		return classifyAsLocal(flow.Proto, model.DirectionIn, replyTuple, sock)
	}
	if _, ok := localIPs[flow.OrigDstIP]; ok {
		return classifyAsLocal(flow.Proto, model.DirectionIn, inTuple, socketEntry{})
	}
	if sock, ok := sockets.ByTuple[outTuple.key()]; ok && sock.Present {
		return classifyAsLocal(flow.Proto, model.DirectionOut, outTuple, sock)
	}
	if sock, ok := sockets.ByTuple[inTuple.key()]; ok && sock.Present {
		return classifyAsLocal(flow.Proto, model.DirectionIn, inTuple, sock)
	}
	return classifiedFlow{
		Proto:       flow.Proto,
		Direction:   model.DirectionForward,
		OrigSrcIP:   flow.OrigSrcIP,
		OrigDstIP:   flow.OrigDstIP,
		OrigSrcPort: flow.OrigSrcPort,
		OrigDstPort: flow.OrigDstPort,
	}
}

func classifyAsLocal(proto string, direction model.Direction, t tuple, sock socketEntry) classifiedFlow {
	return classifiedFlow{
		Proto:      proto,
		Direction:  direction,
		LocalIP:    t.LocalIP,
		LocalPort:  t.LocalPort,
		RemoteIP:   t.RemoteIP,
		RemotePort: t.RemotePort,
		Tuple:      t,
		Connected:  sock.Connected,
	}
}
