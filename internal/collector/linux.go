package collector

import "traffic-go/internal/model"

type classifiedFlow struct {
	Proto       string
	Direction   model.Direction
	LocalIP     string
	LocalPort   int
	RemoteIP    string
	RemotePort  int
	Inode       uint64
	Connected   bool
	Tuple       tuple
	OrigSrcIP   string
	OrigDstIP   string
	OrigSrcPort int
	OrigDstPort int
}

func classifyFlow(flow model.ConntrackFlow, localIPs map[string]struct{}) classifiedFlow {
	if _, ok := localIPs[flow.OrigSrcIP]; ok {
		t := tuple{
			Proto:      flow.Proto,
			LocalIP:    flow.OrigSrcIP,
			LocalPort:  flow.OrigSrcPort,
			RemoteIP:   flow.OrigDstIP,
			RemotePort: flow.OrigDstPort,
		}
		return classifiedFlow{
			Proto:      flow.Proto,
			Direction:  model.DirectionOut,
			LocalIP:    t.LocalIP,
			LocalPort:  t.LocalPort,
			RemoteIP:   t.RemoteIP,
			RemotePort: t.RemotePort,
			Tuple:      t,
		}
	}
	if _, ok := localIPs[flow.OrigDstIP]; ok {
		t := tuple{
			Proto:      flow.Proto,
			LocalIP:    flow.OrigDstIP,
			LocalPort:  flow.OrigDstPort,
			RemoteIP:   flow.OrigSrcIP,
			RemotePort: flow.OrigSrcPort,
		}
		return classifiedFlow{
			Proto:      flow.Proto,
			Direction:  model.DirectionIn,
			LocalIP:    t.LocalIP,
			LocalPort:  t.LocalPort,
			RemoteIP:   t.RemoteIP,
			RemotePort: t.RemotePort,
			Tuple:      t,
		}
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
