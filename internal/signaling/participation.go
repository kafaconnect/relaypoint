package signaling

// Interval is membership as a half-open range [JoinSeq, LeftSeq): join/assign opens, participant.left closes, LeftOpen marks ∞ (agent-feed-fanout Decision 6).
type Interval struct {
	JoinSeq  int64
	LeftSeq  int64
	LeftOpen bool
}

// ParticipationView is the SINGLE .log-derived membership source shared by write-plane authz and the read-plane projector so the two can never disagree.
type ParticipationView struct {
	intervals map[string][]Interval
}

// Facts MUST be in .log sequence order.
func FoldParticipation(facts []*Event) *ParticipationView {
	v := &ParticipationView{intervals: map[string][]Interval{}}
	for _, e := range facts {
		v.ApplyFact(e)
	}
	return v
}

func NewParticipationView() *ParticipationView {
	return &ParticipationView{intervals: map[string][]Interval{}}
}

// Facts MUST arrive in sequence order.
func (v *ParticipationView) ApplyFact(e *Event) {
	switch e.EventType {
	case "participant.joined", "interaction.assigned":
		v.open(e.ActorId, e.Sequence)
	case "participant.left":
		v.close(e.ActorId, e.Sequence)
	}
}

func (v *ParticipationView) Agents() []string {
	out := make([]string, 0, len(v.intervals))
	for a := range v.intervals {
		out = append(out, a)
	}
	return out
}

func (v *ParticipationView) SetIntervals(agent string, in []Interval) {
	if len(in) == 0 {
		return
	}
	v.intervals[agent] = append([]Interval(nil), in...)
}

func (v *ParticipationView) open(agent string, seq int64) {
	cur := v.intervals[agent]
	if n := len(cur); n > 0 && cur[n-1].LeftOpen {
		return
	}
	v.intervals[agent] = append(cur, Interval{JoinSeq: seq, LeftOpen: true})
}

func (v *ParticipationView) close(agent string, seq int64) {
	cur := v.intervals[agent]
	for i := len(cur) - 1; i >= 0; i-- {
		if cur[i].LeftOpen {
			cur[i].LeftSeq, cur[i].LeftOpen = seq, false
			return
		}
	}
}

func (v *ParticipationView) IsParticipantNow(agent string) bool {
	cur := v.intervals[agent]
	if n := len(cur); n > 0 {
		return cur[n-1].LeftOpen
	}
	return false
}

func (v *ParticipationView) Intervals(agent string) []Interval {
	cur := v.intervals[agent]
	if cur == nil {
		return nil
	}
	return append([]Interval(nil), cur...)
}
