package signaling

// Membership of an agent in an interaction is a half-open interval [JoinSeq, LeftSeq):
// a participant.joined / interaction.assigned at .log sequence J opens [J, ∞); a
// participant.left / un-assign / transfer-away at L closes it to [J, L). LeftOpen marks an
// interval that has not been closed (still ∞). See openspec change agent-feed-fanout (Decision 6).
type Interval struct {
	JoinSeq  int64
	LeftSeq  int64
	LeftOpen bool
}

// ParticipationView folds an interaction's .log facts into per-agent membership intervals. It is
// the SINGLE .log-derived source of truth shared by the router's agent-command authz (write plane)
// and the fan-out projector (read plane) so the two can never disagree. Reusable on its own; the
// fan-out projector will fold the same facts via FoldParticipation.
type ParticipationView struct {
	// agent -> its intervals in .log order (an agent may join, leave, and re-join).
	intervals map[string][]Interval
}

// FoldParticipation builds a ParticipationView from facts in .log sequence order. Only the
// participation facts (participant.joined / interaction.assigned open; participant.left close)
// affect membership; every other fact type is ignored. Facts MUST be ordered by sequence — the
// router replays them that way and the projector consumes a serial MaxAckPending=1 stream.
func FoldParticipation(facts []*Event) *ParticipationView {
	v := &ParticipationView{intervals: map[string][]Interval{}}
	for _, e := range facts {
		switch e.EventType {
		case "participant.joined", "interaction.assigned":
			v.open(e.ActorId, e.Sequence)
		case "participant.left":
			v.close(e.ActorId, e.Sequence)
		}
	}
	return v
}

func (v *ParticipationView) open(agent string, seq int64) {
	cur := v.intervals[agent]
	if n := len(cur); n > 0 && cur[n-1].LeftOpen {
		return // already an open interval — a duplicate join is a no-op
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

// IsParticipantNow reports whether the agent currently holds an OPEN interval — i.e. has joined and
// not yet left. This is the authorization predicate for an agent-role command at the .log head.
func (v *ParticipationView) IsParticipantNow(agent string) bool {
	cur := v.intervals[agent]
	if n := len(cur); n > 0 {
		return cur[n-1].LeftOpen
	}
	return false
}

// Intervals returns a copy of the agent's membership intervals in .log order (the fan-out
// projector interval-guards each projection by these). Nil for an agent that never participated.
func (v *ParticipationView) Intervals(agent string) []Interval {
	cur := v.intervals[agent]
	if cur == nil {
		return nil
	}
	return append([]Interval(nil), cur...)
}
