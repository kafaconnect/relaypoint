---
id: V2-01
slice: V2
title: thread traceparent across the .log + agent-feed hops (Append ctx + projector extract/inject)
status: todo
issue:
specs: [observability]
---
Add ctx to signaling.LogStore.Append; the jetstreamStore publishes the .log fact via PublishMsg with
the traceparent header (obs.InjectTraceparent). Projector jsLogSource.Deliver extracts the header into
the Fact; the fold loop seeds ctx (obs.ContextFromTraceparent) for FeedSink.Publish, which injects the
header onto the agent-feed message. Update the 4 test doubles + 2 router callers. Test the round-trip.
Scenario: obs.rp-log-hop-preserves-trace.

## Log
