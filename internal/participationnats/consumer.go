package participationnats

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	interactionv1 "github.com/kafaconnect/relaypoint/gen/go/relaypoint/interaction/v1"
	"github.com/kafaconnect/relaypoint/internal/obs"
	"github.com/kafaconnect/relaypoint/internal/participation"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	StreamName = "PARTICIPATION_COMMANDS"
	Durable    = "relaypoint-participation"
)

type CommandHandler interface {
	Apply(context.Context, participation.Principal, *interactionv1.ParticipationCommand) (participation.Result, error)
}

type Consumer struct {
	sub     *nats.Subscription
	handler CommandHandler
	grant   participation.TransportGrant
	logger  *slog.Logger
	ready   atomic.Bool
}

func EnsureStream(js nats.JetStreamContext) error {
	if js == nil {
		return participation.ErrInvalid
	}
	config := &nats.StreamConfig{
		Name: StreamName, Subjects: []string{"corex.participation.commands.v1.*.*"},
		Storage: nats.FileStorage, Retention: nats.WorkQueuePolicy,
		Discard: nats.DiscardOld, Duplicates: 10 * time.Minute,
	}
	info, err := js.StreamInfo(StreamName)
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = js.AddStream(config)
		return err
	}
	if err != nil {
		return err
	}
	if info.Config.Name != config.Name || info.Config.Retention != config.Retention ||
		info.Config.Storage != config.Storage || len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != config.Subjects[0] {
		return participation.ErrInvalid
	}
	return nil
}

func NewConsumer(js nats.JetStreamContext, handler CommandHandler, grant participation.TransportGrant, logger *slog.Logger) (*Consumer, error) {
	if js == nil || handler == nil || logger == nil || grant.ServiceID != "corex" ||
		!participation.ValidTransportGrant(grant, participation.CapabilityWrite) {
		return nil, participation.ErrInvalid
	}
	sub, err := js.PullSubscribe(
		"corex.participation.commands.v1.*.*",
		Durable,
		nats.BindStream(StreamName),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.MaxAckPending(1),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(-1),
	)
	if err != nil {
		return nil, err
	}
	consumer := &Consumer{sub: sub, handler: handler, grant: grant, logger: logger}
	consumer.ready.Store(true)
	return consumer, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	if c == nil || c.sub == nil {
		return participation.ErrInvalid
	}
	defer c.ready.Store(false)
	for {
		fetchCtx, cancel := context.WithTimeout(ctx, time.Second)
		messages, err := c.sub.Fetch(1, nats.Context(fetchCtx))
		cancel()
		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for _, message := range messages {
			c.handle(ctx, message)
		}
	}
}

func (c *Consumer) Ready() error {
	if c == nil || !c.ready.Load() {
		return errors.New("participation consumer not ready")
	}
	return nil
}

func (c *Consumer) Close() error {
	if c == nil || c.sub == nil {
		return nil
	}
	c.ready.Store(false)
	return c.sub.Drain()
}

func (c *Consumer) handle(parent context.Context, message *nats.Msg) {
	command := new(interactionv1.ParticipationCommand)
	decodeErr := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(message.Data, command)
	ctx := obs.ContextFromTraceparent(parent, message.Header.Get("traceparent"))
	ctx = obs.WithRequestID(ctx, command.GetEventId())
	ctx = obs.WithCorrelation(ctx, c.logger.With("tenant_id", command.GetTenantId()))
	record, canonicalErr := participation.Canonical(command)
	wantSubject, subjectErr := interactionv1.ParticipationCommandAddress(command.GetTenantId(), command.GetInteractionId())
	if decodeErr != nil || len(command.ProtoReflect().GetUnknown()) != 0 || canonicalErr != nil || subjectErr != nil ||
		message.Subject != wantSubject || message.Header.Get(nats.MsgIdHdr) != command.GetEventId() ||
		message.Header.Get("traceparent") != command.GetTraceparent() || string(record.Body) != string(message.Data) ||
		!subjectMatchesPayload(message.Subject, command) {
		obs.Logger(ctx).Error("participation.command.rejected", "reason", "invalid_message")
		_ = message.Term()
		return
	}
	result, err := c.handler.Apply(ctx, participation.Principal{
		TenantID: command.GetTenantId(), Grant: c.grant,
	}, command)
	if err == nil || result == participation.CompareAndSetLost {
		if ackErr := message.Ack(); ackErr != nil {
			obs.Logger(ctx).Error("participation.command.ack_failed", "reason", "ack_failed")
		}
		return
	}
	if errors.Is(err, participation.ErrInvalid) || errors.Is(err, participation.ErrPermissionDenied) || errors.Is(err, participation.ErrDivergentHistory) || errors.Is(err, participation.ErrReconcileExhausted) {
		obs.Logger(ctx).Error("participation.command.poisoned", "reason", "permanent_failure")
		_ = message.Term()
		return
	}
	obs.Logger(ctx).Error("participation.command.retrying", "reason", "transient_failure")
	_ = message.Nak()
}

func subjectMatchesPayload(subject string, command *interactionv1.ParticipationCommand) bool {
	tokens := strings.Split(subject, ".")
	return len(tokens) == 6 && tokens[4] == command.GetTenantId() && tokens[5] == command.GetInteractionId()
}
