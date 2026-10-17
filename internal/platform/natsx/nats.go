package natsx

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Publisher interface {
	Publish(context.Context, string, string, []byte) error
}
type Bus struct {
	Conn *nats.Conn
	JS   jetstream.JetStream
}

func Connect(url, name string) (*Bus, error) {
	c, err := nats.Connect(url, nats.Name(name), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second), nats.Timeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(c)
	if err != nil {
		c.Close()
		return nil, err
	}
	return &Bus{Conn: c, JS: js}, nil
}
func (b *Bus) EnsureRunEventsStream(ctx context.Context, maxBytes int64) error {
	return b.ensureRunEventsStream(ctx, "RUN_EVENTS", "run.>", maxBytes)
}
func (b *Bus) ensureRunEventsStream(ctx context.Context, name, subject string, maxBytes int64) error {
	_, err := b.JS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              name,
		Subjects:          []string{subject},
		Storage:           jetstream.FileStorage,
		Retention:         jetstream.LimitsPolicy,
		MaxAge:            7 * 24 * time.Hour,
		MaxMsgsPerSubject: 500000,
		Duplicates:        10 * time.Minute,
		MaxMsgSize:        MaxMessageSize,
		Discard:           jetstream.DiscardOld,
		Compression:       jetstream.S2Compression,
		MaxBytes:          maxBytes,
	})
	return err
}

// MaxMessageSize is the stream's per-message limit. A pull request's byte
// budget also counts the subject and ack reply, so gateway buffers must be at
// least twice this to always deliver a maximum-size message.
const MaxMessageSize = 64 << 10

// SinkBackOff is the sink consumer's redelivery schedule. JetStream applies its
// first step as the ack wait of a first delivery, so progress heartbeats must
// be sent well inside it.
func SinkBackOff() []time.Duration {
	return []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second, 60 * time.Second}
}

func (b *Bus) EnsureSinkConsumer(ctx context.Context) (jetstream.Consumer, error) {
	return b.ensureSinkConsumer(ctx, "RUN_EVENTS", "run.>", "run-events-sink")
}
func (b *Bus) ensureSinkConsumer(ctx context.Context, stream, subject, durable string) (jetstream.Consumer, error) {
	return b.JS.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       SinkBackOff()[0],
		MaxDeliver:    -1,
		MaxAckPending: 2000,
		BackOff:       SinkBackOff(),
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	})
}
func (b *Bus) Publish(ctx context.Context, subject, id string, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := b.JS.Publish(ctx, subject, data, jetstream.WithMsgID(id))
	return err
}
func (b *Bus) Close() { b.Conn.Close() }
