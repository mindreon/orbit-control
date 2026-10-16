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
	_, err := b.JS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              "RUN_EVENTS",
		Subjects:          []string{"run.>"},
		Storage:           jetstream.FileStorage,
		Retention:         jetstream.LimitsPolicy,
		MaxAge:            7 * 24 * time.Hour,
		MaxMsgsPerSubject: 500000,
		Duplicates:        10 * time.Minute,
		MaxMsgSize:        64 << 10,
		Discard:           jetstream.DiscardOld,
		Compression:       jetstream.S2Compression,
		MaxBytes:          maxBytes,
	})
	return err
}
func (b *Bus) EnsureSinkConsumer(ctx context.Context) (jetstream.Consumer, error) {
	return b.JS.CreateOrUpdateConsumer(ctx, "RUN_EVENTS", jetstream.ConsumerConfig{
		Durable:       "run-events-sink",
		FilterSubject: "run.>",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    10,
		MaxAckPending: 2000,
		BackOff:       []time.Duration{time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second, 60 * time.Second},
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
