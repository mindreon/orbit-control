package natsx

import (
	"context"
	"github.com/nats-io/nats.go"
	"time"
)

type Publisher interface {
	Publish(context.Context, string, string, []byte) error
}
type Bus struct {
	Conn *nats.Conn
	JS   nats.JetStreamContext
}

func Connect(url string) (*Bus, error) {
	c, e := nats.Connect(url, nats.Timeout(5*time.Second))
	if e != nil {
		return nil, e
	}
	js, e := c.JetStream()
	if e != nil {
		c.Close()
		return nil, e
	}
	return &Bus{c, js}, nil
}
func (b *Bus) EnsureRunEventsStream() error {
	_, e := b.JS.AddStream(&nats.StreamConfig{Name: "RUN_EVENTS", Subjects: []string{"run.>"}, Storage: nats.FileStorage, Retention: nats.LimitsPolicy, MaxAge: 7 * 24 * time.Hour, Duplicates: 24 * time.Hour, MaxMsgSize: 64 << 10})
	return e
}
func (b *Bus) Publish(ctx context.Context, subject, id string, data []byte) error {
	_, e := b.JS.Publish(subject, data, nats.MsgId(id), nats.Context(ctx))
	return e
}
func (b *Bus) Close() { b.Conn.Close() }
