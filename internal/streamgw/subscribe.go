package streamgw

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
)

type eventSource interface {
	Stream(context.Context, string) (jetstream.Stream, error)
	OrderedConsumer(context.Context, string, jetstream.OrderedConsumerConfig) (jetstream.Consumer, error)
}

type streamInfoCache struct {
	mu      sync.Mutex
	first   uint64
	expires time.Time
}

func consumerConfig(subject string, cursor uint64) (jetstream.OrderedConsumerConfig, error) {
	cfg := jetstream.OrderedConsumerConfig{FilterSubjects: []string{subject},
		DeliverPolicy: jetstream.DeliverAllPolicy, InactiveThreshold: 30 * time.Second}
	if cursor == math.MaxUint64 {
		return cfg, fmt.Errorf("cursor overflow")
	}
	if cursor > 0 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = cursor + 1
	}
	return cfg, nil
}

func (s *Server) firstSequence(ctx context.Context) (uint64, error) {
	s.info.mu.Lock()
	defer s.info.mu.Unlock()
	if time.Now().Before(s.info.expires) {
		return s.info.first, nil
	}
	stream, err := s.js.Stream(ctx, s.stream)
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	s.info.first = info.State.FirstSeq
	s.info.expires = time.Now().Add(s.cfg.InfoCache)
	return s.info.first, nil
}

// A last-message marker lookup is independent of pending event frames.
func (s *Server) endMarkerPresent(ctx context.Context, claims auth.StreamClaims, cursor uint64) (bool, error) {
	c, err := s.js.OrderedConsumer(ctx, s.stream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{natsx.EndSubject(claims.TenantID, claims.SessionID, claims.RunID)}, DeliverPolicy: jetstream.DeliverLastPolicy,
		InactiveThreshold: 30 * time.Second,
	})
	if err != nil {
		return false, err
	}
	batch, err := c.FetchNoWait(1)
	if err != nil {
		return false, err
	}
	finished := false
	for msg := range batch.Messages() {
		_, err := msg.Metadata()
		if err != nil {
			return false, err
		}
		finished = true
	}
	return finished, batch.Error()
}
