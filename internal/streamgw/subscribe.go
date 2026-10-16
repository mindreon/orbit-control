package streamgw

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
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

// A last-message ordered snapshot lets a run resume after its terminal event
// with just end. It uses the same consumer permissions, never direct message GET.
func (s *Server) finishedAt(ctx context.Context, claims auth.StreamClaims, cursor uint64) (bool, error) {
	c, err := s.js.OrderedConsumer(ctx, s.stream, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject(claims)}, DeliverPolicy: jetstream.DeliverLastPolicy,
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
		meta, err := msg.Metadata()
		if err != nil {
			return false, err
		}
		if meta.Sequence.Stream > cursor || contract.Validate("RunEvent", msg.Data()) != nil {
			continue
		}
		var event contract.RunEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			return false, err
		}
		finished = matches(claims, event, msg.Subject()) && (event.Type == "run.completed" || event.Type == "run.failed")
	}
	return finished, batch.Error()
}
