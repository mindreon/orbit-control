package task

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Policy limits what an attempt may do (05 §6). The tenant, the task and the agent profile each carry one, and they
// only ever tighten each other: denied tools add up and the smallest exploration cap and the smallest concurrency win.
// A missing field limits nothing.
type Policy struct {
	DeniedTools             []string `json:"denied_tools"`
	ExplorationMaxToolCalls *int     `json:"exploration_max_tool_calls,omitempty"`
	// MaxConcurrency is how many attempts of a task may run at once. Missing means the orchestrator's default (4).
	MaxConcurrency *int `json:"max_concurrency,omitempty"`
	// MaxReviewRounds is how many times a reviewer may send a node back (0 turns review off). Missing means the
	// runtime's default (5). Layers tighten each other: the smaller wins, so a 0 anywhere switches review off.
	MaxReviewRounds *int `json:"max_review_rounds,omitempty"`
}

// MaxReviewRoundsLimit is the most review rounds a policy may allow.
const MaxReviewRoundsLimit = 20

// MaxConcurrencyLimit is the most attempts one task may be allowed to run at once.
const MaxConcurrencyLimit = 64

func (p Policy) validate() error {
	for _, name := range p.DeniedTools {
		if strings.TrimSpace(name) == "" {
			return errors.New("denied_tools must not contain an empty name")
		}
	}
	if p.ExplorationMaxToolCalls != nil && *p.ExplorationMaxToolCalls < 0 {
		return errors.New("exploration_max_tool_calls must not be negative")
	}
	if p.MaxConcurrency != nil && (*p.MaxConcurrency < 1 || *p.MaxConcurrency > MaxConcurrencyLimit) {
		return fmt.Errorf("max_concurrency must be between 1 and %d", MaxConcurrencyLimit)
	}
	if p.MaxReviewRounds != nil && (*p.MaxReviewRounds < 0 || *p.MaxReviewRounds > MaxReviewRoundsLimit) {
		return fmt.Errorf("max_review_rounds must be between 0 and %d", MaxReviewRoundsLimit)
	}
	return nil
}

func (p Policy) normalized() Policy {
	if p.DeniedTools == nil {
		p.DeniedTools = []string{}
	}
	return p
}

// smallest is the tighter of two optional caps; a missing one limits nothing.
func smallest(a, b *int) *int {
	switch {
	case a == nil && b == nil:
		return nil
	case a == nil:
		return &[]int{*b}[0]
	case b == nil || *a <= *b:
		return &[]int{*a}[0]
	}
	return &[]int{*b}[0]
}

func (s *Service) GetTenantPolicy(ctx context.Context, p Principal) (Policy, error) {
	if s.projection == nil {
		return Policy{}, errNeedsStore("the tenant policy")
	}
	return s.projection.GetTenantPolicy(ctx, p)
}

func (s *Service) SetTenantPolicy(ctx context.Context, p Principal, policy Policy) (Policy, error) {
	if p.TenantID == "" || p.UserID == "" {
		return Policy{}, errors.New("tenant and user are required")
	}
	if err := policy.validate(); err != nil {
		return Policy{}, err
	}
	if s.projection == nil {
		return Policy{}, errNeedsStore("the tenant policy")
	}
	return s.projection.SetTenantPolicy(ctx, p, policy.normalized())
}

func errNeedsStore(what string) error {
	return errors.New(what + " needs the durable task store")
}
