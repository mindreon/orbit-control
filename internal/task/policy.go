package task

import (
	"context"
	"errors"
	"strings"
)

// Policy limits what an attempt may do (05 §6). The tenant, the task and the agent profile each carry one, and they
// only ever tighten each other: denied tools add up and the smallest exploration cap wins. A missing field limits
// nothing.
type Policy struct {
	DeniedTools             []string `json:"denied_tools"`
	ExplorationMaxToolCalls *int     `json:"exploration_max_tool_calls,omitempty"`
}

func (p Policy) validate() error {
	for _, name := range p.DeniedTools {
		if strings.TrimSpace(name) == "" {
			return errors.New("denied_tools must not contain an empty name")
		}
	}
	if p.ExplorationMaxToolCalls != nil && *p.ExplorationMaxToolCalls < 0 {
		return errors.New("exploration_max_tool_calls must not be negative")
	}
	return nil
}

func (p Policy) normalized() Policy {
	if p.DeniedTools == nil {
		p.DeniedTools = []string{}
	}
	return p
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
