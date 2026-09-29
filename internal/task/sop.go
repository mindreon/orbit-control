package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SOP is an ordered list of steps. A version is immutable: registering the same id and version again with the same
// steps is a no-op, with different steps it conflicts (06 §2).
type SOP struct {
	SOPID     string    `json:"sop_id"`
	Version   int       `json:"version"`
	Ref       string    `json:"ref"`
	Steps     []SOPStep `json:"steps"`
	CreatedAt time.Time `json:"created_at"`
}

// DefaultStepAttempts is how many refusals a step takes before the SOP gives up on it (AgentScope's default).
const DefaultStepAttempts = 3

// SOPStep is one milestone: a name, what it must achieve, and how many tries it gets.
type SOPStep struct {
	Subject     string `json:"subject"`
	Description string `json:"description"`
	MaxAttempts int    `json:"max_attempts"`
}

// UnmarshalJSON accepts a bare string as shorthand for a step whose subject and description are that text.
func (st *SOPStep) UnmarshalJSON(raw []byte) error {
	var shorthand string
	if json.Unmarshal(raw, &shorthand) == nil {
		*st = SOPStep{Subject: shorthand, Description: shorthand}
		return nil
	}
	type plain SOPStep
	var full plain
	if err := json.Unmarshal(raw, &full); err != nil {
		return err
	}
	*st = SOPStep(full)
	return nil
}

func (st SOPStep) normalized() SOPStep {
	if st.Description == "" {
		st.Description = st.Subject
	}
	if st.MaxAttempts == 0 {
		st.MaxAttempts = DefaultStepAttempts
	}
	return st
}

var errSOPNeedsStore = errors.New("the SOP registry needs the durable task store")

func (s *Service) RegisterSOP(ctx context.Context, p Principal, sop SOP) (SOP, error) {
	if p.TenantID == "" || p.UserID == "" || sop.SOPID == "" || sop.Version < 1 {
		return SOP{}, errors.New("tenant, user, sop_id and positive version are required")
	}
	if len(sop.Steps) == 0 {
		return SOP{}, errors.New("an SOP needs at least one step")
	}
	for index, step := range sop.Steps {
		step = step.normalized()
		if strings.TrimSpace(step.Subject) == "" || step.MaxAttempts < 1 {
			return SOP{}, errors.New("every SOP step needs a subject and at least one attempt")
		}
		sop.Steps[index] = step
	}
	if s.projection == nil {
		return SOP{}, errSOPNeedsStore
	}
	sop.Ref = fmt.Sprintf("%s@%d", sop.SOPID, sop.Version)
	return s.projection.RegisterSOP(ctx, p, sop)
}

func (s *Service) ListSOPs(ctx context.Context, p Principal) ([]SOP, error) {
	if s.projection == nil {
		return nil, errSOPNeedsStore
	}
	return s.projection.ListSOPs(ctx, p)
}
