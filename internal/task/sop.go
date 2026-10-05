package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// SOP is a named procedure: steps and the dependencies between them. A version is immutable: registering the same id and
// version again with the same definition is a no-op, with a different one it conflicts (06 §2).
//
// Version 2 follows AgentScope 2.0.9's SOP where the concept matches (`name`, `description`, `steps`; per step `subject`,
// `description`, `max_attempts`, `executor`, `verifier`) and adds what the plan needs (`id`, `depends_on`,
// `output_schema_ref`, `required_artifacts`, `human_approval`). `executor` and `verifier` are profile refs, not Agent
// objects. A v1 definition (`{subject, description, max_attempts}` steps, or bare strings) is a valid v2 one: no ids (the
// position names them: s1, s2, ...) and no `depends_on` (a step depends on the one before it). The contract is
// orbit-runtime's `orbit_contracts.v3.sop`; the checks below are the same ones.
type SOP struct {
	SOPID       string    `json:"sop_id"`
	Version     int       `json:"version"`
	Ref         string    `json:"ref"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Steps       []SOPStep `json:"steps"`
	CreatedAt   time.Time `json:"created_at"`
}

// DefaultStepAttempts is how many refusals a step takes before the SOP gives up on it (AgentScope's default).
const DefaultStepAttempts = 3

const (
	maxSOPSteps        = 50
	maxStepAttempts    = 20
	maxSOPText         = 8000
	maxSOPSubject      = 200
	maxSOPNameLen      = 200
	sopHumanApprBefore = "before"
	sopHumanApprAfter  = "after"
)

var (
	sopStepID      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
	sopVersionedID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*@[1-9][0-9]*$`)
	sopSchemaRef   = regexp.MustCompile(`^schema://\S+/[1-9][0-9]*$`)
)

// SOPStep is one milestone: what it must achieve, how many tries it gets, who does it and who judges it, and what it waits for.
type SOPStep struct {
	// ID names the step in `depends_on`; left out, it is s<position>.
	ID          string `json:"id"`
	Subject     string `json:"subject"`
	Description string `json:"description"`
	MaxAttempts int    `json:"max_attempts"`
	// Executor is the profile ref (expert@3) that does the step; empty is the task's expert.
	Executor string `json:"executor,omitempty"`
	// Verifier says who judges the step and by what; nil is the default verifier, which still judges it.
	Verifier *SOPVerifier `json:"verifier,omitempty"`
	// DependsOn lists the ids of the steps this one waits for. Absent (nil) is the step before it; empty is a step that
	// starts at once. A stored step always has it spelled out.
	DependsOn         []string          `json:"depends_on"`
	OutputSchemaRef   string            `json:"output_schema_ref,omitempty"`
	RequiredArtifacts []SOPArtifactSpec `json:"required_artifacts,omitempty"`
	// HumanApproval makes a person approve before the step starts ("before") or after it finished ("after").
	HumanApproval string `json:"human_approval,omitempty"`
}

// SOPVerifier is the extra criteria a step is judged by, and the expert whose instructions the verifier is given.
type SOPVerifier struct {
	Instructions string `json:"instructions,omitempty"`
	Expert       string `json:"expert,omitempty"`
}

// SOPArtifactSpec is a file the step must leave in the workspace.
type SOPArtifactSpec struct {
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	MinCount  int    `json:"min_count,omitempty"`
}

// UnmarshalJSON accepts a bare string as shorthand for a step whose subject and description are that text, and refuses
// fields it does not know: the runtime's contract refuses them too, and a step stored with one would be unusable.
func (st *SOPStep) UnmarshalJSON(raw []byte) error {
	var shorthand string
	if json.Unmarshal(raw, &shorthand) == nil {
		*st = SOPStep{Subject: shorthand, Description: shorthand}
		return nil
	}
	type plain SOPStep
	var full plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&full); err != nil {
		return err
	}
	*st = SOPStep(full)
	return nil
}

// InvalidSOPError is a definition that cannot be registered, with what is wrong with it.
type InvalidSOPError struct{ Reason string }

func (e *InvalidSOPError) Error() string { return "invalid SOP: " + e.Reason }

func invalidSOP(format string, args ...any) error {
	return &InvalidSOPError{Reason: fmt.Sprintf(format, args...)}
}

// NormalizeSOPSteps gives every step its defaults: the id, the description, the attempts and what it depends on. It is
// idempotent and does not judge the steps, so a stored v1 definition reads as the v2 one it is.
func NormalizeSOPSteps(steps []SOPStep) []SOPStep {
	out := make([]SOPStep, len(steps))
	for index, step := range steps {
		step.Subject = strings.TrimSpace(step.Subject)
		if step.ID == "" {
			step.ID = fmt.Sprintf("s%d", index+1)
		}
		if strings.TrimSpace(step.Description) == "" {
			step.Description = step.Subject
		}
		if step.MaxAttempts == 0 {
			step.MaxAttempts = DefaultStepAttempts
		}
		switch {
		case step.DependsOn == nil && index > 0:
			step.DependsOn = []string{fmt.Sprintf("s%d", index)}
			if previous := steps[index-1].ID; previous != "" {
				step.DependsOn = []string{previous}
			}
		case step.DependsOn == nil:
			step.DependsOn = []string{}
		default:
			step.DependsOn = dedupe(step.DependsOn)
		}
		for i := range step.RequiredArtifacts {
			if step.RequiredArtifacts[i].MinCount == 0 {
				step.RequiredArtifacts[i].MinCount = 1
			}
		}
		out[index] = step
	}
	return out
}

func dedupe(items []string) []string {
	seen := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}

// ValidateSOP checks a definition the way the runtime does and returns it normalized: one to 50 steps, unique ids, every
// dependency a known step other than itself, no cycle, and the fields within their limits.
func ValidateSOP(sop SOP) (SOP, error) {
	if len(sop.Steps) == 0 {
		return SOP{}, invalidSOP("an SOP needs at least one step")
	}
	if len(sop.Steps) > maxSOPSteps {
		return SOP{}, invalidSOP("an SOP has at most %d steps", maxSOPSteps)
	}
	sop.Name = strings.TrimSpace(sop.Name)
	if sop.Name == "" {
		sop.Name = sop.SOPID
	}
	if len(sop.Name) > maxSOPNameLen || len(sop.Description) > maxSOPText {
		return SOP{}, invalidSOP("the name or the description is too long")
	}
	// Ids first: a step with no id is named by its position, which must not take one that was written.
	written := map[string]bool{}
	for _, step := range sop.Steps {
		if step.ID == "" {
			continue
		}
		if !sopStepID.MatchString(step.ID) {
			return SOP{}, invalidSOP("step id %q is not valid", step.ID)
		}
		if written[step.ID] {
			return SOP{}, invalidSOP("step ids must be unique: %q is used twice", step.ID)
		}
		written[step.ID] = true
	}
	for index, step := range sop.Steps {
		if step.ID == "" && written[fmt.Sprintf("s%d", index+1)] {
			return SOP{}, invalidSOP("step %d has no id and its default id s%d is taken", index+1, index+1)
		}
	}
	steps := NormalizeSOPSteps(sop.Steps)
	ids := make(map[string]int, len(steps))
	for index, step := range steps {
		if ids[step.ID] != 0 {
			return SOP{}, invalidSOP("step ids must be unique: %q is used twice", step.ID)
		}
		ids[step.ID] = index + 1
	}
	for index, step := range steps {
		if err := validateSOPStep(index+1, step); err != nil {
			return SOP{}, err
		}
		for _, dependency := range step.DependsOn {
			if dependency == step.ID {
				return SOP{}, invalidSOP("step %s depends on itself", step.ID)
			}
			if ids[dependency] == 0 {
				return SOP{}, invalidSOP("step %s depends on unknown step %q", step.ID, dependency)
			}
		}
	}
	if sopHasCycle(steps) {
		return SOP{}, invalidSOP("the steps' dependencies contain a cycle")
	}
	sop.Steps = steps
	return sop, nil
}

func validateSOPStep(position int, step SOPStep) error {
	if step.Subject == "" || len(step.Subject) > maxSOPSubject {
		return invalidSOP("step %d needs a subject of at most %d characters", position, maxSOPSubject)
	}
	if len(step.Description) > maxSOPText {
		return invalidSOP("step %s: the description is too long", step.ID)
	}
	if step.MaxAttempts < 1 || step.MaxAttempts > maxStepAttempts {
		return invalidSOP("step %s: max_attempts is from 1 to %d", step.ID, maxStepAttempts)
	}
	if step.Executor != "" && !sopVersionedID.MatchString(step.Executor) {
		return invalidSOP("step %s: executor %q is not a versioned profile ref (expert@3)", step.ID, step.Executor)
	}
	if verifier := step.Verifier; verifier != nil {
		if len(verifier.Instructions) > maxSOPText {
			return invalidSOP("step %s: the verifier's instructions are too long", step.ID)
		}
		if verifier.Expert != "" && !sopVersionedID.MatchString(verifier.Expert) {
			return invalidSOP("step %s: verifier expert %q is not a versioned profile ref (expert@3)", step.ID, verifier.Expert)
		}
	}
	if step.OutputSchemaRef != "" && !sopSchemaRef.MatchString(step.OutputSchemaRef) {
		return invalidSOP("step %s: output_schema_ref must look like schema://name/1", step.ID)
	}
	for _, artifact := range step.RequiredArtifacts {
		if artifact.Name == "" || artifact.MediaType == "" || artifact.MinCount < 1 {
			return invalidSOP("step %s: a required artifact needs a name, a media type and a count of at least 1", step.ID)
		}
	}
	if step.HumanApproval != "" && step.HumanApproval != sopHumanApprBefore && step.HumanApproval != sopHumanApprAfter {
		return invalidSOP("step %s: human_approval is \"before\", \"after\" or left out", step.ID)
	}
	return nil
}

// sopHasCycle runs Kahn's algorithm over the dependencies.
func sopHasCycle(steps []SOPStep) bool {
	waiting := make(map[string]int, len(steps))
	dependents := map[string][]string{}
	for _, step := range steps {
		waiting[step.ID] = len(step.DependsOn)
		for _, dependency := range step.DependsOn {
			dependents[dependency] = append(dependents[dependency], step.ID)
		}
	}
	ready := []string{}
	for _, step := range steps {
		if waiting[step.ID] == 0 {
			ready = append(ready, step.ID)
		}
	}
	done := 0
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		done++
		for _, next := range dependents[id] {
			waiting[next]--
			if waiting[next] == 0 {
				ready = append(ready, next)
			}
		}
	}
	return done != len(steps)
}

// SameSOP reports whether two definitions are the same one once normalized: what makes registering a version again a no-op.
func SameSOP(a, b SOP) bool {
	left, _ := json.Marshal(sopIdentity(a))
	right, _ := json.Marshal(sopIdentity(b))
	return bytes.Equal(left, right)
}

func sopIdentity(sop SOP) any {
	name := strings.TrimSpace(sop.Name)
	if name == "" {
		name = sop.SOPID
	}
	return struct {
		Name        string    `json:"name"`
		Description string    `json:"description"`
		Steps       []SOPStep `json:"steps"`
	}{name, sop.Description, NormalizeSOPSteps(sop.Steps)}
}

var errSOPNeedsStore = errNeedsStore("the SOP registry")

func (s *Service) RegisterSOP(ctx context.Context, p Principal, sop SOP) (SOP, error) {
	if p.TenantID == "" || p.UserID == "" || sop.SOPID == "" || sop.Version < 1 {
		return SOP{}, errors.New("tenant, user, sop_id and positive version are required")
	}
	sop, err := ValidateSOP(sop)
	if err != nil {
		return SOP{}, err
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
