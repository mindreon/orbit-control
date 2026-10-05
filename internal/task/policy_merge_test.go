package task

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mindreon/orbit-control/internal/orch"
)

// The workflow reads only the policy it is started with, so the tenant's caps must be merged in at create time: the
// smaller of the tenant's and the task's wins, a missing one limits nothing, and when both are missing the field is left
// out so the runtime's own default applies (max_review_rounds 5, max_concurrency 4). For review rounds 0 means off, so a
// 0 on either side switches reviews off.

type mergeClient struct {
	TaskClient
	started *orch.TaskWorkflowInput
}

func (c *mergeClient) StartTask(_ context.Context, in orch.TaskWorkflowInput) (orch.TaskView, error) {
	c.started = &in
	return orch.TaskView{}, nil
}

type tenantPolicyProjection struct {
	ProjectionStore
	tenant Policy
}

func (p *tenantPolicyProjection) GetTenantPolicy(context.Context, Principal) (Policy, error) {
	return p.tenant, nil
}
func (p *tenantPolicyProjection) CreateTask(context.Context, Principal, *Task) error { return nil }

func intp(v int) *int { return &v }

func TestCreateMergesTenantAndTaskPolicyCaps(t *testing.T) {
	rounds := []struct {
		name         string
		tenant, task *int
		want         string // JSON of the field sent to the workflow; "" means omitted
	}{
		{"unset everywhere", nil, nil, ""},
		{"tenant 0", intp(0), nil, "0"},
		{"tenant 3", intp(3), nil, "3"},
		{"task 0", nil, intp(0), "0"},
		{"task 8", nil, intp(8), "8"},
		{"tenant 0 task 8", intp(0), intp(8), "0"},
		{"tenant 3 task 0", intp(3), intp(0), "0"},
		{"tenant 3 task 8: tenant is tighter", intp(3), intp(8), "3"},
		{"tenant 0 task 0", intp(0), intp(0), "0"},
	}
	conc := []struct {
		name         string
		tenant, task *int
		want         string
	}{
		{"unset everywhere", nil, nil, ""},
		{"tenant 2", intp(2), nil, "2"},
		{"task 6", nil, intp(6), "6"},
		{"tenant 2 task 6", intp(2), intp(6), "2"},
		{"tenant 6 task 2", intp(6), intp(2), "2"},
	}
	run := func(field, name string, tenant, task *int, want string, set func(*Policy, *int)) {
		t.Run(field+"/"+name, func(t *testing.T) {
			var tenantPolicy, taskPolicy Policy
			set(&tenantPolicy, tenant)
			set(&taskPolicy, task)
			client := &mergeClient{}
			service := NewWithProjection(client, &tenantPolicyProjection{tenant: tenantPolicy})
			if _, err := service.Create(context.Background(), Principal{TenantID: "t", UserID: "u"}, CreateInput{Title: "t", Goal: "g", Policy: taskPolicy}); err != nil {
				t.Fatal(err)
			}
			if client.started == nil {
				t.Fatal("the workflow was not started")
			}
			raw, err := json.Marshal(client.started.Policy)
			if err != nil {
				t.Fatal(err)
			}
			var sent map[string]json.RawMessage
			if err := json.Unmarshal(raw, &sent); err != nil {
				t.Fatal(err)
			}
			got, present := sent[field]
			switch {
			case want == "" && present:
				t.Errorf("%s must be omitted, got %s", field, got)
			case want != "" && string(got) != want:
				t.Errorf("%s = %s (present %v), want %s", field, got, present, want)
			}
		})
	}
	for _, c := range rounds {
		run("max_review_rounds", c.name, c.tenant, c.task, c.want, func(p *Policy, v *int) { p.MaxReviewRounds = v })
	}
	for _, c := range conc {
		run("max_concurrency", c.name, c.tenant, c.task, c.want, func(p *Policy, v *int) { p.MaxConcurrency = v })
	}
}
