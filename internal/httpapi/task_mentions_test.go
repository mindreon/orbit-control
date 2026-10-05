package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/app"
	v3 "github.com/mindreon/orbit-control/internal/contract/v3"
	"github.com/mindreon/orbit-control/internal/orch"
)

// A user can @-mention roles of the task's team in a message. The mentions are checked against the team the task's
// configuration has now, before anything is sent to the workflow.
//
// How it can go wrong, written down before the code:
//   - a role the team does not have, or a mention on a task without a team, reaches the workflow;
//   - the refusal is not a 422 with the code and the field of the mention;
//   - mentions are dropped on the way to the workflow, or a message without any gets an empty `mentions` that changes its
//     request hash;
//   - the same command_id with other mentions is answered as a replay instead of 409;
//   - a mention that is not a list of strings is accepted.

type teamClient struct {
	planClient
	team map[string]any
}

func (c *teamClient) GetTaskView(context.Context, string, string) (orch.TaskView, error) {
	config := map[string]any{"config_version": float64(1), "mode": "default"}
	if c.team != nil {
		config["team"] = c.team
	}
	return orch.TaskView{Config: config}, nil
}

func newMentionTask(t *testing.T, team map[string]any) (http.Handler, *teamClient, string) {
	t.Helper()
	client := &teamClient{team: team}
	h := HandlerWith(app.NewWithOptions(app.Options{TaskClient: client}))
	create := httptest.NewRecorder()
	h.ServeHTTP(create, internalReq(http.MethodPost, "/v1/tasks", `{"title":"demo","goal":"ship"}`))
	var task struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal(create.Body.Bytes(), &task); err != nil || task.ID == "" {
		t.Fatalf("create task = %d: %s", create.Code, create.Body.String())
	}
	return h, client, task.ID
}

func crew() map[string]any {
	return map[string]any{"leader": "lead", "members": []any{
		map[string]any{"role": "lead", "expert": "expert_a@1"}, map[string]any{"role": "writer", "expert": "expert_b@1"}}}
}

func TestMessageMentionsAreCheckedAgainstTheTeamAndForwarded(t *testing.T) {
	h, client, id := newMentionTask(t, crew())
	path := "/v1/tasks/" + id + "/messages"

	unknown := post(h, path, `{"command_id":"00000000000000000000000001","text":"hi","mentions":["writer","ghost"]}`)
	if unknown.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unknown mention = %d: %s", unknown.Code, unknown.Body.String())
	}
	for _, want := range []string{`"code":"UNKNOWN_MENTION"`, `"field":"mentions[1]"`} {
		if !strings.Contains(unknown.Body.String(), want) {
			t.Errorf("missing %s in %s", want, unknown.Body.String())
		}
	}
	for _, bad := range []string{`"writer"`, `[1]`, `[null]`, `["a","b","c","d","e","f","g","h","i"]`} {
		if rec := post(h, path, `{"command_id":"00000000000000000000000002","text":"hi","mentions":`+bad+`}`); rec.Code != http.StatusBadRequest {
			t.Errorf("mentions %s = %d: %s", bad, rec.Code, rec.Body.String())
		}
	}
	if n := len(client.sent("sendMessage")); n != 0 {
		t.Fatalf("refused messages reached the workflow %d times", n)
	}

	body := `{"command_id":"00000000000000000000000003","text":"hi","mentions":["@writer","writer","lead"]}`
	first := post(h, path, body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("mention = %d: %s", first.Code, first.Body.String())
	}
	sent := client.sent("sendMessage")
	got, _ := sent[0].payload["mentions"].([]string)
	if len(sent) != 1 || len(got) != 2 || got[0] != "writer" || got[1] != "lead" {
		t.Fatalf("the update the workflow got: %+v", sent)
	}
	if replay := post(h, path, body); replay.Code != http.StatusAccepted || replay.Body.String() != first.Body.String() || len(client.sent("sendMessage")) != 1 {
		t.Fatalf("same command and body is a replay: %d %s", replay.Code, replay.Body.String())
	}
	other := post(h, path, `{"command_id":"00000000000000000000000003","text":"hi","mentions":["lead"]}`)
	if other.Code != http.StatusConflict || !strings.Contains(other.Body.String(), "IDEMPOTENCY_KEY_REUSED") {
		t.Fatalf("other mentions under the same command = %d: %s", other.Code, other.Body.String())
	}

	plain := post(h, path, `{"command_id":"00000000000000000000000004","text":"no one","mentions":[]}`)
	if plain.Code != http.StatusAccepted {
		t.Fatalf("no mentions = %d: %s", plain.Code, plain.Body.String())
	}
	if _, has := client.sent("sendMessage")[1].payload["mentions"]; has {
		t.Error("a message without mentions carries no mentions field, so its request hash is unchanged")
	}
}

func TestMentionsNeedATeam(t *testing.T) {
	h, client, id := newMentionTask(t, nil)
	rec := post(h, "/v1/tasks/"+id+"/messages", `{"command_id":"00000000000000000000000005","text":"hi","mentions":["lead"]}`)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), `"code":"MENTIONS_NOT_ALLOWED"`) || !strings.Contains(rec.Body.String(), `"field":"mentions"`) {
		t.Fatalf("mentions without a team = %d: %s", rec.Code, rec.Body.String())
	}
	if len(client.sent("sendMessage")) != 0 {
		t.Error("nothing may reach the workflow")
	}
	if rec := post(h, "/v1/tasks/"+id+"/messages", `{"command_id":"00000000000000000000000006","text":"plain"}`); rec.Code != http.StatusAccepted {
		t.Errorf("a plain message still works: %d %s", rec.Code, rec.Body.String())
	}
}

// What control sends as sendMessage must decode strictly into the contract's SendMessageInput.
func TestSentMessageDecodesStrictlyIntoTheContract(t *testing.T) {
	h, client, id := newMentionTask(t, crew())
	if rec := post(h, "/v1/tasks/"+id+"/messages", `{"command_id":"00000000000000000000000007","text":"hi","mentions":["writer"]}`); rec.Code != http.StatusAccepted {
		t.Fatalf("send = %d: %s", rec.Code, rec.Body.String())
	}
	raw, _ := json.Marshal(client.sent("sendMessage")[0].payload)
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	var in v3.SendMessageInput
	if err := dec.Decode(&in); err != nil || len(in.Mentions) != 1 || in.Mentions[0] != "writer" {
		t.Fatalf("SendMessageInput does not accept %s: %v %+v", raw, err, in)
	}
}

// Idempotency wins over validation: a retry of a command that already succeeded returns its stored result even though the
// team it was checked against is gone, and only a command seen for the first time is checked.
func TestReplayDoesNotRecheckMentionsAfterTheTeamChanged(t *testing.T) {
	h, client, id := newMentionTask(t, crew())
	path := "/v1/tasks/" + id + "/messages"
	body := `{"command_id":"00000000000000000000000008","text":"hi","mentions":["writer"]}`
	first := post(h, path, body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("send = %d: %s", first.Code, first.Body.String())
	}

	client.team = nil // the task's team is removed
	replay := post(h, path, body)
	if replay.Code != http.StatusAccepted || replay.Body.String() != first.Body.String() {
		t.Fatalf("replay after the team changed = %d: %s (first %s)", replay.Code, replay.Body.String(), first.Body.String())
	}
	if n := len(client.sent("sendMessage")); n != 1 {
		t.Fatalf("the replay reached the workflow: %d updates", n)
	}
	changed := post(h, path, `{"command_id":"00000000000000000000000008","text":"hi","mentions":["lead"]}`)
	if changed.Code != http.StatusConflict || !strings.Contains(changed.Body.String(), "IDEMPOTENCY_KEY_REUSED") {
		t.Fatalf("a different body is still 409: %d %s", changed.Code, changed.Body.String())
	}
	fresh := post(h, path, `{"command_id":"00000000000000000000000009","text":"hi","mentions":["writer"]}`)
	if fresh.Code != http.StatusUnprocessableEntity || !strings.Contains(fresh.Body.String(), "MENTIONS_NOT_ALLOWED") {
		t.Fatalf("a new command is checked against the team as it is now: %d %s", fresh.Code, fresh.Body.String())
	}

	// A refused command gave its claim up: once the team has the role, the same command id goes through.
	client.team = crew()
	retry := post(h, path, `{"command_id":"00000000000000000000000009","text":"hi","mentions":["writer"]}`)
	if retry.Code != http.StatusAccepted {
		t.Fatalf("retry of a refused command once the team has the role = %d: %s", retry.Code, retry.Body.String())
	}
}
