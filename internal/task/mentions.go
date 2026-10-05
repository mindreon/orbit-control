package task

import (
	"context"
	"fmt"
	"strings"
)

// A user can @-mention members of the task's team in a message (15 M8, T8.6). Mentions are roles of the team in the task's
// configuration at the moment of sending; the workflow gets them as `mentions` on sendMessage.

// MaxMentions is the most roles one message can name: a team has at most eight members.
const MaxMentions = 8

// MentionError is a refusal of a message's mentions. It is a 422: the message is well formed, but names a role the task
// cannot give it to.
type MentionError struct {
	Code   string // UNKNOWN_MENTION or MENTIONS_NOT_ALLOWED
	Field  string // "mentions" or "mentions[i]"
	Reason string
}

func (e *MentionError) Error() string { return fmt.Sprintf("%s: %s: %s", e.Code, e.Field, e.Reason) }

// NormalizeMentions drops a leading @ and surrounding spaces and removes repeats, keeping the first of each. It needs
// no team, so the same request always normalizes to the same list and hashes alike.
func NormalizeMentions(mentions []string) []string {
	seen := make(map[string]struct{}, len(mentions))
	out := make([]string, 0, len(mentions))
	for _, role := range mentions {
		role = strings.TrimPrefix(strings.TrimSpace(role), "@")
		if _, dup := seen[role]; dup {
			continue
		}
		seen[role] = struct{}{}
		out = append(out, role)
	}
	return out
}

// CheckMentions accepts normalized mentions only when the task has a team and every one is a role of it. It reads the
// team as the task's configuration has it now; callers run it through UpdateChecked so that a replay does not.
func (s *Service) CheckMentions(ctx context.Context, p Principal, id string, mentions []string) error {
	if len(mentions) == 0 {
		return nil
	}
	view, err := s.TaskConfig(ctx, p, id)
	if err != nil {
		return err
	}
	if view.Team == nil {
		return &MentionError{Code: "MENTIONS_NOT_ALLOWED", Field: "mentions", Reason: "this task has no team to mention"}
	}
	roles := make(map[string]struct{}, len(view.Team.Members))
	for _, member := range view.Team.Members {
		roles[member.Role] = struct{}{}
	}
	for i, role := range mentions {
		if _, ok := roles[role]; !ok {
			return &MentionError{Code: "UNKNOWN_MENTION", Field: fmt.Sprintf("mentions[%d]", i), Reason: "not a role of this task's team"}
		}
	}
	return nil
}
