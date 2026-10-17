// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Adapted from openjiuwen agent_teams/prompts/en/scheduler_*.md.
package team

import (
	"fmt"
	"strings"

	"github.com/mindreon/orbit-control/internal/contract"
)

const (
	MailNotice    = "[Team Mail] Read the messages and user steering delivered with this turn, then coordinate the team."
	BudgetNotice  = "[Budget Low] The run budget is nearly exhausted. Call team_finish to summarize the completed work."
	SettledNotice = "[All Settled] No work can advance. Call team_finish with your summary, or create or reassign tasks."
)

func TaskNotice(task contract.TeamTask, board contract.TeamBoard) string {
	var text strings.Builder
	text.WriteString(fmt.Sprintf("[Task Started] Task [%s] %q is assigned to you — begin now.\n\n## Goal and acceptance criteria\n\n%s\n",
		task.TaskId, task.Title, task.Description))
	if task.Review != nil && task.Review.ReworkNote != nil && *task.Review.ReworkNote != "" {
		text.WriteString(fmt.Sprintf("\n## Reviewer feedback\n\n%s\n", *task.Review.ReworkNote))
	}
	for _, dep := range task.Deps {
		for _, prior := range board.Tasks {
			if prior.TaskId != dep {
				continue
			}
			text.WriteString(fmt.Sprintf("\n## Dependency [%s]\n", dep))
			if prior.ResultText != nil {
				text.WriteString(fmt.Sprintf("%s\n", *prior.ResultText))
			}
			for _, file := range prior.Files {
				text.WriteString(fmt.Sprintf("- /workspace/%s\n", file))
			}
		}
	}
	text.WriteString("\nCall team_complete_task when done. Use team_send_message to reach the leader for clarification.\n")
	return text.String()
}
func ReviewNotice(task contract.TeamTask) string {
	var text strings.Builder
	text.WriteString(fmt.Sprintf("[Review Assigned] Verify task [%s] %q against its acceptance criteria.\n\n%s\n\n## Deliverable\n",
		task.TaskId, task.Title, task.Description))
	if task.ResultText != nil {
		text.WriteString(fmt.Sprintf("%s\n", *task.ResultText))
	}
	for _, file := range task.Files {
		text.WriteString(fmt.Sprintf("- /workspace/%s\n", file))
	}
	text.WriteString("\nInspect the files in /workspace. Call team_submit_verdict with approve or reject and concrete feedback.\n")
	return text.String()
}
func EscalationNotice(ids []string, board contract.TeamBoard) string {
	var text strings.Builder
	text.WriteString("[Tasks Escalated] Automatic rework has stopped. Reassign tasks or call team_finish.\n")
	for _, id := range ids {
		for _, task := range board.Tasks {
			if task.TaskId != id {
				continue
			}
			text.WriteString(fmt.Sprintf("- [%s] %s", id, task.Title))
			for _, dependent := range board.Tasks {
				for _, dep := range dependent.Deps {
					if dep == id {
						text.WriteString(fmt.Sprintf("; blocks %s", dependent.TaskId))
					}
				}
			}
			text.WriteByte('\n')
		}
	}
	return text.String()
}
