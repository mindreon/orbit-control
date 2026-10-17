// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Adapted from openjiuwen agent_teams/agent/scheduling/verdict.py.
package team

import (
	"strings"

	"github.com/mindreon/orbit-control/internal/contract"
)

// Settle returns the settled state and next review metadata. A single reject
// vetoes immediately; no score pool or temporary reviewers participate.
func Settle(task contract.TeamTask, maxRounds int) (contract.TaskStatus, contract.TaskReview) {
	if task.Review == nil {
		return task.Status, contract.TaskReview{}
	}
	review := *task.Review
	comments := []string{}
	approved := map[contract.MemberName]bool{}
	for _, vote := range review.Votes {
		if vote.Verdict == "reject" {
			comments = append(comments, vote.Comment)
		} else if vote.Verdict == "approve" {
			approved[vote.Reviewer] = true
		}
	}
	if len(comments) > 0 {
		round := 1
		if review.Round != nil {
			round += *review.Round
		}
		review.Round = new(round)
		review.ReworkNote = new(TruncateEncoded(strings.Join(comments, "\n"), CommentBytes))
		if round >= maxRounds {
			return "failed", review
		}
		return "pending", review
	}
	for _, reviewer := range review.Reviewers {
		if !approved[reviewer] {
			return task.Status, review
		}
	}
	return "completed", review
}
