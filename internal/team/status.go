// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Adapted from openjiuwen agent_teams/schema/status.py; scheduled mode only.
package team

import "github.com/mindreon/orbit-control/internal/contract"

var transitions = map[contract.TaskStatus][]contract.TaskStatus{
	"pending":     {"in_progress", "blocked", "cancelled"},
	"blocked":     {"pending", "cancelled"},
	"in_progress": {"in_review", "completed", "pending", "cancelled", "failed"},
	"in_review":   {"completed", "pending", "cancelled", "failed"},
	"failed":      {"pending"},
	"completed":   {},
	"cancelled":   {},
}

// CanTransition describes state edges; callers enforce operation-specific authority.
func CanTransition(from, to contract.TaskStatus) bool {
	for _, allowed := range transitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

func Terminal(status contract.TaskStatus) bool {
	return status == "completed" || status == "cancelled" || status == "failed"
}
