// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Adapted from openjiuwen agent_teams/agent/scheduling/scheduler.py.
package team

import "github.com/mindreon/orbit-control/internal/contract"

type Active struct {
	Member, Kind, TaskID string
}
type Action struct {
	Member, Kind, TaskID string
}

func Busy(taskID string, active []Active) bool {
	for _, turn := range active {
		if turn.TaskID == taskID {
			return true
		}
	}
	return false
}
func DependenciesReady(task contract.TeamTask, board contract.TeamBoard, active []Active) bool {
	for _, dep := range task.Deps {
		found := false
		for _, prior := range board.Tasks {
			if prior.TaskId == dep {
				found = prior.Status == "completed" && !Busy(dep, active)
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func capacity(active []Active, maxParallel int) (map[string]bool, int) {
	busy := map[string]bool{}
	for _, turn := range active {
		busy[turn.Member] = true
		if turn.Kind != "lead" {
			maxParallel--
		}
	}
	return busy, maxParallel
}

// StartScan preserves creation order and returns an updated board without
// mutating the supplied task slice. Blocked dependencies unlock only when idle.
func StartScan(board contract.TeamBoard, active []Active, maxParallel int, reserve bool) (contract.TeamBoard, []Action) {
	board.Tasks = append([]contract.TeamTask(nil), board.Tasks...)
	actions := []Action{}
	busy, remaining := capacity(active, maxParallel)
	for i := range board.Tasks {
		task := &board.Tasks[i]
		if !DependenciesReady(*task, board, active) {
			continue
		}
		if task.Status == "blocked" {
			task.Status = "pending"
		}
		if reserve || remaining <= 0 || task.Status != "pending" || task.Assignee == nil || busy[string(*task.Assignee)] {
			continue
		}
		member := string(*task.Assignee)
		actions = append(actions, Action{Member: member, Kind: "task", TaskID: task.TaskId})
		busy[member] = true
		remaining--
	}
	return board, actions
}

func ReviewScan(board contract.TeamBoard, active []Active, maxParallel int, reserve bool) []Action {
	actions := []Action{}
	if reserve {
		return actions
	}
	busy, remaining := capacity(active, maxParallel)
	for _, task := range board.Tasks {
		if task.Status != "in_review" || task.Review == nil {
			continue
		}
		assigneeBusy := false
		for _, turn := range active {
			if turn.TaskID == task.TaskId && turn.Kind == "task" {
				assigneeBusy = true
			}
		}
		if assigneeBusy {
			continue
		}
		voted := map[contract.MemberName]bool{}
		for _, vote := range task.Review.Votes {
			voted[vote.Reviewer] = true
		}
		for _, reviewer := range task.Review.Reviewers {
			if remaining > 0 && !busy[string(reviewer)] && !voted[reviewer] {
				actions = append(actions, Action{Member: string(reviewer), Kind: "review", TaskID: task.TaskId})
				busy[string(reviewer)] = true
				remaining--
			}
		}
	}
	return actions
}
