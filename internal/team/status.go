// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package team

import "github.com/mindreon/orbit-control/internal/contract"

func Terminal(status contract.TaskStatus) bool {
	return status == "completed" || status == "cancelled" || status == "failed"
}
