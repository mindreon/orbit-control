package config

import (
	"fmt"
	"os"
	"strconv"
)

func metricLimits() (int, int, error) {
	values := []int{500, 50}
	for i, name := range []string{"ORBIT_METRICS_TENANT_LIMIT", "ORBIT_METRICS_MODEL_LIMIT"} {
		if raw := os.Getenv(name); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 {
				return 0, 0, fmt.Errorf("%s must be positive", name)
			}
			values[i] = n
		}
	}
	return values[0], values[1], nil
}
