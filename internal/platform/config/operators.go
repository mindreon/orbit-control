package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

func (c *Config) loadOperators() error {
	c.PlatformOperators = map[string]bool{}
	if raw := os.Getenv("ORBIT_PLATFORM_OPERATORS"); raw != "" {
		for item := range strings.SplitSeq(raw, ",") {
			id := strings.TrimSpace(item)
			if !ids.Valid(id) {
				return fmt.Errorf("ORBIT_PLATFORM_OPERATORS must contain principal UUIDs")
			}
			c.PlatformOperators[strings.ToLower(id)] = true
		}
	}
	c.OpsTemporalURLTemplate = os.Getenv("ORBIT_OPS_TEMPORAL_URL_TEMPLATE")
	c.OpsTraceURLTemplate = os.Getenv("ORBIT_OPS_TRACE_URL_TEMPLATE")
	for _, template := range []string{c.OpsTemporalURLTemplate, c.OpsTraceURLTemplate} {
		remaining := strings.NewReplacer("{namespace}", "", "{workflow_id}", "", "{trace_id}", "").Replace(template)
		if strings.ContainsAny(remaining, "{}") {
			return fmt.Errorf("unsupported ops URL template placeholder")
		}
	}
	return nil
}
