package schedules

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

func TestCronAndTimezone(t *testing.T) {
	now := time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC)
	for _, tc := range []struct{ cron, zone, code string }{
		{cron: "*/4 * * * *", zone: "UTC", code: "SCHEDULE_INTERVAL_TOO_SHORT"},
		{cron: "*/5 * * * *", zone: "Asia/Shanghai"},
		{cron: "0 9 * * 1", zone: "UTC"},
		{cron: "@every 5m", zone: "UTC", code: "SCHEDULE_INVALID_CRON"},
		{cron: "CRON_TZ=UTC 0 9 * * *", zone: "UTC", code: "SCHEDULE_INVALID_CRON"},
		{cron: "0 9 1 * 1", zone: "UTC", code: "SCHEDULE_INVALID_CRON"},
		{cron: "0 0 29 2 *", zone: "UTC"},
		{cron: "*/5 * * * *", zone: "America/New_York"},
		{cron: "* * * * *", zone: "Local", code: "SCHEDULE_INVALID_TIMEZONE"},
		{cron: "0 9 * * *", zone: "Mars/Base", code: "SCHEDULE_INVALID_TIMEZONE"},
		{cron: "0 9 * * *", zone: "", code: "SCHEDULE_INVALID_TIMEZONE"},
	} {
		t.Run(tc.cron+tc.zone, func(t *testing.T) {
			err := ValidateCron(tc.cron, tc.zone, now)
			if tc.code == "" {
				require.NoError(t, err)
			} else {
				var typed *httpx.Error
				require.ErrorAs(t, err, &typed)
				require.Equal(t, tc.code, typed.Code)
			}
		})
	}
}
