// Package schedules owns schedule validation, authorization and synchronization.
package schedules

import (
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/robfig/cron/v3"

	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

func ValidateCron(expression, timezone string, now time.Time) error {
	if timezone == "" || timezone == "Local" {
		return httpx.E(422, "SCHEDULE_INVALID_TIMEZONE", "Use an IANA timezone")
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return httpx.E(422, "SCHEDULE_INVALID_TIMEZONE", "Unknown timezone")
	}
	fields := strings.Fields(expression)
	if len(fields) != 5 || strings.HasPrefix(expression, "@") || strings.Contains(expression, "TZ=") ||
		(fields[2] != "*" && fields[4] != "*") {
		return httpx.E(422, "SCHEDULE_INVALID_CRON", "Use a five-field cron with no simultaneous day and weekday restriction")
	}
	schedule, err := cron.ParseStandard(expression)
	if err != nil {
		return httpx.E(422, "SCHEDULE_INVALID_CRON", "Invalid cron expression")
	}
	cursor := now.In(location)
	end := cursor.AddDate(0, 0, 366)
	var previous time.Time
	for range 50 {
		next := schedule.Next(cursor)
		if next.IsZero() || next.After(end) {
			return nil
		}
		if !previous.IsZero() && next.Sub(previous) < 5*time.Minute {
			return httpx.E(422, "SCHEDULE_INTERVAL_TOO_SHORT", "Schedule interval must be at least five minutes")
		}
		previous, cursor = next, next
	}
	return nil
}
