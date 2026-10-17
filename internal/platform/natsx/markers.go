package natsx

import (
	"encoding/json/v2"
	"strings"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

// EndMarker constructs the durable run termination marker published after a
// terminal RunEvent. The marker is transport metadata, not a RunEvent.
func EndMarker(tenant, session, run, eventID string) (subject, msgID string, body []byte, err error) {
	body, err = json.Marshal(map[string]string{"run_id": run, "event_id": eventID})
	return EndSubject(tenant, session, run), "end:" + run, body, err
}

func EndSubject(tenant, session, run string) string {
	return "run." + tenant + "." + session + "." + run + ".end"
}

func IsEndSubject(subject string) bool {
	parts := strings.Split(subject, ".")
	return len(parts) == 5 && parts[0] == "run" && parts[4] == "end" &&
		ids.Valid(parts[1]) && ids.Valid(parts[2]) && ids.Valid(parts[3])
}
