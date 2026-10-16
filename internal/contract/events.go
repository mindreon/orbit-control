package contract

import (
	"encoding/json/v2"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// EventFilter is shared by live streams and persisted history.
type EventFilter struct {
	Types   []string
	Exclude []string
}

func ParseEventFilter(q url.Values) (EventFilter, error) {
	f := EventFilter{Types: []string{}, Exclude: []string{}}
	_, types := q["types"]
	_, exclude := q["exclude"]
	if types && exclude {
		return f, fmt.Errorf("types and exclude are mutually exclusive")
	}
	for _, name := range []string{"types", "exclude"} {
		values, present := q[name]
		if !present {
			continue
		}
		if len(values) != 1 {
			return f, fmt.Errorf("invalid event filter")
		}
		for value := range strings.SplitSeq(values[0], ",") {
			raw, err := json.Marshal(value)
			if err != nil {
				return f, err
			}
			if err := Validate("EventType", raw); err != nil {
				return f, fmt.Errorf("invalid event filter")
			}
			if name == "types" {
				f.Types = append(f.Types, value)
			} else {
				f.Exclude = append(f.Exclude, value)
			}
		}
	}
	return f, nil
}

func (f EventFilter) Allows(kind string) bool {
	return (len(f.Types) == 0 || slices.Contains(f.Types, kind)) && !slices.Contains(f.Exclude, kind)
}

// ParseEventCursor accepts only decimal uint64, including the zero sentinel.
func ParseEventCursor(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, fmt.Errorf("invalid cursor")
		}
	}
	return strconv.ParseUint(value, 10, 64)
}
