package team

import "encoding/json"

// Marshal uses the legacy JSON encoder: string-keyed maps are sorted and HTML
// escaping is enabled, matching Temporal's default payload representation.
// workflowcheck follows Go 1.27 reflection into runtime implementation details;
// this narrow boundary has no custom marshalers with IO or workflow commands.
//
//workflowcheck:ignore
func Marshal(value any) ([]byte, error) { return json.Marshal(value) }

// Unmarshal decodes recorded values into contract structs, never iterates a map
// to issue commands, and uses no clock or external state.
//
//workflowcheck:ignore
func Unmarshal(raw []byte, out any) error { return json.Unmarshal(raw, out) }
