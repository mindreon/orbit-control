package task

import (
	"context"
	"encoding/json"
)

// ProjectionStore is the durable task projection boundary. The service keeps
// its in-process cache for fan-out, while this interface owns restart safety.
type ProjectionStore interface {
	CommandLedger
	CreateTask(context.Context, Principal, *Task) error
	GetTask(context.Context, Principal, string) (*Task, error)
	ListTasks(context.Context, Principal) ([]*Task, error)
	UpdateTask(context.Context, Principal, *Task) error
	DeleteTask(context.Context, Principal, string) error
	AppendTaskEvent(context.Context, Event) (Event, error)
	ListTaskEvents(context.Context, Principal, string, uint64) ([]Event, uint64, error)
	RegisterProfile(context.Context, Principal, Profile) (Profile, error)
	ListProfiles(context.Context, Principal) ([]Profile, error)
	GetProfile(context.Context, Principal, string) (Profile, error)
	GetTenantPolicy(context.Context, Principal) (Policy, error)
	SetTenantPolicy(context.Context, Principal, Policy) (Policy, error)
	RegisterSOP(context.Context, Principal, SOP) (SOP, error)
	ListSOPs(context.Context, Principal) ([]SOP, error)
	ListManifests(context.Context, Principal, string) ([]ArtifactManifest, error)
	GetManifest(context.Context, Principal, string) (ArtifactManifest, error)
}

// NodeStructure is where a node sits in a compiled SOP, as the projection kept it from node.status_changed.
type NodeStructure struct {
	ParentNodeID string          // empty: the node is not nested
	SopStep      json.RawMessage // a JSON object; nil: the node is not part of an SOP
}

// NodeStructureReader is implemented by a projection that keeps NodeStructure. getPlan does not carry the SOP step, so
// the plan API adds it from here.
type NodeStructureReader interface {
	NodeStructure(ctx context.Context, p Principal, taskID string) (map[string]NodeStructure, error)
}
