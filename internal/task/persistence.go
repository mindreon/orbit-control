package task

import "context"

// ProjectionStore is the durable task projection boundary. The service keeps
// its in-process cache for fan-out, while this interface owns restart safety.
type ProjectionStore interface {
	CreateTask(context.Context, Principal, *Task) error
	GetTask(context.Context, Principal, string) (*Task, error)
	ListTasks(context.Context, Principal) ([]*Task, error)
	UpdateTask(context.Context, Principal, *Task) error
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
