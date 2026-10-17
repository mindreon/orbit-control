package team

type FailureClass string

const (
	Retryable   FailureClass = "retryable"
	Invariant   FailureClass = "invariant"
	TaskFailure FailureClass = "task"
)

var failureClasses = map[string]FailureClass{
	"SEGMENT_FAILED": Retryable, "STUCK": Retryable, "SEGMENT_LIMIT": Retryable,
	"CONTEXT_TOO_LARGE": Retryable, "MODEL_ERROR": Retryable, "AGENT_ERROR": Retryable,
	"LOOP_DETECTED": Retryable, "SANDBOX_ERROR": Retryable,
	"CHECKPOINT_CONFLICT": Invariant, "CHECKPOINT_UNREADABLE": Invariant,
	"LOCK_VERIFICATION_FAILED": Invariant, "INVALID_SEGMENT_INPUT": Invariant,
	"INVALID_SEGMENT_RESULT": Invariant, "INVALID_SPEC": Invariant, "SEGMENT_SUPERSEDED": Invariant,
	"START_SEGMENT_FAILED": Invariant, "RECORD_SEGMENT_FAILED": Invariant,
	"RECONCILE_FAILED": Invariant, "STATUS_FAILED": Invariant, "SANDBOX_UNAVAILABLE": Invariant,
	"TURN_LOST": Invariant, "TURN_START_CONFLICT": Invariant,
	"POLICY_DENIED": TaskFailure, "TOOL_POLICY_ERROR": TaskFailure, "INTERACTION_FAILED": TaskFailure,
	"UNATTENDED_INTERACTION": TaskFailure, "PREPARE_FAILED": TaskFailure,
	"QUEUE_FULL": TaskFailure, "RUN_NOT_ACTIVE": TaskFailure, "INTERACTION_NOT_PENDING": TaskFailure,
	"CANCELLED": TaskFailure, "ASSET_SUSPENDED": TaskFailure,
}

func Classify(code string) FailureClass {
	if class, ok := failureClasses[code]; ok {
		return class
	}
	return TaskFailure
}

// Classified lets tests distinguish an explicitly reviewed code from a fallback.
func Classified(code string) bool {
	_, ok := failureClasses[code]
	return ok
}
