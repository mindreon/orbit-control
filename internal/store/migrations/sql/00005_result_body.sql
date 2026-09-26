-- C35: result_body so a restarted control can apply a delivered result (FM-71),
-- and CHECK constraints on approvals.status and approvals.decision (FM-73).
-- The trigger gains a stash branch and requires result_body unchanged on T1–T12.

-- +goose Up

ALTER TABLE approvals
  ADD COLUMN result_body JSONB NULL;

ALTER TABLE approvals
  ADD CONSTRAINT approvals_status_check CHECK (
    status IN ('pending', 'decided', 'cancelled')
  );

ALTER TABLE approvals
  ADD CONSTRAINT approvals_decision_check CHECK (
    decision IN ('', 'allow', 'reject')
  );

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.orbit_approvals_delivery_transition() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
  IF OLD.id IS DISTINCT FROM NEW.id
     OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
     OR OLD.task_id IS DISTINCT FROM NEW.task_id
     OR OLD.approval_request_id IS DISTINCT FROM NEW.approval_request_id
     OR OLD.call_id IS DISTINCT FROM NEW.call_id
     OR OLD.turn_id IS DISTINCT FROM NEW.turn_id
     OR OLD.agent_id IS DISTINCT FROM NEW.agent_id
     OR OLD.agent_path IS DISTINCT FROM NEW.agent_path
     OR OLD.tool_name IS DISTINCT FROM NEW.tool_name
     OR OLD.reason IS DISTINCT FROM NEW.reason
     OR OLD.arguments IS DISTINCT FROM NEW.arguments
     OR OLD.risk IS DISTINCT FROM NEW.risk
     OR OLD.allow_always IS DISTINCT FROM NEW.allow_always
     OR OLD.rule_id IS DISTINCT FROM NEW.rule_id
     OR OLD.created_at IS DISTINCT FROM NEW.created_at
  THEN
    RAISE EXCEPTION 'approval is not pending' USING ERRCODE = 'P0001';
  END IF;

  -- FM-71: stash the result while delivered and result_attempt is still null. Not a delivery transition.
  IF OLD.delivery_state = 'delivered'
     AND NEW.delivery_state = 'delivered'
     AND OLD.status IS NOT DISTINCT FROM NEW.status
     AND OLD.decision IS NOT DISTINCT FROM NEW.decision
     AND OLD.decided_at IS NOT DISTINCT FROM NEW.decided_at
     AND OLD.delivery_attempt = NEW.delivery_attempt
     AND OLD.result_attempt IS NULL
     AND NEW.result_attempt IS NULL
     AND OLD.result_body IS NULL
     AND NEW.result_body IS NOT NULL
  THEN
    NEW.delivery_updated_at := OLD.delivery_updated_at;
    RETURN NEW;
  END IF;


  -- FM-61: record that this delivery_attempt's result was written. No delivery transition.
  IF OLD.delivery_state = 'delivered'
     AND NEW.delivery_state = 'delivered'
     AND OLD.status IS NOT DISTINCT FROM NEW.status
     AND OLD.decision IS NOT DISTINCT FROM NEW.decision
     AND OLD.decided_at IS NOT DISTINCT FROM NEW.decided_at
     AND OLD.delivery_attempt = NEW.delivery_attempt
     AND OLD.result_attempt IS NULL
     AND NEW.result_attempt = OLD.delivery_attempt
     AND NEW.result_body IS NOT DISTINCT FROM OLD.result_body
  THEN
    NEW.delivery_updated_at := OLD.delivery_updated_at;
    RETURN NEW;
  END IF;

  -- T1: claim. status stays 'decided'; decision is allow or reject (C34 "allowed"/"rejected").
  IF OLD.status = 'pending' AND OLD.delivery_state IS NULL
     AND NEW.status = 'decided'
     AND NEW.decision IN ('allow', 'reject')
     AND NEW.delivery_state = 'in_flight'
     AND NEW.delivery_attempt = OLD.delivery_attempt + 1
     AND NEW.decided_at IS NOT NULL
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NEW.result_body IS NOT DISTINCT FROM OLD.result_body
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T2, T3, T4, T5: leave in_flight. Attempt must match so a late result cannot land on a newer claim.
  IF OLD.status = 'decided' AND OLD.delivery_state = 'in_flight'
     AND NEW.status = 'decided'
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NEW.delivery_state IN ('delivered', 'unknown', 'not_delivered', 'unresolved')
     AND NEW.result_body IS NOT DISTINCT FROM OLD.result_body
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T6: reconciler claims another try.
  IF OLD.delivery_state = 'unknown' AND NEW.delivery_state = 'in_flight'
     AND NEW.status IS NOT DISTINCT FROM OLD.status
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt + 1
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NEW.result_body IS NOT DISTINCT FROM OLD.result_body
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T7, T8.
  IF OLD.delivery_state = 'unknown'
     AND NEW.delivery_state IN ('delivered', 'unresolved')
     AND NEW.status IS NOT DISTINCT FROM OLD.status
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NEW.result_body IS NOT DISTINCT FROM OLD.result_body
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T9: reopen only from not_delivered.
  IF OLD.status = 'decided' AND OLD.delivery_state = 'not_delivered'
     AND NEW.status = 'pending' AND NEW.decision = '' AND NEW.decided_at IS NULL
     AND NEW.delivery_state IS NULL
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NEW.result_body IS NOT DISTINCT FROM OLD.result_body
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T10: cancel a pending approval.
  IF OLD.status = 'pending' AND OLD.delivery_state IS NULL
     AND NEW.status = 'cancelled' AND NEW.delivery_state IS NULL
     AND NEW.decided_at IS NOT NULL
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NEW.result_body IS NOT DISTINCT FROM OLD.result_body
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T11: delivered, then the resumed turn reports DECIDED_APPROVALS_LIMIT.
  IF OLD.delivery_state = 'delivered' AND NEW.delivery_state = 'unresolved'
     AND NEW.status IS NOT DISTINCT FROM OLD.status
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NEW.result_body IS NOT DISTINCT FROM OLD.result_body
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T12: unresolved that did not fail the room (T8), once a result is known.
  IF OLD.delivery_state = 'unresolved' AND NEW.delivery_state = 'delivered'
     AND NEW.status IS NOT DISTINCT FROM OLD.status
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NOT EXISTS (
       SELECT 1 FROM rooms r
        WHERE r.id = NEW.task_id AND r.tenant_id = NEW.tenant_id AND r.state = 'failed'
     )
     AND NEW.result_body IS NOT DISTINCT FROM OLD.result_body
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  RAISE EXCEPTION 'approval is not pending' USING ERRCODE = 'P0001';
END;
$$;
-- +goose StatementEnd

GRANT UPDATE (result_body) ON approvals TO orbit_app;

-- +goose Down

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION public.orbit_approvals_delivery_transition() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
  IF OLD.id IS DISTINCT FROM NEW.id
     OR OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
     OR OLD.task_id IS DISTINCT FROM NEW.task_id
     OR OLD.approval_request_id IS DISTINCT FROM NEW.approval_request_id
     OR OLD.call_id IS DISTINCT FROM NEW.call_id
     OR OLD.turn_id IS DISTINCT FROM NEW.turn_id
     OR OLD.agent_id IS DISTINCT FROM NEW.agent_id
     OR OLD.agent_path IS DISTINCT FROM NEW.agent_path
     OR OLD.tool_name IS DISTINCT FROM NEW.tool_name
     OR OLD.reason IS DISTINCT FROM NEW.reason
     OR OLD.arguments IS DISTINCT FROM NEW.arguments
     OR OLD.risk IS DISTINCT FROM NEW.risk
     OR OLD.allow_always IS DISTINCT FROM NEW.allow_always
     OR OLD.rule_id IS DISTINCT FROM NEW.rule_id
     OR OLD.created_at IS DISTINCT FROM NEW.created_at
  THEN
    RAISE EXCEPTION 'approval is not pending' USING ERRCODE = 'P0001';
  END IF;

  -- FM-61: record that this delivery_attempt's result was written. No delivery transition.
  IF OLD.delivery_state = 'delivered'
     AND NEW.delivery_state = 'delivered'
     AND OLD.status IS NOT DISTINCT FROM NEW.status
     AND OLD.decision IS NOT DISTINCT FROM NEW.decision
     AND OLD.decided_at IS NOT DISTINCT FROM NEW.decided_at
     AND OLD.delivery_attempt = NEW.delivery_attempt
     AND OLD.result_attempt IS NULL
     AND NEW.result_attempt = OLD.delivery_attempt
  THEN
    NEW.delivery_updated_at := OLD.delivery_updated_at;
    RETURN NEW;
  END IF;

  -- T1: claim. status stays 'decided'; decision is allow or reject (C34 "allowed"/"rejected").
  IF OLD.status = 'pending' AND OLD.delivery_state IS NULL
     AND NEW.status = 'decided'
     AND NEW.decision IN ('allow', 'reject')
     AND NEW.delivery_state = 'in_flight'
     AND NEW.delivery_attempt = OLD.delivery_attempt + 1
     AND NEW.decided_at IS NOT NULL
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T2, T3, T4, T5: leave in_flight. Attempt must match so a late result cannot land on a newer claim.
  IF OLD.status = 'decided' AND OLD.delivery_state = 'in_flight'
     AND NEW.status = 'decided'
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NEW.delivery_state IN ('delivered', 'unknown', 'not_delivered', 'unresolved')
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T6: reconciler claims another try.
  IF OLD.delivery_state = 'unknown' AND NEW.delivery_state = 'in_flight'
     AND NEW.status IS NOT DISTINCT FROM OLD.status
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt + 1
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T7, T8.
  IF OLD.delivery_state = 'unknown'
     AND NEW.delivery_state IN ('delivered', 'unresolved')
     AND NEW.status IS NOT DISTINCT FROM OLD.status
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T9: reopen only from not_delivered.
  IF OLD.status = 'decided' AND OLD.delivery_state = 'not_delivered'
     AND NEW.status = 'pending' AND NEW.decision = '' AND NEW.decided_at IS NULL
     AND NEW.delivery_state IS NULL
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T10: cancel a pending approval.
  IF OLD.status = 'pending' AND OLD.delivery_state IS NULL
     AND NEW.status = 'cancelled' AND NEW.delivery_state IS NULL
     AND NEW.decided_at IS NOT NULL
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T11: delivered, then the resumed turn reports DECIDED_APPROVALS_LIMIT.
  IF OLD.delivery_state = 'delivered' AND NEW.delivery_state = 'unresolved'
     AND NEW.status IS NOT DISTINCT FROM OLD.status
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  -- T12: unresolved that did not fail the room (T8), once a result is known.
  IF OLD.delivery_state = 'unresolved' AND NEW.delivery_state = 'delivered'
     AND NEW.status IS NOT DISTINCT FROM OLD.status
     AND NEW.decision IS NOT DISTINCT FROM OLD.decision
     AND NEW.decided_at IS NOT DISTINCT FROM OLD.decided_at
     AND NEW.delivery_attempt = OLD.delivery_attempt
     AND NEW.result_attempt IS NOT DISTINCT FROM OLD.result_attempt
     AND NOT EXISTS (
       SELECT 1 FROM rooms r
        WHERE r.id = NEW.task_id AND r.tenant_id = NEW.tenant_id AND r.state = 'failed'
     )
  THEN
    NEW.delivery_updated_at := now();
    RETURN NEW;
  END IF;

  RAISE EXCEPTION 'approval is not pending' USING ERRCODE = 'P0001';
END;
$$;
-- +goose StatementEnd

ALTER TABLE approvals DROP CONSTRAINT approvals_decision_check;
ALTER TABLE approvals DROP CONSTRAINT approvals_status_check;

REVOKE UPDATE (result_body) ON approvals FROM orbit_app;
ALTER TABLE approvals DROP COLUMN result_body;
