-- C34 §2.4.2 delivery columns and the T1–T12 transition trigger (FM-64),
-- plus result_attempt, the idempotency key for writing a delivered decision
-- back (FM-61). last_event_seq and failure become writable by orbit_app
-- (FM-62, DECIDED_APPROVALS_LIMIT).

-- +goose Up

ALTER TABLE approvals
  ADD COLUMN delivery_state TEXT NULL,
  ADD COLUMN delivery_updated_at TIMESTAMPTZ NULL,
  ADD COLUMN delivery_attempt INT NOT NULL DEFAULT 0,
  ADD COLUMN result_attempt INT NULL;

ALTER TABLE approvals
  ADD CONSTRAINT approvals_delivery_state_check CHECK (
    delivery_state IS NULL OR delivery_state IN (
      'in_flight', 'delivered', 'unknown', 'not_delivered', 'unresolved'
    )
  );

DROP TRIGGER approvals_frozen_once_decided ON approvals;
DROP FUNCTION public.orbit_approvals_frozen_once_decided();

-- +goose StatementBegin
CREATE FUNCTION public.orbit_approvals_delivery_transition() RETURNS trigger
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

CREATE TRIGGER approvals_delivery_transition
  BEFORE UPDATE ON approvals
  FOR EACH ROW EXECUTE FUNCTION public.orbit_approvals_delivery_transition();

GRANT UPDATE (last_event_seq, failure) ON rooms TO orbit_app;
GRANT UPDATE (delivery_state, delivery_updated_at, delivery_attempt, result_attempt) ON approvals TO orbit_app;

-- +goose Down

REVOKE UPDATE (delivery_state, delivery_updated_at, delivery_attempt, result_attempt) ON approvals FROM orbit_app;
REVOKE UPDATE (last_event_seq, failure) ON rooms FROM orbit_app;

DROP TRIGGER approvals_delivery_transition ON approvals;
DROP FUNCTION public.orbit_approvals_delivery_transition();

ALTER TABLE approvals DROP CONSTRAINT approvals_delivery_state_check;
ALTER TABLE approvals
  DROP COLUMN result_attempt,
  DROP COLUMN delivery_attempt,
  DROP COLUMN delivery_updated_at,
  DROP COLUMN delivery_state;

-- +goose StatementBegin
CREATE FUNCTION public.orbit_approvals_frozen_once_decided() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
  IF OLD.status <> 'pending' THEN
    RAISE EXCEPTION 'approval is not pending' USING ERRCODE = 'P0001';
  END IF;
  RETURN NEW;
END
$$;
-- +goose StatementEnd

CREATE TRIGGER approvals_frozen_once_decided
  BEFORE UPDATE ON approvals
  FOR EACH ROW EXECUTE FUNCTION public.orbit_approvals_frozen_once_decided();
