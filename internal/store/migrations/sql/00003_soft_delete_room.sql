-- Contract §18.5 / §18.7a (C32 rev2, sign-off amendment). Soft delete cannot
-- be a plain UPDATE from orbit_app: the new row fails rooms_select
-- (deleted_at IS NULL), and Postgres checks the new row against SELECT
-- policies when the UPDATE's WHERE reads table columns. This SECURITY DEFINER
-- function is therefore the only soft-delete path. It is owned by the
-- NOLOGIN BYPASSRLS role orbit_definer, re-checks tenant/creator/liveness
-- itself, and returns the affected row count (0 → 404).
--
-- It is the single entry in the S-DB-11 SECURITY DEFINER allowlist
-- (internal/store/pgstore/sdb11_static_test.go).

-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION orbit_soft_delete_room(p_id text, p_user text) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
  v_tenant text := current_setting('app.tenant_id', true);
  v_count  integer;
BEGIN
  IF v_tenant IS NULL OR v_tenant = '' OR p_user IS NULL OR p_user = '' THEN
    RETURN 0;
  END IF;
  UPDATE public.rooms
     SET deleted_at = now(), deleted_by = p_user, updated_at = now()
   WHERE id = p_id
     AND tenant_id = v_tenant
     AND created_by = p_user
     AND deleted_at IS NULL;
  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END
$$;
-- +goose StatementEnd

-- ALTER ... OWNER requires the new owner to hold CREATE on the schema; grant
-- it only for the ownership transfer.
GRANT CREATE ON SCHEMA public TO orbit_definer;
ALTER FUNCTION orbit_soft_delete_room(text, text) OWNER TO orbit_definer;
REVOKE CREATE ON SCHEMA public FROM orbit_definer;

GRANT SELECT (id, tenant_id, created_by, deleted_at),
      UPDATE (deleted_at, deleted_by, updated_at)
   ON rooms TO orbit_definer;

REVOKE ALL ON FUNCTION orbit_soft_delete_room(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION orbit_soft_delete_room(text, text) TO orbit_app;

-- +goose Down
DROP FUNCTION orbit_soft_delete_room(text, text);
REVOKE ALL ON rooms FROM orbit_definer;
