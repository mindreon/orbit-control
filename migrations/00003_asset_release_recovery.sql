-- +goose Up
ALTER TABLE asset_versions ADD COLUMN uploaded_by uuid REFERENCES principals(id);
ALTER TABLE asset_versions ADD COLUMN release_error text;
ALTER TABLE asset_versions ADD COLUMN release_retryable boolean NOT NULL DEFAULT true;
ALTER TABLE asset_versions ADD COLUMN permissions_jcs bytea;
ALTER TABLE asset_versions ADD COLUMN identity_jcs bytea;
REVOKE DELETE ON asset_versions,asset_files FROM orbit_app;
REVOKE UPDATE ON asset_files FROM orbit_app;
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION protect_asset_content() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.asset_id,NEW.version,NEW.manifest,NEW.content_digest,NEW.blob_index_ref,NEW.permissions,NEW.compat,NEW.uploaded_by,NEW.permissions_jcs,NEW.identity_jcs)
 IS DISTINCT FROM (OLD.asset_id,OLD.version,OLD.manifest,OLD.content_digest,OLD.blob_index_ref,OLD.permissions,OLD.compat,OLD.uploaded_by,OLD.permissions_jcs,OLD.identity_jcs)
 THEN RAISE EXCEPTION 'asset version content is immutable' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION protect_asset_content() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.asset_id,NEW.version,NEW.manifest,NEW.content_digest,NEW.blob_index_ref,NEW.permissions,NEW.compat)
 IS DISTINCT FROM (OLD.asset_id,OLD.version,OLD.manifest,OLD.content_digest,OLD.blob_index_ref,OLD.permissions,OLD.compat)
 THEN RAISE EXCEPTION 'asset version content is immutable' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
ALTER TABLE asset_versions DROP COLUMN uploaded_by,DROP COLUMN release_error,DROP COLUMN release_retryable,DROP COLUMN permissions_jcs,DROP COLUMN identity_jcs;
GRANT DELETE ON asset_versions,asset_files TO orbit_app;
GRANT UPDATE ON asset_files TO orbit_app;
