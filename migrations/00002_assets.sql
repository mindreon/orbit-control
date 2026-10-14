-- +goose Up
ALTER TABLE asset_channels DROP CONSTRAINT asset_channels_channel_check;
ALTER TABLE asset_channels ADD CHECK (channel ~ '^[a-z][a-z0-9_-]{0,63}$');
ALTER TABLE asset_versions ADD COLUMN release_channel text NOT NULL DEFAULT 'stable';
ALTER TABLE asset_versions ADD COLUMN release_started boolean NOT NULL DEFAULT false;
ALTER TABLE asset_versions ADD COLUMN withdraw_requested boolean NOT NULL DEFAULT false;
ALTER TABLE asset_versions ADD COLUMN withdraw_delivered boolean NOT NULL DEFAULT false;
ALTER TABLE asset_versions ADD COLUMN review_required int NOT NULL DEFAULT 1 CHECK(review_required BETWEEN 1 AND 20);
ALTER TABLE asset_versions ADD COLUMN workspace_review boolean NOT NULL DEFAULT true;
ALTER TABLE asset_versions ADD COLUMN previous_version_id uuid;
ALTER TABLE asset_versions ADD COLUMN previous_channel_id uuid;
ALTER TABLE asset_versions ADD COLUMN release_generation int NOT NULL DEFAULT 0;
ALTER TABLE asset_reviews ADD COLUMN generation int NOT NULL DEFAULT 0;
ALTER TABLE asset_reviews ADD COLUMN delivered boolean NOT NULL DEFAULT false;
CREATE UNIQUE INDEX asset_review_once ON asset_reviews(tenant_id,version_id,generation,reviewer_id);
CREATE UNIQUE INDEX asset_report_once ON asset_check_reports(tenant_id,version_id,"check");
CREATE UNIQUE INDEX asset_install_scope ON asset_installs(tenant_id,scope_type,scope_id,asset_id);
ALTER TABLE run_locks ADD COLUMN lock jsonb;

-- Prevent any caller (including owner) from replacing a version's frozen content.
-- +goose StatementBegin
CREATE FUNCTION protect_asset_content() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.asset_id,NEW.version,NEW.manifest,NEW.content_digest,NEW.blob_index_ref,NEW.permissions,NEW.compat)
 IS DISTINCT FROM (OLD.asset_id,OLD.version,OLD.manifest,OLD.content_digest,OLD.blob_index_ref,OLD.permissions,OLD.compat)
 THEN RAISE EXCEPTION 'asset version content is immutable' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER asset_content_immutable BEFORE UPDATE ON asset_versions FOR EACH ROW EXECUTE FUNCTION protect_asset_content();
-- +goose Down
DROP TRIGGER asset_content_immutable ON asset_versions;
DROP FUNCTION protect_asset_content();
ALTER TABLE run_locks DROP COLUMN lock;
DROP INDEX asset_install_scope;
DROP INDEX asset_report_once;
DROP INDEX asset_review_once;
ALTER TABLE asset_reviews DROP COLUMN delivered, DROP COLUMN generation;
ALTER TABLE asset_versions DROP COLUMN release_channel, DROP COLUMN release_started, DROP COLUMN withdraw_requested, DROP COLUMN withdraw_delivered, DROP COLUMN review_required, DROP COLUMN workspace_review, DROP COLUMN previous_version_id, DROP COLUMN previous_channel_id, DROP COLUMN release_generation;
ALTER TABLE asset_channels DROP CONSTRAINT asset_channels_channel_check;
-- A downgrade cannot represent custom channels.
DELETE FROM asset_channels WHERE channel NOT IN ('stable','beta');
ALTER TABLE asset_channels ADD CHECK(channel IN ('stable','beta'));
