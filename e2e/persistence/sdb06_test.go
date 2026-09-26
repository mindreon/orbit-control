//go:build e2e

package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store/migrations"
)

const schemaSnapshotSQL = `
SELECT coalesce(string_agg(x, E'\n' ORDER BY x), '') FROM (
  SELECT 'col:' || table_name || '.' || column_name || ':' || data_type || ':' || is_nullable || ':' || coalesce(column_default, '') AS x
    FROM information_schema.columns WHERE table_schema = 'public' AND table_name <> 'goose_db_version'
  UNION ALL
  SELECT 'con:' || conrelid::regclass::text || ':' || conname || ':' || pg_get_constraintdef(oid)
    FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND conrelid <> 0 AND conrelid::regclass::text <> 'goose_db_version'
  UNION ALL
  SELECT 'idx:' || indexname || ':' || indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename <> 'goose_db_version'
  UNION ALL
  SELECT 'pol:' || tablename || ':' || policyname || ':' || cmd || ':' || coalesce(qual, '') || ':' || coalesce(with_check, '')
    FROM pg_policies WHERE schemaname = 'public'
  UNION ALL
  SELECT 'fn:' || p.oid::regprocedure::text || ':' || pg_get_userbyid(p.proowner) || ':' || coalesce(array_to_string(p.proacl, ','), '')
    FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace
  UNION ALL
  SELECT 'rls:' || relname || ':' || relrowsecurity || ':' || relforcerowsecurity
    FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind = 'r' AND relname <> 'goose_db_version'
  UNION ALL
  SELECT 'grant:' || table_name || ':' || grantee || ':' || privilege_type
    FROM information_schema.role_table_grants WHERE table_schema = 'public' AND table_name <> 'goose_db_version'
  UNION ALL
  SELECT 'colgrant:' || table_name || '.' || column_name || ':' || grantee || ':' || privilege_type
    FROM information_schema.column_privileges WHERE table_schema = 'public' AND grantee = 'orbit_definer'
) s`

const objectCountSQL = `
SELECT (SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r','S','i') AND relname NOT LIKE 'goose_db_version%'),
       (SELECT count(*) FROM pg_policies WHERE schemaname = 'public'),
       (SELECT count(*) FROM pg_proc WHERE pronamespace = 'public'::regnamespace)`

type schemaState struct {
	Fingerprint string `json:"fingerprint"`
	Objects     int    `json:"relationsSequencesIndexes"`
	Policies    int    `json:"policies"`
	Functions   int    `json:"functions"`
}

func snapshot(ctx context.Context) (schemaState, error) {
	conn, err := pgx.Connect(ctx, ownerURL)
	if err != nil {
		return schemaState{}, err
	}
	defer conn.Close(ctx)
	var text string
	var st schemaState
	if err := conn.QueryRow(ctx, schemaSnapshotSQL).Scan(&text); err != nil {
		return st, err
	}
	if err := conn.QueryRow(ctx, objectCountSQL).Scan(&st.Objects, &st.Policies, &st.Functions); err != nil {
		return st, err
	}
	if text != "" {
		sum := sha256.Sum256([]byte(text))
		st.Fingerprint = hex.EncodeToString(sum[:])
	}
	return st, nil
}

func dropVersionTable(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, ownerURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS goose_db_version`)
	return err
}

type migStep struct {
	Applied int         `json:"applied"`
	Version int64       `json:"version"`
	Error   string      `json:"error,omitempty"`
	Schema  schemaState `json:"schema"`
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return sqlState(err)
}

// runSDB06Migrations is S-DB-6 (ISO-12). It leaves the database migrated
// for the rest of the suite.
func runSDB06Migrations(ctx context.Context) bool {
	const c = "S-DB-6"
	fms := []string{"FM-35", "FM-36", "FM-37"}
	ok := true
	step := func(id, desc string, steps []string, expected, actual any, pass bool) {
		got := recordCase(caseInput{ID: id, Contract: c, Kind: "isolated", FailureModes: fms, Description: desc,
			Steps: steps, Request: map[string]string{"role": "orbit_owner", "tool": "goose (embedded migrations)"},
			Expected: expected, Actual: actual, Pass: pass})
		ok = ok && got.Pass
	}

	resetErr := migrations.Reset(ctx, ownerURL)
	if resetErr != nil {
		// A database that was never migrated has no version table yet.
		resetErr = nil
	}
	dropErr := dropVersionTable(ctx)
	empty, snapErr := snapshot(ctx)
	step("S-DB-6/empty-schema", "start from an empty schema: every migration down, goose version table dropped",
		[]string{"goose down to 0 (if migrated)", "DROP TABLE goose_db_version", "snapshot public schema"},
		map[string]any{"objects": 0, "policies": 0, "functions": 0},
		map[string]any{"objects": empty.Objects, "policies": empty.Policies, "functions": empty.Functions, "error": errText(firstErr(resetErr, dropErr, snapErr))},
		dropErr == nil && snapErr == nil && empty.Objects == 0 && empty.Policies == 0 && empty.Functions == 0)

	up := func() migStep {
		n, err := migrations.Up(ctx, ownerURL)
		v, verr := migrations.Version(ctx, ownerURL)
		s, serr := snapshot(ctx)
		return migStep{Applied: n, Version: v, Error: errText(firstErr(err, verr, serr)), Schema: s}
	}
	first := up()
	step("S-DB-6/up-on-empty", "up on an empty database succeeds and applies every migration",
		[]string{"goose up", "read version", "snapshot schema"},
		map[string]any{"applied": 3, "version": 3, "error": ""}, first,
		first.Error == "" && first.Applied == 3 && first.Version == 3 && first.Schema.Fingerprint != "")

	second := up()
	step("S-DB-6/up-again-noop", "up on an already migrated database succeeds and changes nothing",
		[]string{"goose up", "read version", "snapshot schema", "compare with the previous snapshot"},
		map[string]any{"applied": 0, "version": 3, "error": "", "schemaFingerprint": first.Schema.Fingerprint},
		map[string]any{"applied": second.Applied, "version": second.Version, "error": second.Error, "schemaFingerprint": second.Schema.Fingerprint},
		second.Error == "" && second.Applied == 0 && second.Version == 3 && second.Schema.Fingerprint == first.Schema.Fingerprint)

	downErr := migrations.Reset(ctx, ownerURL)
	down, derr := snapshot(ctx)
	step("S-DB-6/down-clean", "down to 0 removes every table, policy and function",
		[]string{"goose down to 0", "snapshot schema"},
		map[string]any{"error": "", "objects": 0, "policies": 0, "functions": 0},
		map[string]any{"error": errText(firstErr(downErr, derr)), "objects": down.Objects, "policies": down.Policies, "functions": down.Functions},
		downErr == nil && derr == nil && down.Objects == 0 && down.Policies == 0 && down.Functions == 0)

	third := up()
	step("S-DB-6/up-after-down", "up after down reproduces the fresh schema exactly",
		[]string{"goose up", "snapshot schema", "compare with the fresh-up snapshot"},
		map[string]any{"applied": 3, "version": 3, "error": "", "schemaFingerprint": first.Schema.Fingerprint},
		map[string]any{"applied": third.Applied, "version": third.Version, "error": third.Error, "schemaFingerprint": third.Schema.Fingerprint},
		third.Error == "" && third.Applied == 3 && third.Version == 3 && third.Schema.Fingerprint == first.Schema.Fingerprint)
	return ok
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
