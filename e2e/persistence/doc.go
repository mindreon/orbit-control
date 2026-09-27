// Package persistence is the contract §18 E2E suite (build tag e2e). It drives
// the S-DB-13 scenarios through the HTTP API against a real Postgres and
// writes artifacts/e2e-persistence-report.json. Isolated SQL/role/static
// checks are only those listed in docs/persistence-failure-modes.md.
//
//	go test -tags e2e -count=1 ./e2e/...
//
// Needs ORBIT_TEST_DB_URL (orbit_app) and ORBIT_TEST_MIGRATE_DB_URL
// (orbit_owner) on a database prepared by deploy/postgres/bootstrap-roles.sql.
package persistence
