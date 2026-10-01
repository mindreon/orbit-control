//go:build e2e

package persistence

import (
	"net/http"
	"strings"
	"testing"
)

// The worker reads the files of the skills a task was given from control's internal listener (15 T8.4).
//
// How it can go wrong, written down before the code:
//   - a skill that is not in the catalog, or whose text was never filled, is served as if it were fine;
//   - an id built to climb out of the catalog (`..`, encoded slashes, NUL) or one far longer than any skill id is not
//     refused cleanly (it must be a 404, never a 500 or a path that reads something else);
//   - the response says why a skill is refused, which tells a caller more than "not found".
//
// The bundle itself (a skill with a SKILL.md and readable text) needs catalog rows, which only the full stack has: the
// persistence database starts without the shipped snapshot. E25's neighbour on the stack covers that path.

func TestInternalSkillsAreRefusedCleanly(t *testing.T) {
	s := startServer(t, serverOpts{tenant: "t-internal-skills", maxConns: 4})
	cases := []struct{ id, path string }{
		{"unknown-two-segments", "/internal/skills/nobody/none"},
		{"unknown-one-segment", "/internal/skills/nobody"},
		{"dotdot", "/internal/skills/%2E%2E/%2E%2E"},
		{"encoded-slash", "/internal/skills/a%2Fb/c"},
		{"nul-byte", "/internal/skills/a%00b/c"},
		{"too-long", "/internal/skills/" + strings.Repeat("h", 400) + "/" + strings.Repeat("s", 400)},
	}
	for _, c := range cases {
		s.check(t, "INTERNAL-SKILLS/"+c.id, "INTERNAL-SKILLS", "refuse "+c.id+" with a plain 404", httpReq{
			Method: http.MethodGet, Path: c.path,
		}, httpExp{Status: http.StatusNotFound, BodyExcludes: []string{"cannot be used", "usable", "SKILL.md", "files"}})
	}
}
