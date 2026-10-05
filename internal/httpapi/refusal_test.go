package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/orch"
)

func TestWriteOrchRefusalAnswersByType(t *testing.T) {
	refused := func(refusalType string) error {
		return errors.Join(errors.New("wrapped"), &orch.Refusal{Type: refusalType, Message: "no"})
	}
	for refusalType, want := range map[string]int{"UNKNOWN_APPROVAL": http.StatusNotFound, "SCHEMA_INVALID": http.StatusUnprocessableEntity, "INVALID_TRANSITION": http.StatusConflict, "NEW_TYPE": http.StatusConflict} {
		rec := httptest.NewRecorder()
		if !writeOrchRefusal(rec, refused(refusalType)) || rec.Code != want || !strings.Contains(rec.Body.String(), `"code":"`+refusalType+`"`) {
			t.Errorf("%s: handled=%v status=%d body=%s", refusalType, true, rec.Code, rec.Body.String())
		}
	}
	if writeOrchRefusal(httptest.NewRecorder(), temporal.NewApplicationError("x", "Y")) {
		t.Fatal("an error that is not a Refusal must be left to the caller")
	}
}
