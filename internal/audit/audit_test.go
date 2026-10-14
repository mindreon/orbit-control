package audit

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestCanonicalHashAndTampering(t *testing.T) {
	r := Record{ID: "entry", TenantID: "tenant", ActorID: "actor", Action: "role.granted", Detail: json.RawMessage(`{"b":2,"a":{"z":1,"c":2}}`), OccurredAt: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)}
	prev := make([]byte, 32)
	h, e := Hash(prev, r)
	if e != nil {
		t.Fatal(e)
	}
	r.Detail = json.RawMessage(`{ "a":{"c":2,"z":1},"b":2 }`)
	same, e := Hash(prev, r)
	if e != nil || !bytes.Equal(h, same) {
		t.Fatal("JSON order affects canonical hash")
	}
	r.Action = "tampered"
	changed, e := Hash(prev, r)
	if e != nil || bytes.Equal(h, changed) {
		t.Fatal("mutation not detected")
	}
	r.Action = "role.granted"
	prev[0] = 1
	changed, e = Hash(prev, r)
	if e != nil || bytes.Equal(h, changed) {
		t.Fatal("previous hash not included")
	}
}

func TestHashPreservesLargeIntegerTampering(t *testing.T) {
	r := Record{Detail: json.RawMessage(`{"amount":9007199254740992}`)}
	h, e := Hash(nil, r)
	if e != nil {
		t.Fatal(e)
	}
	r.Detail = json.RawMessage(`{"amount":9007199254740993}`)
	changed, e := Hash(nil, r)
	if e != nil || bytes.Equal(h, changed) {
		t.Fatal("integer precision lost during canonicalization")
	}
}
