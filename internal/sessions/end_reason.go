package sessions

import (
	"encoding/json/v2"
	"slices"
	"sync"

	contractv2 "github.com/mindreon/orbit-control/contract/v2"
	"github.com/mindreon/orbit-control/internal/contract"
)

func EndReason(result contract.SegmentResult) *string {
	switch result.Status {
	case "completed":
		return new("completed")
	case "yielded":
		if result.YieldReason != nil {
			return new(string(*result.YieldReason))
		}
	case "interrupted":
		if result.Interaction != nil {
			return new(string(result.Interaction.Kind))
		}
	case "failed":
		if result.Error != nil {
			return new(string(result.Error.Code))
		}
	}
	return nil
}

// Read the authored contract enums, not a second manually maintained list.
var endReasons = sync.OnceValue(func() []string {
	values := []string{"completed", "run_terminated"}
	files, err := contractv2.Schemas.ReadDir("schema")
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		raw, err := contractv2.Schemas.ReadFile("schema/" + file.Name())
		if err != nil {
			panic(err)
		}
		var doc struct {
			Defs map[string]struct {
				Enum       []string `json:"enum"`
				Properties map[string]struct {
					Enum []string `json:"enum"`
				} `json:"properties"`
			} `json:"$defs"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			panic(err)
		}
		for _, name := range []string{"YieldReason", "InteractionKind"} {
			values = append(values, doc.Defs[name].Enum...)
		}
		values = append(values, doc.Defs["SegmentError"].Properties["code"].Enum...)
	}
	slices.Sort(values)
	return slices.Compact(values)
})

func EndReasons() []string { return slices.Clone(endReasons()) }
