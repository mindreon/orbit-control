package contract

import (
	"bytes"
	"fmt"
	"sync"

	contractv2 "github.com/mindreon/orbit-control/contract/v2"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

var compiledSchemas = sync.OnceValues(func() (map[string]*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	files, err := contractv2.Schemas.ReadDir("schema")
	if err != nil {
		return nil, err
	}
	refs := map[string]string{}
	for _, file := range files {
		raw, err := contractv2.Schemas.ReadFile("schema/" + file.Name())
		if err != nil {
			return nil, err
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		m := doc.(map[string]any)
		id := m["$id"].(string)
		if err := c.AddResource(id, doc); err != nil {
			return nil, err
		}
		defs, _ := m["$defs"].(map[string]any)
		for name := range defs {
			refs[name] = id + "#/$defs/" + name
		}
	}
	result := map[string]*jsonschema.Schema{}
	for name, ref := range refs {
		schema, err := c.Compile(ref)
		if err != nil {
			return nil, err
		}
		result[name] = schema
	}
	return result, nil
})

// Validate checks a wire payload, including formats and conditional constraints.
// Decoding generated types alone is not a substitute for boundary validation.
func Validate(kind string, raw []byte) error {
	schemas, err := compiledSchemas()
	if err != nil {
		return err
	}
	schema, ok := schemas[kind]
	if !ok {
		return fmt.Errorf("unknown contract kind %q", kind)
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	return schema.Validate(value)
}
