package contract

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	contractv2 "github.com/mindreon/orbit-control/contract/v2"
)

var consumerSchemas = sync.OnceValues(func() (map[string]*jsonschema.Schema, error) { return compileSchemas(false) })
var strictSchemas = sync.OnceValues(func() (map[string]*jsonschema.Schema, error) { return compileSchemas(true) })

// relaxConsumers changes only objects explicitly marked x-extra=ignore.
func relaxConsumers(value any) {
	switch v := value.(type) {
	case map[string]any:
		if v["x-extra"] == "ignore" && v["additionalProperties"] == false {
			delete(v, "additionalProperties")
		}
		for _, child := range v {
			relaxConsumers(child)
		}
	case []any:
		for _, child := range v {
			relaxConsumers(child)
		}
	}
}

func compileSchemas(strict bool) (map[string]*jsonschema.Schema, error) {
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
		if !strict {
			relaxConsumers(doc)
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
}

// Validate checks consumer payloads, ignoring future fields on x-extra=ignore objects.
// Formats, conditional constraints and authored manifests remain enforced.
func Validate(kind string, raw []byte) error { return validateWire(kind, raw, false) }

// ValidateStrict checks producer output against the unmodified schemas.
func ValidateStrict(kind string, raw []byte) error { return validateWire(kind, raw, true) }

func validateWire(kind string, raw []byte, strict bool) error {
	loader := consumerSchemas
	if strict {
		loader = strictSchemas
	}
	schemas, err := loader()
	if err != nil {
		return fmt.Errorf("/: schema")
	}
	schema, ok := schemas[kind]
	if !ok {
		return fmt.Errorf("/: kind")
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("/: json")
	}
	err = schema.Validate(value)
	if err == nil {
		return nil
	}
	var failure *jsonschema.ValidationError
	if !errors.As(err, &failure) {
		return fmt.Errorf("/: schema")
	}
	// Do not retain the validator's error: it can contain plaintext instance values.
	var rules []string
	var collect func(*jsonschema.ValidationError)
	collect = func(e *jsonschema.ValidationError) {
		if len(e.Causes) > 0 {
			for _, cause := range e.Causes {
				collect(cause)
			}
			return
		}
		path := ""
		for _, part := range e.InstanceLocation {
			path += "/" + strings.ReplaceAll(strings.ReplaceAll(part, "~", "~0"), "/", "~1")
		}
		if path == "" {
			path = "/"
		}
		keywords := e.ErrorKind.KeywordPath()
		rule := "schema"
		if len(keywords) > 0 {
			rule = keywords[0]
		}
		rules = append(rules, path+": "+rule)
	}
	collect(failure)
	return errors.New(strings.Join(rules, "; "))
}
