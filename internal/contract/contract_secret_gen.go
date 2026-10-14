// Code generated from contract/v2/schema. DO NOT EDIT.

package contract

import (
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"
)

// Secret preserves JSON wire values while redacting formatted and structured logs.
type Secret string

func (Secret) String() string       { return "[REDACTED]" }
func (Secret) GoString() string     { return "[REDACTED]" }
func (Secret) LogValue() slog.Value { return slog.StringValue("[REDACTED]") }

// contractLogValue redacts nested Secret fields before slog's JSON handler
// marshals a struct; encoding/json itself intentionally preserves wire secrets.
func contractLogValue(value reflect.Value) any {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		return contractLogValue(value.Elem())
	}
	if value.Type() == reflect.TypeFor[Secret]() {
		return "[REDACTED]"
	}
	if value.Type() == reflect.TypeFor[time.Time]() {
		return value.Interface()
	}
	switch value.Kind() {
	case reflect.Struct:
		result := map[string]any{}
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			result[name] = contractLogValue(value.Field(i))
		}
		return result
	case reflect.Slice, reflect.Array:
		result := make([]any, value.Len())
		for i := range value.Len() {
			result[i] = contractLogValue(value.Index(i))
		}
		return result
	case reflect.Map:
		result := map[string]any{}
		iter := value.MapRange()
		for iter.Next() {
			result[fmt.Sprint(iter.Key().Interface())] = contractLogValue(iter.Value())
		}
		return result
	default:
		return value.Interface()
	}
}
func (value CheckpointKeyResponse) LogValue() slog.Value {
	return slog.AnyValue(contractLogValue(reflect.ValueOf(value)))
}
func (value GetLockResponse) LogValue() slog.Value {
	return slog.AnyValue(contractLogValue(reflect.ValueOf(value)))
}
func (value LockFileContent) LogValue() slog.Value {
	return slog.AnyValue(contractLogValue(reflect.ValueOf(value)))
}
func (value ModelCredentials) LogValue() slog.Value {
	return slog.AnyValue(contractLogValue(reflect.ValueOf(value)))
}
func (value SecretGrant) LogValue() slog.Value {
	return slog.AnyValue(contractLogValue(reflect.ValueOf(value)))
}
func (value SecretGrantResponse) LogValue() slog.Value {
	return slog.AnyValue(contractLogValue(reflect.ValueOf(value)))
}
func (value TokenResponse) LogValue() slog.Value {
	return slog.AnyValue(contractLogValue(reflect.ValueOf(value)))
}
