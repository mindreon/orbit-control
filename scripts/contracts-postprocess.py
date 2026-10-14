#!/usr/bin/env python3
"""Apply schema extensions and generate shared names; run only via gen-contracts.sh."""

import json
from pathlib import Path
import re
import sys

root = Path(__file__).resolve().parents[1]
out = Path(sys.argv[1])
docs = [
    json.loads(p.read_text())
    for p in sorted((root / "contract/v2/schema").glob("*.json"))
]
source = out.read_text()
for doc in docs:
    for name, definition in doc.get("$defs", {}).items():
        if (
            definition.get("type") == "object"
            and definition.get("properties") == {}
            and definition.get("additionalProperties") is False
        ):
            source = re.sub(
                rf"type {name} map\[string\]interface\{{\}}",
                f"type {name} struct {{}}",
                source,
            )
        for field, schema in definition.get("properties", {}).items():
            if not isinstance(schema, dict):
                continue
            target = (
                "Secret"
                if schema.get("x-sensitive")
                else "int64"
                if schema.get("maximum") == 2**53 - 1
                else None
            )
            if target:
                # Match by struct name and JSON field, independent of the generator's initialisms.
                pattern = rf"(type {name} struct \{{.*?\n\}})"

                def change(match):
                    return re.sub(
                        rf'(\w+\s+)(\*?)(?:string|int)( `json:"{field}(?:,|"))',
                        rf"\g<1>\g<2>{target}\g<3>",
                        match[0],
                    )

                source = re.sub(pattern, change, source, flags=re.S)
                if target == "Secret":
                    # String constraints still apply to the wire value, not its redacted representation.
                    block = re.search(
                        rf"type {name} struct \{{(.*?)\n\}}", source, re.S
                    )[1]
                    gofield = re.search(rf'(\w+)\s+\*?Secret `json:"{field}', block)[1]
                    source = re.sub(
                        rf"utf8.RuneCountInString\((plain\.{gofield})\)",
                        r"utf8.RuneCountInString(string(\1))",
                        source,
                    )
                    source = re.sub(
                        rf"(regexp.MatchString\([^,\n]+, )([*]?plain\.{gofield})(\))",
                        r"\1string(\2)\3",
                        source,
                    )
out.write_text(source)
header = (
    "// Code generated from contract/v2/schema. DO NOT EDIT.\n\npackage contract\n\n"
)
secret = """// Secret preserves JSON wire values while redacting formatted and structured logs.
type Secret string

func (Secret) String() string { return "[REDACTED]" }
func (Secret) GoString() string { return "[REDACTED]" }
func (Secret) LogValue() slog.Value { return slog.StringValue("[REDACTED]") }
"""
logging = """
// contractLogValue redacts nested Secret fields before slog's JSON handler
// marshals a struct; encoding/json itself intentionally preserves wire secrets.
func contractLogValue(value reflect.Value) any {
 if !value.IsValid() { return nil }
 if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
  if value.IsNil() { return nil }; return contractLogValue(value.Elem())
 }
 if value.Type() == reflect.TypeFor[Secret]() { return "[REDACTED]" }
 if value.Type() == reflect.TypeFor[time.Time]() { return value.Interface() }
 switch value.Kind() {
 case reflect.Struct:
  result := map[string]any{}
  for i := 0; i < value.NumField(); i++ {
   field := value.Type().Field(i)
   if field.PkgPath != "" { continue }
   name := strings.Split(field.Tag.Get("json"), ",")[0]
   if name == "-" { continue }; if name == "" { name = field.Name }
   result[name] = contractLogValue(value.Field(i))
  }; return result
 case reflect.Slice, reflect.Array:
  result := make([]any, value.Len())
  for i := range value.Len() { result[i] = contractLogValue(value.Index(i)) }; return result
 case reflect.Map:
  result := map[string]any{}
  iter := value.MapRange(); for iter.Next() { result[fmt.Sprint(iter.Key().Interface())] = contractLogValue(iter.Value()) }; return result
 default: return value.Interface()
 }
}
"""
definitions = {
    name: definition
    for doc in docs
    for name, definition in doc.get("$defs", {}).items()
}
sensitive_types = {
    name
    for name, definition in definitions.items()
    if '"x-sensitive": true' in json.dumps(definition)
}
references = {
    name: set(re.findall(r"#/\$defs/(\w+)", json.dumps(definition)))
    for name, definition in definitions.items()
}
while True:
    expanded = sensitive_types | {
        name for name, refs in references.items() if refs & sensitive_types
    }
    if expanded == sensitive_types:
        break
    sensitive_types = expanded
methods = "\n".join(
    f"func (value {name}) LogValue() slog.Value {{ return slog.AnyValue(contractLogValue(reflect.ValueOf(value))) }}"
    for name in sorted(sensitive_types)
)
(out.parent / "contract_secret_gen.go").write_text(
    header
    + 'import ("fmt"; "log/slog"; "reflect"; "strings"; "time")\n\n'
    + secret
    + logging
    + methods
    + "\n"
)
reg = next(d for d in docs if d["$id"].endswith("/registry.json"))


def camel(name):
    return "".join(w[0].upper() + w[1:] for w in re.split(r"[._-]", name))


constants = []
for kind, prefix in [
    ("task-queues", "TaskQueue"),
    ("activities", "Activity"),
    ("updates", "Update"),
    ("signals", "Signal"),
    ("queries", "Query"),
    ("errors", "Error"),
]:
    for name in reg["x-" + kind]:
        constants.append(f"const {prefix}{camel(name)} = {json.dumps(name)}")
constants.append(
    "var NonRetryableErrors = []string{"
    + ",".join(
        "Error" + camel(n)
        for n, spec in reg["x-errors"].items()
        if not spec["retryable"]
    )
    + "}"
)
(out.parent / "contract_registry_gen.go").write_text(
    header + "\n".join(constants) + "\n"
)
exports = sorted(e for d in docs for e in d["x-exports"])
(out.parent / "contract_types_gen_test.go").write_text(
    header
    + "var constructors = map[string]func() any{\n"
    + "".join(f'"{n}": func() any {{ return new({n}) }},\n' for n in exports)
    + "}\n"
)
