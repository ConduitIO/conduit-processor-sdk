// Copyright © 2026 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
)

// SchemaEncodeStrategy selects how the schema encode middleware builds a new
// schema when a processor kept a record's schema subject and version but
// returned data that no longer fits that schema (for example, it added a
// field). Data that still fits is always encoded with the schema the record
// carried, whatever the strategy.
type SchemaEncodeStrategy string

const (
	// SchemaEncodeStrategyInfer extracts a schema from the Go values and
	// registers it as a new version of the carried subject. Extraction does
	// not keep Avro logical types, enums, named types or int vs long. This is
	// the default.
	SchemaEncodeStrategyInfer SchemaEncodeStrategy = "infer"
	// SchemaEncodeStrategyExtend builds the new version from the carried
	// schema: fields still present keep their exact carried definition, added
	// top-level fields get an inferred nullable type with a null default, and
	// removed top-level fields are left out. A value that does not fit its
	// carried type fails the record with [ErrSchemaExtendMismatch].
	SchemaEncodeStrategyExtend SchemaEncodeStrategy = "extend"
)

var (
	// ErrSchemaExtendMismatch is returned (wrapped) in an [ErrorRecord] by the
	// schema encode middleware with [SchemaEncodeStrategyExtend] when the data
	// does not fit the extended schema exactly: a value of a different type
	// than its carried field, a null in a non-null field, a value finer than
	// its logical type (a decimal scale, a time precision), a key added to or
	// removed from a nested record, or payload before and after with different
	// top-level fields. The error names the field path, never the value.
	ErrSchemaExtendMismatch = errors.New("value does not fit the carried schema")

	// ErrSchemaExtendUnsupported is returned (wrapped) in an [ErrorRecord] by
	// the schema encode middleware with [SchemaEncodeStrategyExtend] when the
	// carried schema cannot be extended: it is not an Avro record schema, it
	// has schema references, or the extended schema does not parse (an added
	// record whose inferred name collides with a carried named type, or a kept
	// field that refers to a named type defined only in a removed field).
	ErrSchemaExtendUnsupported = errors.New("carried schema cannot be extended")
)

// extendPart is one structured part of a record (the key, payload.before or
// payload.after) that the extended schema has to describe.
type extendPart struct {
	name string // "key", "payload.before" or "payload.after", for errors
	data opencdc.StructuredData
}

// extendResult is the outcome of extendSchema.
type extendResult struct {
	// bytes is the schema text to register, or nil if the carried schema can
	// be used as it is.
	bytes []byte
	// encoded holds the encoded bytes of each part, in the order of the parts
	// passed to extendSchema.
	encoded [][]byte
}

// extendSchema builds the schema described in [SchemaEncodeStrategyExtend]
// from carried for parts, and encodes every part with it. It returns
// ErrSchemaExtendMismatch or ErrSchemaExtendUnsupported (wrapped) if that is
// not possible without changing data. parts must not be empty and must not
// hold empty data; the caller passes empty parts through unencoded.
func extendSchema(carried schema.Schema, parts []extendPart) (extendResult, error) {
	where := fmt.Sprintf("schema %s:%d", carried.Subject, carried.Version)
	top, rawFields, err := parseRecordSchema(carried)
	if err != nil {
		return extendResult{}, fmt.Errorf("%s: %w", where, err)
	}

	// Invariant 6: one schema describes every part, so every part must have
	// the same top-level fields. Writing a null or a default for a field one
	// part lacks would invent data.
	for _, part := range parts[1:] {
		if path, ok := sameKeys(parts[0], part); !ok {
			return extendResult{}, fmt.Errorf("%s: field %q is not in both %s and %s: %w",
				where, path, parts[0].name, part.name, ErrSchemaExtendMismatch)
		}
	}

	fields, changed, err := extendFields(rawFields, parts)
	if err != nil {
		return extendResult{}, fmt.Errorf("%s: %w", where, err)
	}

	sch := carried
	var newBytes []byte
	if changed {
		fieldsJSON, err := json.Marshal(fields)
		if err != nil {
			return extendResult{}, fmt.Errorf("%s: failed to build fields: %w", where, err)
		}
		top["fields"] = fieldsJSON
		newBytes, err = json.Marshal(top)
		if err != nil {
			return extendResult{}, fmt.Errorf("%s: failed to build schema: %w", where, err)
		}
		sch = schema.Schema{Subject: carried.Subject, Type: schema.TypeAvro, Bytes: newBytes}
	}

	if _, err := sch.Serde(); err != nil {
		return extendResult{}, fmt.Errorf("%s: extended schema does not parse: %w: %w", where, ErrSchemaExtendUnsupported, err)
	}

	// Invariant 6: every part must survive an encode and decode with the
	// extended schema unchanged. This rejects a changed type, a null in a
	// non-null field, a decimal or time finer than its logical type, and a key
	// added to or removed from a nested record, instead of coercing them.
	encoded := make([][]byte, len(parts))
	for i, part := range parts {
		b, err := encodeLossless(sch, part.data)
		if err != nil {
			return extendResult{}, fmt.Errorf("%s: %s: %w: %w", where, part.name, ErrSchemaExtendMismatch, err)
		}
		encoded[i] = b
	}
	return extendResult{bytes: newBytes, encoded: encoded}, nil
}

// parseRecordSchema splits an Avro record schema into its top-level JSON
// object and its field definitions, both as raw JSON, so the fields that are
// kept can be copied without re-deriving them.
func parseRecordSchema(sch schema.Schema) (map[string]json.RawMessage, []json.RawMessage, error) {
	if sch.Type != schema.TypeAvro {
		return nil, nil, fmt.Errorf("type %s, only avro can be extended: %w", sch.Type, ErrSchemaExtendUnsupported)
	}
	if len(sch.References) > 0 {
		return nil, nil, fmt.Errorf("schema references are not supported: %w", ErrSchemaExtendUnsupported)
	}
	var top map[string]json.RawMessage
	var typ string
	if err := json.Unmarshal(sch.Bytes, &top); err != nil || json.Unmarshal(top["type"], &typ) != nil || typ != "record" {
		return nil, nil, fmt.Errorf("not a record schema: %w", ErrSchemaExtendUnsupported)
	}
	var rawFields []json.RawMessage
	if err := json.Unmarshal(top["fields"], &rawFields); err != nil {
		return nil, nil, fmt.Errorf("unreadable record fields: %w", ErrSchemaExtendUnsupported)
	}
	return top, rawFields, nil
}

// extendFields returns the field definitions of the extended schema: the
// carried fields the data still has, in carried order, then the added fields
// in name order. changed is false if nothing was added or removed.
func extendFields(rawFields []json.RawMessage, parts []extendPart) (fields []json.RawMessage, changed bool, err error) {
	keys := parts[0].data // all parts have the same keys, checked by the caller
	carriedNames := make(map[string]struct{}, len(rawFields))
	fields = make([]json.RawMessage, 0, len(rawFields)+len(keys))

	// Invariant 6: a field the data still has keeps its carried definition,
	// so no type is re-derived from a decoded Go value. A field the data no
	// longer has was removed by the processor and is left out.
	for _, raw := range rawFields {
		var f struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, false, fmt.Errorf("unreadable field: %w", ErrSchemaExtendUnsupported)
		}
		carriedNames[f.Name] = struct{}{}
		if _, ok := keys[f.Name]; ok {
			fields = append(fields, raw)
		} else {
			changed = true
		}
	}

	var added []string
	for k := range keys {
		if _, ok := carriedNames[k]; !ok {
			added = append(added, k)
		}
	}
	sort.Strings(added) // the same added set always gives the same schema text
	for _, name := range added {
		f, err := addedField(name, addedValue(parts, name))
		if err != nil {
			return nil, false, fmt.Errorf("field %q: %w", name, err)
		}
		fields = append(fields, f)
		changed = true
	}
	return fields, changed, nil
}

// sameKeys reports whether a and b have the same top-level keys, and returns
// the first key that is in only one of them.
func sameKeys(a, b extendPart) (string, bool) {
	for k := range a.data {
		if _, ok := b.data[k]; !ok {
			return k, false
		}
	}
	for k := range b.data {
		if _, ok := a.data[k]; !ok {
			return k, false
		}
	}
	return "", true
}

// addedValue returns the value an added field's type is inferred from: the
// last part's value if it is not nil (payload.after comes last), otherwise
// the first non-nil one, otherwise nil.
func addedValue(parts []extendPart, name string) any {
	for i := len(parts) - 1; i >= 0; i-- {
		if v := parts[i].data[name]; !isNil(v) {
			return v
		}
	}
	return nil
}

// addedField returns the Avro field definition for a top-level field the
// processor added: the type the extractor infers for value, made nullable,
// with a null default.
func addedField(name string, value any) (json.RawMessage, error) {
	srd, err := schema.KnownSerdeFactories[schema.TypeAvro].SerdeForType(opencdc.StructuredData{name: value})
	if err != nil {
		return nil, fmt.Errorf("failed to infer a type: %w: %w", ErrSchemaExtendUnsupported, err)
	}
	var inferred struct {
		Fields []struct {
			Type json.RawMessage `json:"type"`
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(srd.String()), &inferred); err != nil || len(inferred.Fields) != 1 {
		return nil, fmt.Errorf("failed to read the inferred type: %w", ErrSchemaExtendUnsupported)
	}
	typ, err := nullableType(inferred.Fields[0].Type)
	if err != nil {
		return nil, err
	}
	// Invariant 6: the added field is nullable with a null default, so the
	// new version stays backward compatible with the carried one. Its value is
	// still checked by the round trip in extendSchema.
	//nolint:wrapcheck // marshaling a struct of raw JSON and strings cannot fail in a way worth wrapping
	return json.Marshal(struct {
		Name    string          `json:"name"`
		Type    json.RawMessage `json:"type"`
		Default *struct{}       `json:"default"`
	}{Name: name, Type: typ})
}

// nullableType returns typ as a union whose first branch is null, which Avro
// requires for a field with a null default.
func nullableType(typ json.RawMessage) (json.RawMessage, error) {
	null := json.RawMessage(`"null"`)
	var branches []json.RawMessage
	if err := json.Unmarshal(typ, &branches); err != nil {
		// not a union
		branches = []json.RawMessage{typ}
	}
	branches = slices.DeleteFunc(branches, func(b json.RawMessage) bool {
		return bytes.Equal(bytes.TrimSpace(b), null)
	})
	out, err := json.Marshal(append([]json.RawMessage{null}, branches...))
	if err != nil {
		return nil, fmt.Errorf("failed to build a nullable type: %w", ErrSchemaExtendUnsupported)
	}
	return out, nil
}

// isEmptyPart reports whether a key or payload part holds nothing to encode:
// nil, zero-length raw data, or structured data without fields. With
// [SchemaEncodeStrategyExtend] such a part is passed through unencoded.
func isEmptyPart(data opencdc.Data) bool {
	switch d := data.(type) {
	case nil:
		return true
	case opencdc.RawData:
		return len(d) == 0
	case opencdc.StructuredData:
		return len(d) == 0
	default:
		return false
	}
}
