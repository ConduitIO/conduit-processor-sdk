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
	"errors"
	"fmt"
	"sync"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/iskorotkov/avro/v2"
)

// ErrFieldNotInSchema is returned (wrapped) in an [ErrorRecord] by the schema
// encode middleware when a processor attached a schema subject and version that
// differ from the incoming record's, and the key or payload holds a field that
// schema does not have. The Avro encoder would silently drop such a field, so
// the record fails instead. Fix it by attaching a schema that has the field, or
// by removing the field from the record in the processor.
var ErrFieldNotInSchema = errors.New("field is not in the attached schema and would be dropped by the encoder")

// parsedAvroSchemas caches parsed Avro schemas by fingerprint for
// checkNoDroppedFields. The number of distinct schemas a pipeline sees is
// bounded by what is registered, so the cache is not evicted.
var parsedAvroSchemas sync.Map // map[uint64]avro.Schema

// checkNoDroppedFields returns an error wrapping ErrFieldNotInSchema if data
// holds a record field that sch does not define, at any depth. Only Avro
// schemas are checked; other schema types return nil.
//
// Invariant 6: the Avro encoder writes the fields a record schema defines and
// ignores any other map key, so without this check an extra field disappears
// without an error.
func checkNoDroppedFields(sch schema.Schema, data any) error {
	if sch.Type != schema.TypeAvro {
		return nil
	}
	fp := sch.Fingerprint()
	parsed, ok := parsedAvroSchemas.Load(fp)
	if !ok {
		// A private cache keeps named types of unrelated schemas apart.
		s, err := avro.ParseBytesWithCache(sch.Bytes, "", &avro.SchemaCache{})
		if err != nil {
			return fmt.Errorf("failed to parse schema %s:%d: %w", sch.Subject, sch.Version, err)
		}
		parsed, _ = parsedAvroSchemas.LoadOrStore(fp, s)
	}
	if path, dropped := findDroppedField(parsed.(avro.Schema), data, ""); dropped { //nolint:forcetypeassert // only avro.Schema is stored
		return fmt.Errorf("field %q, schema %s:%d: %w", path, sch.Subject, sch.Version, ErrFieldNotInSchema)
	}
	return nil
}

// checkPayloadNoDroppedFields runs checkNoDroppedFields on the structured
// parts of payload.
func checkPayloadNoDroppedFields(sch schema.Schema, payload opencdc.Change) error {
	if before, ok := payload.Before.(opencdc.StructuredData); ok {
		if err := checkNoDroppedFields(sch, before); err != nil {
			return fmt.Errorf("failed to encode before payload: %w", err)
		}
	}
	if after, ok := payload.After.(opencdc.StructuredData); ok {
		if err := checkNoDroppedFields(sch, after); err != nil {
			return fmt.Errorf("failed to encode after payload: %w", err)
		}
	}
	return nil
}

// findDroppedField walks data along s and returns the path of the first map
// key that is not a field of the record schema it is encoded with. It only
// reports a field when it is certain the encoder would drop it: where the
// branch of a union cannot be determined, it does not descend.
func findDroppedField(s avro.Schema, data any, path string) (string, bool) {
	if data == nil {
		return "", false
	}
	switch s := s.(type) {
	case *avro.RefSchema:
		return findDroppedField(s.Schema(), data, path)
	case *avro.RecordSchema:
		return findDroppedFieldInRecord(s, data, path)
	case *avro.MapSchema:
		m, ok := asFieldMap(data)
		if !ok {
			return "", false
		}
		for k, v := range m {
			if p, dropped := findDroppedField(s.Values(), v, joinFieldPath(path, k)); dropped {
				return p, true
			}
		}
	case *avro.ArraySchema:
		items, ok := data.([]any)
		if !ok {
			return "", false
		}
		for i, v := range items {
			if p, dropped := findDroppedField(s.Items(), v, fmt.Sprintf("%s[%d]", path, i)); dropped {
				return p, true
			}
		}
	case *avro.UnionSchema:
		return findDroppedFieldInUnion(s, data, path)
	}
	return "", false
}

// findDroppedFieldInRecord reports the first key of data that is not a field
// of s, then descends into the fields that are.
func findDroppedFieldInRecord(s *avro.RecordSchema, data any, path string) (string, bool) {
	m, ok := asFieldMap(data)
	if !ok {
		return "", false // not a map, the encoder fails or uses a struct
	}
	fields := make(map[string]avro.Schema, len(s.Fields()))
	for _, f := range s.Fields() {
		fields[f.Name()] = f.Type()
	}
	for k, v := range m {
		ft, ok := fields[k]
		if !ok {
			return joinFieldPath(path, k), true
		}
		if p, dropped := findDroppedField(ft, v, joinFieldPath(path, k)); dropped {
			return p, true
		}
	}
	return "", false
}

// findDroppedFieldInUnion picks the union branch data is encoded with: the
// named form {"branch": value} if data has it, otherwise the only non-null
// branch. With several non-null branches and a bare value, the branch is
// chosen by the codec at encode time, so nothing is reported.
func findDroppedFieldInUnion(s *avro.UnionSchema, data any, path string) (string, bool) {
	if m, ok := asFieldMap(data); ok && len(m) == 1 {
		for name, v := range m {
			if branch, pos := s.Types().Get(name); pos >= 0 {
				return findDroppedField(branch, v, path)
			}
		}
	}
	var only avro.Schema
	for _, t := range s.Types() {
		if t.Type() == avro.Null {
			continue
		}
		if only != nil {
			return "", false
		}
		only = t
	}
	if only == nil {
		return "", false
	}
	return findDroppedField(only, data, path)
}

func asFieldMap(v any) (map[string]any, bool) {
	switch v := v.(type) {
	case map[string]any:
		return v, true
	case opencdc.StructuredData:
		return v, true
	default:
		return nil, false
	}
}

func joinFieldPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
