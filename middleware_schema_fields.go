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
	"reflect"
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

// parsedAvroSchemas caches a fieldWalker per schema, keyed by the schema's
// full bytes (see [schemaCacheKey]), for checkNoDroppedFields. The number of
// distinct schemas a pipeline sees is bounded by what is registered, so the
// cache is not evicted.
var parsedAvroSchemas sync.Map // map[string]*fieldWalker

// fieldWalker is a parsed Avro schema plus the field names of every record
// schema in it, built once per schema so the walk does not rebuild them for
// each record.
type fieldWalker struct {
	root   avro.Schema
	fields map[*avro.RecordSchema]map[string]avro.Schema
}

func newFieldWalker(root avro.Schema) *fieldWalker {
	w := &fieldWalker{root: root, fields: make(map[*avro.RecordSchema]map[string]avro.Schema)}
	w.index(root)
	return w
}

// index records the field names of every record schema reachable from s.
func (w *fieldWalker) index(s avro.Schema) {
	switch s := s.(type) {
	case *avro.RefSchema:
		w.index(s.Schema())
	case *avro.RecordSchema:
		if _, seen := w.fields[s]; seen {
			return // recursive types
		}
		fields := make(map[string]avro.Schema, len(s.Fields()))
		w.fields[s] = fields
		for _, f := range s.Fields() {
			fields[f.Name()] = f.Type()
			w.index(f.Type())
		}
	case *avro.MapSchema:
		w.index(s.Values())
	case *avro.ArraySchema:
		w.index(s.Items())
	case *avro.UnionSchema:
		for _, t := range s.Types() {
			w.index(t)
		}
	}
}

// checkNoDroppedFields returns an error wrapping ErrFieldNotInSchema if data
// holds a record field that sch does not define, at any depth. Only Avro
// schemas are checked; other schema types return nil.
//
// Invariant 6: the Avro encoder writes the fields a record schema defines and
// ignores any other map key, so without this check an extra field disappears
// without an error. That includes a key whose value is nil.
func checkNoDroppedFields(sch schema.Schema, data any) error {
	if sch.Type != schema.TypeAvro {
		return nil
	}
	key := schemaCacheKey(sch)
	cached, ok := parsedAvroSchemas.Load(key)
	if !ok {
		// A private cache keeps named types of unrelated schemas apart.
		s, err := avro.ParseBytesWithCache(sch.Bytes, "", &avro.SchemaCache{}) //nolint:forbidigo // fresh cache owned by this one schema; parsed only to walk field names, never to encode
		if err != nil {
			return fmt.Errorf("failed to parse schema %s:%d: %w", sch.Subject, sch.Version, err)
		}
		cached, _ = parsedAvroSchemas.LoadOrStore(key, newFieldWalker(s))
	}
	w := cached.(*fieldWalker) //nolint:forcetypeassert // only *fieldWalker is stored
	if path, dropped := w.find(w.root, reflect.ValueOf(data), ""); dropped {
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

// find walks v along s and returns the path of the first map key that is not
// a field of the record schema it is encoded with. Any slice or array and any
// map with string keys is walked, because the encoder accepts typed Go
// containers ([]map[string]any, map[string]opencdc.StructuredData, ...) as
// well as []any and map[string]any. It only reports a field when it is
// certain the encoder would drop it: where the branch of a union cannot be
// determined, it does not descend.
func (w *fieldWalker) find(s avro.Schema, v reflect.Value, path string) (string, bool) {
	v = unwrapValue(v)
	if !v.IsValid() {
		return "", false
	}
	switch s := s.(type) {
	case *avro.RefSchema:
		return w.find(s.Schema(), v, path)
	case *avro.RecordSchema:
		return w.findInRecord(s, v, path)
	case *avro.MapSchema:
		if !isStringMap(v) {
			return "", false
		}
		iter := v.MapRange()
		for iter.Next() {
			if p, dropped := w.find(s.Values(), iter.Value(), joinFieldPath(path, iter.Key().String())); dropped {
				return p, true
			}
		}
	case *avro.ArraySchema:
		if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
			return "", false
		}
		for i := range v.Len() {
			if p, dropped := w.find(s.Items(), v.Index(i), fmt.Sprintf("%s[%d]", path, i)); dropped {
				return p, true
			}
		}
	case *avro.UnionSchema:
		return w.findInUnion(s, v, path)
	}
	return "", false
}

// findInRecord reports the first key of v that is not a field of s, then
// descends into the fields that are.
func (w *fieldWalker) findInRecord(s *avro.RecordSchema, v reflect.Value, path string) (string, bool) {
	if !isStringMap(v) {
		return "", false // not a map, the encoder fails or uses a struct
	}
	fields := w.fields[s]
	iter := v.MapRange()
	for iter.Next() {
		k := iter.Key().String()
		ft, ok := fields[k]
		if !ok {
			return joinFieldPath(path, k), true
		}
		if p, dropped := w.find(ft, iter.Value(), joinFieldPath(path, k)); dropped {
			return p, true
		}
	}
	return "", false
}

// findInUnion picks the union branch v is encoded with: the named form
// {"branch": value} if v has it, otherwise the only non-null branch. With
// several non-null branches and a bare value, the branch is chosen by the
// codec at encode time, so nothing is reported. A single-key map whose key is
// the short name of a named branch is the named form written without its
// namespace, which the codec rejects with its own error; that is left to the
// codec instead of being reported as a dropped field.
func (w *fieldWalker) findInUnion(s *avro.UnionSchema, v reflect.Value, path string) (string, bool) {
	if isStringMap(v) && v.Len() == 1 {
		iter := v.MapRange()
		iter.Next()
		name := iter.Key().String()
		if branch, pos := s.Types().Get(name); pos >= 0 {
			return w.find(branch, iter.Value(), path)
		}
		for _, t := range s.Types() {
			if n, ok := t.(avro.NamedSchema); ok && n.Name() == name {
				return "", false
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
	return w.find(only, v, path)
}

// unwrapValue strips interfaces and pointers, returning the zero Value for
// nil.
func unwrapValue(v reflect.Value) reflect.Value {
	for v.IsValid() && (v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer) {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	return v
}

func isStringMap(v reflect.Value) bool {
	return v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String
}

func joinFieldPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// schemaCacheKey returns the key under which per-schema derived state is
// cached. It is the full schema text, so two different schemas can never share
// an entry.
//
// Do not use [schema.Schema.Fingerprint] here: it is a 64-bit Rabin CRC, which
// is affine, so a pair of schemas that differ only in a doc string and share a
// fingerprint is cheap to construct. A cache hit would then apply one schema's
// field set to another and the Avro encoder would drop a field without an error
// (invariant 6). The subject and version are left out on purpose, because the
// parsed state depends on the bytes alone.
func schemaCacheKey(sch schema.Schema) string {
	return string(sch.Bytes)
}
