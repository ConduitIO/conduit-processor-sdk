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
	"math"
	"math/big"
	"reflect"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
)

// errSchemaMismatch is returned by encodeLossless when a value cannot be
// encoded with a schema without changing it.
var errSchemaMismatch = errors.New("value does not fit schema")

// encodeLossless encodes data with sch and returns the bytes only if decoding
// them with the same schema gives back a value equivalent to data (see
// equivalentValues). Otherwise it returns an error wrapping errSchemaMismatch,
// or the codec error if data cannot be encoded at all.
//
// This is how the encode middleware decides whether a processor's output still
// fits the schema the record came in with. Checking the round trip instead of
// trusting a successful encode matters, because an Avro encoder accepts some
// values it cannot represent: it ignores map keys that are not fields of the
// record, and it truncates a time to the precision of the logical type. Both
// would drop data without an error (invariant 6).
//
// data is not modified. The codec may rewrite union values in place while it
// encodes, so the encoder gets a deep copy.
func encodeLossless(sch schema.Schema, data opencdc.StructuredData) ([]byte, error) {
	// Cheap pre-check: a top-level key the record schema does not define, or a
	// field missing from data, cannot round-trip. This skips the encode and
	// decode for processors that add a field, which is the common mismatch.
	if fields, ok := topLevelFields(sch); ok {
		if len(fields) != len(data) {
			return nil, fmt.Errorf("%w: has %d top-level fields, schema has %d", errSchemaMismatch, len(data), len(fields))
		}
		for k := range data {
			if _, ok := fields[k]; !ok {
				return nil, fmt.Errorf("%w: field %q is not in the schema", errSchemaMismatch, k)
			}
		}
	}

	srd, err := sch.Serde()
	if err != nil {
		return nil, fmt.Errorf("failed to get serde for schema: %w", err)
	}

	encoded, err := srd.Marshal(deepCopyValue(map[string]any(data)))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errSchemaMismatch, err)
	}

	var decoded opencdc.StructuredData
	if err := srd.Unmarshal(encoded, &decoded); err != nil {
		return nil, fmt.Errorf("%w: encoded value does not decode: %w", errSchemaMismatch, err)
	}
	if path, ok := equivalentValues(map[string]any(data), map[string]any(decoded), ""); !ok {
		return nil, fmt.Errorf("%w: field %q would not survive encoding", errSchemaMismatch, path)
	}
	return encoded, nil
}

// recordFields caches topLevelFields results, keyed by the schema's full bytes
// (see [schemaCacheKey]). A nil value means the schema is not an Avro record.
// The number of distinct schemas is bounded by what is registered, so the
// cache is not evicted.
var recordFields sync.Map // map[string]map[string]struct{}

// topLevelFields returns the field names of sch if it is an Avro record
// schema. ok is false for anything else, and the pre-check is then skipped.
func topLevelFields(sch schema.Schema) (map[string]struct{}, bool) {
	if sch.Type != schema.TypeAvro {
		return nil, false
	}
	key := schemaCacheKey(sch)
	if v, ok := recordFields.Load(key); ok {
		fields, _ := v.(map[string]struct{})
		return fields, fields != nil
	}
	var rec struct {
		Type   any `json:"type"`
		Fields []struct {
			Name string `json:"name"`
		} `json:"fields"`
	}
	var fields map[string]struct{}
	if err := json.Unmarshal(sch.Bytes, &rec); err == nil && rec.Type == "record" {
		fields = make(map[string]struct{}, len(rec.Fields))
		for _, f := range rec.Fields {
			fields[f.Name] = struct{}{}
		}
	}
	recordFields.Store(key, fields)
	return fields, fields != nil
}

// deepCopyValue copies the maps and slices of a decoded record value, so that
// the codec can rewrite them in place without touching the original. Leaf
// values are immutable or replaced as a whole by the codec, so they are shared.
func deepCopyValue(v any) any {
	switch v := v.(type) {
	case opencdc.StructuredData:
		return deepCopyValue(map[string]any(v))
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, val := range v {
			out[k] = deepCopyValue(val)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, val := range v {
			out[i] = deepCopyValue(val)
		}
		return out
	default:
		return v
	}
}

// equivalentValues reports whether got, the value decoded after a round trip
// through a schema, carries the same data as want, the value the processor
// returned. It returns the path of the first difference.
//
// The comparison is by value, not by Go type: the decoder picks its own Go
// types (int32 for an Avro int, for example), and a processor that writes an
// int into an int field has not changed the data. Everything that is data must
// match exactly: map keys, numbers, string and byte contents, time instants,
// durations and decimals.
func equivalentValues(want, got any, path string) (string, bool) {
	if want == nil || got == nil {
		return path, isNil(want) && isNil(got)
	}

	switch w := want.(type) {
	case opencdc.StructuredData:
		return equivalentValues(map[string]any(w), got, path)
	case map[string]any:
		return equivalentMaps(w, got, path)
	case time.Time:
		g, ok := got.(time.Time)
		return path, ok && w.Equal(g)
	case time.Duration:
		g, ok := got.(time.Duration)
		return path, ok && w == g
	case *big.Rat:
		g, ok := got.(*big.Rat)
		return path, ok && w != nil && g != nil && w.Cmp(g) == 0
	case big.Rat:
		g, ok := got.(*big.Rat)
		return path, ok && g != nil && w.Cmp(g) == 0
	case string:
		g, ok := got.(string)
		return path, ok && w == g
	case bool:
		g, ok := got.(bool)
		return path, ok && w == g
	}

	wv, gv := reflect.ValueOf(want), reflect.ValueOf(got)
	if wb, ok := byteContents(wv); ok {
		gb, ok := byteContents(gv)
		return path, ok && bytes.Equal(wb, gb)
	}
	if isNumber(wv.Kind()) {
		return path, isNumber(gv.Kind()) && sameNumber(wv, gv)
	}
	if wv.Kind() == reflect.Slice || wv.Kind() == reflect.Array {
		return equivalentLists(wv, gv, path)
	}
	return path, reflect.DeepEqual(want, got)
}

// equivalentMaps compares a record or map value. Both must have exactly the
// same keys: a key the processor added would be dropped by the encoder, and a
// key it removed would come back as null or as the field default.
func equivalentMaps(want map[string]any, got any, path string) (string, bool) {
	g, ok := asStringMap(got)
	if !ok || len(g) != len(want) {
		return path, false
	}
	for k, wv := range want {
		gv, ok := g[k]
		if !ok {
			return joinPath(path, k), false
		}
		if p, ok := equivalentValues(wv, gv, joinPath(path, k)); !ok {
			return p, false
		}
	}
	return "", true
}

// equivalentLists compares two slices or arrays element by element.
func equivalentLists(want, got reflect.Value, path string) (string, bool) {
	if got.Kind() != reflect.Slice && got.Kind() != reflect.Array {
		return path, false
	}
	if want.Len() != got.Len() {
		return path, false
	}
	for i := range want.Len() {
		p := fmt.Sprintf("%s[%d]", path, i)
		if p, ok := equivalentValues(want.Index(i).Interface(), got.Index(i).Interface(), p); !ok {
			return p, false
		}
	}
	return "", true
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() { //nolint:exhaustive // only nillable kinds matter
	case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Interface:
		return rv.IsNil()
	default:
		return false
	}
}

func asStringMap(v any) (map[string]any, bool) {
	switch v := v.(type) {
	case map[string]any:
		return v, true
	case opencdc.StructuredData:
		return v, true
	default:
		return nil, false
	}
}

// byteContents returns the contents of a []byte or [N]byte value.
func byteContents(v reflect.Value) ([]byte, bool) {
	if (v.Kind() != reflect.Slice && v.Kind() != reflect.Array) || v.Type().Elem().Kind() != reflect.Uint8 {
		return nil, false
	}
	if v.Kind() == reflect.Slice {
		return v.Bytes(), true
	}
	out := make([]byte, v.Len())
	reflect.Copy(reflect.ValueOf(out), v)
	return out, true
}

func isNumber(k reflect.Kind) bool {
	switch k { //nolint:exhaustive // only numeric kinds matter
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// sameNumber compares two numeric values exactly. Integers are compared as
// integers (no float64 rounding above 2^53), floats as float64, which is exact
// for float32 too. NaN equals NaN here, since a NaN that round-trips has not
// been changed.
func sameNumber(a, b reflect.Value) bool {
	ar, aok := exactRat(a)
	br, bok := exactRat(b)
	if aok && bok {
		return ar.Cmp(br) == 0
	}
	// at least one is NaN or infinite
	af, bf := asFloat(a), asFloat(b)
	if math.IsNaN(af) || math.IsNaN(bf) {
		return math.IsNaN(af) && math.IsNaN(bf)
	}
	return af == bf
}

func exactRat(v reflect.Value) (*big.Rat, bool) {
	switch v.Kind() { //nolint:exhaustive // only numeric kinds reach here
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return new(big.Rat).SetInt64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(v.Uint())), true
	default:
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, false
		}
		return new(big.Rat).SetFloat64(f), true
	}
}

func asFloat(v reflect.Value) float64 {
	switch v.Kind() { //nolint:exhaustive // only numeric kinds reach here
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return float64(v.Uint())
	default:
		return v.Float()
	}
}
