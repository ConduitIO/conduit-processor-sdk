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
	"context"
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	sdkschema "github.com/conduitio/conduit-processor-sdk/schema"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// schemaFidelityCase is one Avro schema plus a value that conforms to it. The
// record payload is that value with a leading "id" field, so every case is a
// record schema with exactly one field of the type under test.
type schemaFidelityCase struct {
	name  string
	field string // Avro JSON of the type of field "v"
	value any    // Go value of field "v", as accepted by the encoder
}

// schemaFidelityCases covers the Avro types whose identity does not survive
// schema inference from decoded Go values. Each one was observed to come
// back as a different type (or a different schema) before the fix.
func schemaFidelityCases() []schemaFidelityCase {
	ts := time.Date(2026, 10, 14, 13, 14, 15, 123456000, time.UTC)
	return []schemaFidelityCase{
		{"int", `"int"`, 42},
		{"int max", `"int"`, 2147483647},
		{"long", `"long"`, int64(1) << 40},
		{"float", `"float"`, float32(1.5)},
		{"double", `"double"`, 2.25},
		{"date", `{"type":"int","logicalType":"date"}`, time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)},
		{"time-millis", `{"type":"int","logicalType":"time-millis"}`, 13*time.Hour + 14*time.Minute + 15*time.Second + 123*time.Millisecond},
		{"time-micros", `{"type":"long","logicalType":"time-micros"}`, 13*time.Hour + 14*time.Minute + 15*time.Second + 123456*time.Microsecond},
		{"timestamp-millis", `{"type":"long","logicalType":"timestamp-millis"}`, ts.Truncate(time.Millisecond)},
		{"timestamp-micros", `{"type":"long","logicalType":"timestamp-micros"}`, ts},
		{"local-timestamp-millis", `{"type":"long","logicalType":"local-timestamp-millis"}`, ts.Truncate(time.Millisecond)},
		{"local-timestamp-micros", `{"type":"long","logicalType":"local-timestamp-micros"}`, ts},
		{"enum", `{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}`, "GREEN"},
		{"fixed", `{"type":"fixed","name":"Hash","size":4}`, [4]byte{1, 2, 3, 4}},
		{"uuid", `{"type":"string","logicalType":"uuid"}`, "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"decimal bytes", `{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}`, big.NewRat(12345, 100)},
		{"decimal fixed", `{"type":"fixed","name":"Money","size":8,"logicalType":"decimal","precision":12,"scale":3}`, big.NewRat(-98765, 1000)},
		{"nullable string set", `["null","string"]`, "hello"},
		{"nullable string null", `["null","string"]`, nil},
		{"nullable long", `["null","long"]`, int64(7)},
		{"nullable date", `["null",{"type":"int","logicalType":"date"}]`, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"union string int", `["string","int"]`, 9},
		{
			"nested record",
			`{"type":"record","name":"Inner","fields":[{"name":"d","type":{"type":"int","logicalType":"date"}},{"name":"n","type":"int"},{"name":"c","type":{"type":"enum","name":"Size","symbols":["S","M","L"]}}]}`,
			map[string]any{"d": time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC), "n": 3, "c": "M"},
		},
		{
			"record in array and map",
			`{"type":"record","name":"Outer","fields":[` +
				`{"name":"items","type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"q","type":"int"}]}}},` +
				`{"name":"tags","type":{"type":"map","values":{"type":"enum","name":"Tag","symbols":["A","B"]}}}]}`,
			map[string]any{
				"items": []any{map[string]any{"at": ts.Truncate(time.Millisecond), "q": 2}},
				"tags":  map[string]any{"x": "B"},
			},
		},
		{"array of int", `{"type":"array","items":"int"}`, []any{1, 2, 3}},
		{"array of date", `{"type":"array","items":{"type":"int","logicalType":"date"}}`, []any{time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC)}},
		{"map of int", `{"type":"map","values":"int"}`, map[string]any{"a": 1}},
	}
}

func (c schemaFidelityCase) schema() string {
	return `{"type":"record","name":"fidelity","namespace":"sdk.test","fields":[` +
		`{"name":"id","type":"long"},{"name":"v","type":` + c.field + `}]}`
}

func (c schemaFidelityCase) data() opencdc.StructuredData {
	return opencdc.StructuredData{"id": int64(1), "v": c.value}
}

// newSchemaMiddlewareProcessor wraps f in the default middleware, exactly as
// sdk.Run does for a standalone processor.
func newSchemaMiddlewareProcessor(t *testing.T, f func(context.Context, opencdc.Record) (opencdc.Record, error)) Processor {
	t.Helper()
	p := ProcessorWithMiddleware(NewProcessorFunc(Specification{Name: "fidelity"}, f), DefaultProcessorMiddleware()...)
	if err := p.Configure(context.Background(), config.Config{}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	return p
}

// encodedRecord registers c's schema under a fresh subject and returns a
// record whose key and payload (before and after) are Avro bytes of c's
// value, with the schema attached, as a source connector would produce it.
func encodedRecord(t *testing.T, subject string, c schemaFidelityCase) (opencdc.Record, schema.Schema) {
	t.Helper()
	ctx := context.Background()
	sch, err := sdkschema.Create(ctx, schema.TypeAvro, subject, []byte(c.schema()))
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}
	raw, err := sch.Marshal(c.data())
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	rec := opencdc.Record{
		Operation: opencdc.OperationUpdate,
		Metadata:  opencdc.Metadata{},
		Key:       opencdc.RawData(bytes.Clone(raw)),
		Payload: opencdc.Change{
			Before: opencdc.RawData(bytes.Clone(raw)),
			After:  opencdc.RawData(bytes.Clone(raw)),
		},
	}
	schema.AttachKeySchemaToRecord(rec, sch)
	schema.AttachPayloadSchemaToRecord(rec, sch)
	return rec, sch
}

func fidelitySubject(t *testing.T) string {
	t.Helper()
	return strings.NewReplacer("/", ".", " ", "_").Replace(t.Name())
}

// TestSchemaMiddleware_PassthroughKeepsSchema is the regression test for the
// encode middleware rebuilding the schema from decoded values when a processor
// keeps the schema subject and version (invariant 6). A processor that returns
// its input unchanged must get back the exact bytes and the exact schema it
// was given: no new schema version, no widened or renamed type.
func TestSchemaMiddleware_PassthroughKeepsSchema(t *testing.T) {
	passthrough := func(_ context.Context, r opencdc.Record) (opencdc.Record, error) { return r, nil }

	for _, c := range schemaFidelityCases() {
		t.Run(c.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			p := newSchemaMiddlewareProcessor(t, passthrough)

			in, sch := encodedRecord(t, fidelitySubject(t), c)
			want := in.Clone()

			got := p.Process(ctx, []opencdc.Record{in})
			is.Equal(len(got), 1)
			out, ok := got[0].(SingleRecord)
			if !ok {
				t.Fatalf("expected SingleRecord, got %T: %+v", got[0], got[0])
			}

			for _, md := range []string{
				opencdc.MetadataKeySchemaSubject, opencdc.MetadataKeySchemaVersion,
				opencdc.MetadataPayloadSchemaSubject, opencdc.MetadataPayloadSchemaVersion,
			} {
				if out.Metadata[md] != want.Metadata[md] {
					outSch := fetchAttached(t, out.Metadata, md)
					t.Errorf("%s: got %q, want %q (original schema %s, re-encoded with %s)",
						md, out.Metadata[md], want.Metadata[md], sch.Bytes, outSch)
				}
			}
			is.Equal(out.Key, want.Key)                       // key bytes changed
			is.Equal(out.Payload.Before, want.Payload.Before) // payload.before bytes changed
			is.Equal(out.Payload.After, want.Payload.After)   // payload.after bytes changed
		})
	}
}

// fetchAttached returns the schema text attached under a subject metadata key
// (or its version key), for failure messages.
func fetchAttached(t *testing.T, md opencdc.Metadata, key string) string {
	t.Helper()
	subjectKey, versionKey := opencdc.MetadataPayloadSchemaSubject, opencdc.MetadataPayloadSchemaVersion
	if strings.HasPrefix(key, "opencdc.key") {
		subjectKey, versionKey = opencdc.MetadataKeySchemaSubject, opencdc.MetadataKeySchemaVersion
	}
	v, err := strconv.Atoi(md[versionKey])
	if err != nil {
		return "<no version>"
	}
	sch, err := sdkschema.Get(context.Background(), md[subjectKey], v)
	if err != nil {
		return "<" + err.Error() + ">"
	}
	return string(sch.Bytes)
}

// decodeAttached decodes data with the payload schema attached to rec.
func decodeAttached(t *testing.T, rec opencdc.Record, data opencdc.Data) (opencdc.StructuredData, schema.Schema) {
	t.Helper()
	ctx := context.Background()
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	if err != nil {
		t.Fatalf("payload schema subject: %v", err)
	}
	version, err := rec.Metadata.GetPayloadSchemaVersion()
	if err != nil {
		t.Fatalf("payload schema version: %v", err)
	}
	sch, err := sdkschema.Get(ctx, subject, version)
	if err != nil {
		t.Fatalf("get attached schema: %v", err)
	}
	raw, ok := data.(opencdc.RawData)
	if !ok {
		t.Fatalf("expected encoded payload, got %T", data)
	}
	var out opencdc.StructuredData
	if err := sch.Unmarshal(raw.Bytes(), &out); err != nil {
		t.Fatalf("decode with attached schema: %v", err)
	}
	return out, sch
}

// TestSchemaMiddleware_ChangedValues covers a processor that keeps the schema
// subject and version but writes new values. Values that still fit the
// original schema must be encoded with it. Values that do not fit must never be
// coerced into it: the record falls back to an extracted schema that holds the
// values exactly, registered as a new version.
func TestSchemaMiddleware_ChangedValues(t *testing.T) {
	const sch = `{"type":"record","name":"Row","namespace":"sdk.test","fields":[` +
		`{"name":"n","type":"int"},` +
		`{"name":"d","type":{"type":"int","logicalType":"date"}},` +
		`{"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},` +
		`{"name":"c","type":{"type":"enum","name":"Color","symbols":["RED","GREEN"]}},` +
		`{"name":"f","type":"float"},` +
		`{"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}},` +
		`{"name":"opt","type":["null","string"]}]}`
	base := func() opencdc.StructuredData {
		return opencdc.StructuredData{
			"n":   int32(1),
			"d":   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			"at":  time.Date(2026, 1, 1, 1, 2, 3, 4e6, time.UTC),
			"c":   "RED",
			"f":   float32(0.5),
			"amt": big.NewRat(150, 100),
			"opt": nil,
		}
	}

	testCases := []struct {
		name   string
		change func(opencdc.StructuredData)
		// keepsSchema is true when the changed value fits the original schema
		// and must be encoded with it.
		keepsSchema bool
	}{
		{"same types, new values", func(d opencdc.StructuredData) {
			d["n"] = 7 // a Go int fits an Avro int
			d["d"] = time.Date(2027, 2, 3, 0, 0, 0, 0, time.UTC)
			d["c"] = "GREEN"
			d["opt"] = "set"
			d["amt"] = big.NewRat(-2, 100)
		}, true},
		{"field added", func(d opencdc.StructuredData) { d["vector"] = []any{0.1, 0.2} }, false},
		{"field removed", func(d opencdc.StructuredData) { delete(d, "opt") }, false},
		{"int overflows Avro int", func(d opencdc.StructuredData) { d["n"] = int64(1) << 40 }, false},
		{"time of day in a date", func(d opencdc.StructuredData) { d["d"] = time.Date(2027, 2, 3, 4, 5, 6, 0, time.UTC) }, false},
		{"microseconds in timestamp-millis", func(d opencdc.StructuredData) {
			d["at"] = time.Date(2026, 1, 1, 1, 2, 3, 4005000, time.UTC)
		}, false},
		{"symbol outside the enum", func(d opencdc.StructuredData) { d["c"] = "BLUE" }, false},
		{"double precision in a float", func(d opencdc.StructuredData) { d["f"] = 0.1 }, false},
		{"decimal finer than scale", func(d opencdc.StructuredData) { d["amt"] = big.NewRat(1, 1000) }, false},
		{"type changed", func(d opencdc.StructuredData) { d["n"] = "seven" }, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			subject := fidelitySubject(t)

			orig, err := sdkschema.Create(ctx, schema.TypeAvro, subject, []byte(sch))
			is.NoErr(err)
			raw, err := orig.Marshal(base())
			is.NoErr(err)

			var want opencdc.StructuredData
			p := newSchemaMiddlewareProcessor(t, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				after, ok := r.Payload.After.(opencdc.StructuredData)
				if !ok {
					t.Fatalf("decode middleware did not decode payload.after: %T", r.Payload.After)
				}
				tc.change(after)
				want = after.Clone().(opencdc.StructuredData)
				return r, nil
			})

			in := opencdc.Record{Metadata: opencdc.Metadata{}, Payload: opencdc.Change{After: opencdc.RawData(raw)}}
			schema.AttachPayloadSchemaToRecord(in, orig)

			got := p.Process(ctx, []opencdc.Record{in})
			is.Equal(len(got), 1)
			out, ok := got[0].(SingleRecord)
			if !ok {
				t.Fatalf("expected SingleRecord, got %T: %+v", got[0], got[0])
			}

			decoded, outSch := decodeAttached(t, opencdc.Record(out), out.Payload.After)
			if path, ok := equivalentValues(map[string]any(want), map[string]any(decoded), ""); !ok {
				t.Fatalf("value changed at %q:\nwant %#v\ngot  %#v\nschema %s", path, want, decoded, outSch.Bytes)
			}
			if tc.keepsSchema {
				is.Equal(outSch.Version, orig.Version) // must keep the original schema
				is.Equal(string(outSch.Bytes), sch)
			} else {
				is.True(outSch.Version != orig.Version) // must not coerce into the original schema
			}
		})
	}
}

// TestSchemaMiddleware_NullableRecordBranch pins a case the fix cannot reach.
// The commons Avro serde decodes a ["null", record] field to the plain record
// map, but only encodes it from the named form {"ns.Name": {...}}, so a
// decoded value does not re-encode with its own schema. The middleware must
// then fall back to an extracted schema without losing the values.
// See the conduit-commons issue linked in the PR; once that is fixed this case
// belongs in TestSchemaMiddleware_PassthroughKeepsSchema.
func TestSchemaMiddleware_NullableRecordBranch(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	c := schemaFidelityCase{
		name:  "nullable nested record",
		field: `["null",{"type":"record","name":"Opt","fields":[{"name":"n","type":"int"}]}]`,
		value: map[string]any{"sdk.test.Opt": map[string]any{"n": 5}},
	}
	in, sch := encodedRecord(t, fidelitySubject(t), c)
	p := newSchemaMiddlewareProcessor(t, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) { return r, nil })

	got := p.Process(ctx, []opencdc.Record{in})
	is.Equal(len(got), 1)
	out, ok := got[0].(SingleRecord)
	if !ok {
		t.Fatalf("expected SingleRecord, got %T: %+v", got[0], got[0])
	}
	decoded, outSch := decodeAttached(t, opencdc.Record(out), out.Payload.After)
	want := map[string]any{"id": int64(1), "v": map[string]any{"n": 5}}
	if path, ok := equivalentValues(want, map[string]any(decoded), ""); !ok {
		t.Fatalf("value changed at %q: got %#v", path, decoded)
	}
	is.True(outSch.Version != sch.Version) // documents the known commons limitation

	// A nullable record whose fields mix types cannot be extracted either, so
	// the fallback fails and the record becomes an error record. main does the
	// same; the data is not lost, the record fails loudly.
	mixed := schemaFidelityCase{
		name:  "nullable nested record, mixed types",
		field: `["null",{"type":"record","name":"Opt","fields":[{"name":"n","type":"int"},{"name":"d","type":{"type":"int","logicalType":"date"}}]}]`,
		value: map[string]any{"sdk.test.Opt": map[string]any{"n": 5, "d": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}},
	}
	in, _ = encodedRecord(t, fidelitySubject(t)+".mixed", mixed)
	got = p.Process(ctx, []opencdc.Record{in})
	is.Equal(len(got), 1)
	if _, ok := got[0].(ErrorRecord); !ok {
		t.Fatalf("expected ErrorRecord for a mixed-type nullable record, got %T: %+v", got[0], got[0])
	}
}

func TestEquivalentValues(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		name      string
		want, got any
		equal     bool
	}{
		{"int vs int32", 5, int32(5), true},
		{"int64 vs int32 different", int64(1) << 40, int32(0), false},
		{"large int64 exact", int64(1)<<62 + 1, int64(1)<<62 + 1, true},
		{"large int64 off by one", int64(1)<<62 + 1, int64(1) << 62, false},
		{"float32 vs float64 same", float32(0.5), 0.5, true},
		{"float64 rounded to float32", 0.1, float32(0.1), false},
		{"NaN", math.NaN(), math.NaN(), true},
		{"time same instant", ts, ts.In(time.FixedZone("x", 3600)), true},
		{"time truncated", ts.Add(time.Microsecond), ts, false},
		{"rat", big.NewRat(1, 2), big.NewRat(2, 4), true},
		{"rat truncated", big.NewRat(1, 1000), big.NewRat(0, 1), false},
		{"bytes vs fixed", []byte{1, 2}, [2]byte{1, 2}, true},
		{"bytes differ", []byte{1, 2}, []byte{1, 3}, false},
		{"string vs bytes", "ab", []byte("ab"), false},
		{"extra key", map[string]any{"a": 1}, map[string]any{"a": 1, "b": nil}, false},
		{"missing key", map[string]any{"a": 1, "b": 2}, map[string]any{"a": 1}, false},
		{"nested", map[string]any{"a": []any{map[string]any{"b": 1}}}, map[string]any{"a": []any{map[string]any{"b": int32(1)}}}, true},
		{"nested differ", map[string]any{"a": []any{map[string]any{"b": 1}}}, map[string]any{"a": []any{map[string]any{"b": int32(2)}}}, false},
		{"typed slice vs any slice", []float32{1, 2}, []any{float32(1), float32(2)}, true},
		{"slice length", []any{1}, []any{1, 2}, false},
		{"untyped nil vs typed nil map", nil, map[string]any(nil), true},
		{"nil vs value", nil, 0, false},
		{"bool vs int", true, 1, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := equivalentValues(tc.want, tc.got, "")
			if ok != tc.equal {
				t.Fatalf("equivalentValues(%#v, %#v) = %v, want %v", tc.want, tc.got, ok, tc.equal)
			}
		})
	}
}

// TestEncodeLossless_DoesNotModifyInput guards the deep copy: the codec
// rewrites union values in place, and the fallback path must see the
// processor's values, not the codec's rewritten form.
func TestEncodeLossless_DoesNotModifyInput(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	sch, err := sdkschema.Create(ctx, schema.TypeAvro, fidelitySubject(t),
		[]byte(`{"type":"record","name":"U","fields":[{"name":"m","type":{"type":"map","values":["null","string",{"type":"array","items":"string"}]}},{"name":"a","type":{"type":"array","items":["null","long",{"type":"map","values":"long"}]}}]}`))
	is.NoErr(err)
	data := opencdc.StructuredData{
		"m": map[string]any{"x": "y", "l": []any{"z"}},
		"a": []any{int64(1), map[string]any{"k": int64(2)}},
	}
	want := deepCopyValue(data)
	_, err = encodeLossless(sch, data)
	is.NoErr(err)
	is.Equal(map[string]any(data), want)
}

// captureWarnings redirects the SDK logger into a buffer for the test and
// returns a function that reports how many schema mismatch warnings were
// logged for kind ("key" or "payload") and subject:version.
func captureWarnings(t *testing.T) func(kind string, sch schema.Schema) int {
	t.Helper()
	var buf bytes.Buffer
	old := pprocutils.Logger
	pprocutils.Logger = zerolog.New(&buf)
	t.Cleanup(func() { pprocutils.Logger = old })
	return func(kind string, sch schema.Schema) int {
		needle := "processed record " + kind + " no longer fits its schema " + sch.Subject + ":" + strconv.Itoa(sch.Version) + ","
		return strings.Count(buf.String(), needle)
	}
}

// fallbackFixture registers a schema {id long, d date} under subject and
// returns it with the Avro bytes of a value of it.
func fallbackFixture(t *testing.T, subject string) (schema.Schema, []byte) {
	t.Helper()
	sch, err := sdkschema.Create(context.Background(), schema.TypeAvro, subject,
		[]byte(`{"type":"record","name":"Row","namespace":"sdk.test","fields":[{"name":"id","type":"long"},{"name":"d","type":{"type":"int","logicalType":"date"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sch.Marshal(opencdc.StructuredData{"id": int64(1), "d": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	return sch, raw
}

// TestSchemaMiddleware_KeyFallback: a processor that adds a field to the key
// sends the key through the fallback. The key gets a new schema version that
// holds the new field, and the warning is logged for the key schema.
func TestSchemaMiddleware_KeyFallback(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	warnings := captureWarnings(t)
	sch, raw := fallbackFixture(t, fidelitySubject(t))

	p := newSchemaMiddlewareProcessor(t, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		r.Key.(opencdc.StructuredData)["added"] = "k"
		return r, nil
	})
	in := opencdc.Record{Metadata: opencdc.Metadata{}, Key: opencdc.RawData(raw)}
	schema.AttachKeySchemaToRecord(in, sch)

	got := p.Process(ctx, []opencdc.Record{in})
	is.Equal(len(got), 1)
	out, ok := got[0].(SingleRecord)
	if !ok {
		t.Fatalf("expected SingleRecord, got %T: %+v", got[0], got[0])
	}
	version, err := out.Metadata.GetKeySchemaVersion()
	is.NoErr(err)
	is.True(version != sch.Version) // added field must not be encoded with the original key schema
	outSch, err := sdkschema.Get(ctx, sch.Subject, version)
	is.NoErr(err)
	var decoded opencdc.StructuredData
	is.NoErr(outSch.Unmarshal(out.Key.Bytes(), &decoded))
	is.Equal(decoded["added"], "k")
	is.Equal(warnings("key", sch), 1)
	is.Equal(warnings("payload", sch), 0)
}

// TestSchemaMiddleware_BeforeFitsAfterDoesNot: when only after stops fitting,
// the whole payload falls back, so before and after share one schema, the one
// extracted from after. If before cannot be encoded with that schema (after
// gained a field before lacks), the record fails loudly instead; main behaves
// the same.
func TestSchemaMiddleware_BeforeFitsAfterDoesNot(t *testing.T) {
	testCases := []struct {
		name    string
		change  func(opencdc.StructuredData)
		wantErr string
	}{
		{"after value outside its type", func(d opencdc.StructuredData) {
			d["d"] = time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC) // time of day in a date
		}, ""},
		{"after gained a field", func(d opencdc.StructuredData) { d["added"] = "a" }, "missing required field added"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			warnings := captureWarnings(t)
			sch, raw := fallbackFixture(t, fidelitySubject(t))

			var wantAfter opencdc.StructuredData
			p := newSchemaMiddlewareProcessor(t, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				after := r.Payload.After.(opencdc.StructuredData)
				tc.change(after)
				wantAfter = after.Clone().(opencdc.StructuredData)
				return r, nil
			})
			in := opencdc.Record{
				Metadata: opencdc.Metadata{},
				Payload:  opencdc.Change{Before: opencdc.RawData(raw), After: opencdc.RawData(raw)},
			}
			schema.AttachPayloadSchemaToRecord(in, sch)

			got := p.Process(ctx, []opencdc.Record{in})
			is.Equal(len(got), 1)
			is.Equal(warnings("payload", sch), 1)
			if tc.wantErr != "" {
				errRec, ok := got[0].(ErrorRecord)
				if !ok {
					t.Fatalf("expected ErrorRecord, got %T: %+v", got[0], got[0])
				}
				is.True(strings.Contains(errRec.Error.Error(), tc.wantErr))
				return
			}
			out, ok := got[0].(SingleRecord)
			if !ok {
				t.Fatalf("expected SingleRecord, got %T: %+v", got[0], got[0])
			}
			after, outSch := decodeAttached(t, opencdc.Record(out), out.Payload.After)
			is.True(outSch.Version != sch.Version) // after must not be coerced into the date schema
			if path, ok := equivalentValues(map[string]any(wantAfter), map[string]any(after), ""); !ok {
				t.Fatalf("after changed at %q: %#v", path, after)
			}
			// before is encoded with the same, new schema and keeps its value
			before, _ := decodeAttached(t, opencdc.Record(out), out.Payload.Before)
			is.True(before["d"].(time.Time).Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
		})
	}
}

// TestSchemaMiddleware_WarnsOncePerSchema: the mismatch warning is logged once
// per subject and version, not once per middleware, so a second subject that
// stops fitting still warns.
func TestSchemaMiddleware_WarnsOncePerSchema(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	warnings := captureWarnings(t)
	first, rawFirst := fallbackFixture(t, fidelitySubject(t)+".first")
	second, rawSecond := fallbackFixture(t, fidelitySubject(t)+".second")

	p := newSchemaMiddlewareProcessor(t, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		r.Payload.After.(opencdc.StructuredData)["added"] = true
		return r, nil
	})
	rec := func(sch schema.Schema, raw []byte) opencdc.Record {
		r := opencdc.Record{Metadata: opencdc.Metadata{}, Payload: opencdc.Change{After: opencdc.RawData(raw)}}
		schema.AttachPayloadSchemaToRecord(r, sch)
		return r
	}

	for range 3 {
		got := p.Process(ctx, []opencdc.Record{rec(first, rawFirst)})
		_, ok := got[0].(SingleRecord)
		is.True(ok)
	}
	is.Equal(warnings("payload", first), 1)
	is.Equal(warnings("payload", second), 0)

	got := p.Process(ctx, []opencdc.Record{rec(second, rawSecond)})
	_, ok := got[0].(SingleRecord)
	is.True(ok)
	is.Equal(warnings("payload", second), 1)
	is.Equal(warnings("payload", first), 1)
}
