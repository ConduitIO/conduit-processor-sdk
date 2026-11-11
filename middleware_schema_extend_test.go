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
	"encoding/json"
	"errors"
	"maps"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	sdkschema "github.com/conduitio/conduit-processor-sdk/schema"
	"github.com/matryer/is"
)

// The source schema of the v0.21 CDC-parity smoke (wide Postgres table):
// int4, numeric(14,2) and date columns, plus a nullable text column and a
// nested record.
const (
	extendDecimalType = `{"type":"bytes","logicalType":"decimal","precision":14,"scale":2}`
	extendDateType    = `{"type":"int","logicalType":"date"}`
	extendNestedType  = `{"type":"record","name":"Addr","fields":[{"name":"zip","type":"int"},{"name":"since","type":` + extendDateType + `}]}`
	extendSchemaText  = `{"type":"record","name":"orders","namespace":"pg.public","fields":[` +
		`{"name":"id","type":"int"},` +
		`{"name":"amount","type":` + extendDecimalType + `},` +
		`{"name":"day","type":` + extendDateType + `},` +
		`{"name":"note","type":["null","string"],"default":null},` +
		`{"name":"addr","type":` + extendNestedType + `}]}`
	extendKeySchemaText = `{"type":"record","name":"orders_key","namespace":"pg.public","fields":[{"name":"id","type":"int"}]}`
)

func extendRow() opencdc.StructuredData {
	return opencdc.StructuredData{
		"id":     int32(7),
		"amount": big.NewRat(3232, 100),
		"day":    time.Date(2023, 12, 16, 0, 0, 0, 0, time.UTC),
		"note":   "t4",
		"addr":   map[string]any{"zip": int32(94107), "since": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)},
	}
}

// extendFixture registers the smoke schemas under fresh subjects and returns
// a CDC insert as the Postgres source emits it: Avro key and payload.after,
// no payload.before.
func extendFixture(t *testing.T) opencdc.Record {
	t.Helper()
	ctx := context.Background()
	subject := fidelitySubject(t)
	payloadSch, err := sdkschema.Create(ctx, schema.TypeAvro, subject+"-value", []byte(extendSchemaText))
	if err != nil {
		t.Fatalf("create payload schema: %v", err)
	}
	keySch, err := sdkschema.Create(ctx, schema.TypeAvro, subject+"-key", []byte(extendKeySchemaText))
	if err != nil {
		t.Fatalf("create key schema: %v", err)
	}
	after, err := payloadSch.Marshal(extendRow())
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	key, err := keySch.Marshal(opencdc.StructuredData{"id": int32(7)})
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	rec := opencdc.Record{
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{},
		Key:       opencdc.RawData(key),
		Payload:   opencdc.Change{After: opencdc.RawData(after)},
	}
	schema.AttachKeySchemaToRecord(rec, keySch)
	schema.AttachPayloadSchemaToRecord(rec, payloadSch)
	return rec
}

// newExtendProcessor wraps f in the default middleware and configures it with
// cfg, the way a pipeline's processor settings arrive.
func newExtendProcessor(t *testing.T, cfg config.Config, f func(context.Context, opencdc.Record) (opencdc.Record, error)) Processor {
	t.Helper()
	p := ProcessorWithMiddleware(NewProcessorFunc(Specification{Name: "extend"}, f), DefaultProcessorMiddleware()...)
	if err := p.Configure(context.Background(), cfg); err != nil {
		t.Fatalf("configure: %v", err)
	}
	return p
}

// extendPayloadCfg returns a fresh config each time: Configure removes the
// settings it consumes from the map it is given.
func extendPayloadCfg() config.Config {
	return config.Config{configProcessorSchemaEncodePayloadStrategy: string(SchemaEncodeStrategyExtend)}
}

// processOne runs rec through p and returns the single result.
func processOne(t *testing.T, p Processor, rec opencdc.Record) ProcessedRecord {
	t.Helper()
	got := p.Process(context.Background(), []opencdc.Record{rec})
	if len(got) != 1 {
		t.Fatalf("expected 1 result, got %d", len(got))
	}
	return got[0]
}

func mustSingle(t *testing.T, pr ProcessedRecord) opencdc.Record {
	t.Helper()
	out, ok := pr.(SingleRecord)
	if !ok {
		t.Fatalf("expected SingleRecord, got %T: %+v", pr, pr)
	}
	return opencdc.Record(out)
}

func mustErrorIs(t *testing.T, pr ProcessedRecord, target error) {
	t.Helper()
	er, ok := pr.(ErrorRecord)
	if !ok {
		t.Fatalf("expected ErrorRecord wrapping %v, got %T: %+v", target, pr, pr)
	}
	if !errors.Is(er.Error, target) {
		t.Fatalf("expected error wrapping %v, got %v", target, er.Error)
	}
}

// schemaFields returns the compacted JSON type, and the whole compacted
// definition, of each field of an Avro record schema, plus the field order.
func schemaFields(t *testing.T, sch schema.Schema) (types map[string]string, defs map[string]string, order []string) {
	t.Helper()
	var rec struct {
		Fields []json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(sch.Bytes, &rec); err != nil {
		t.Fatalf("parse schema %s: %v", sch.Bytes, err)
	}
	types, defs = map[string]string{}, map[string]string{}
	for _, raw := range rec.Fields {
		var f struct {
			Name string          `json:"name"`
			Type json.RawMessage `json:"type"`
		}
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("parse field: %v", err)
		}
		types[f.Name] = compactJSON(t, f.Type)
		defs[f.Name] = compactJSON(t, raw)
		order = append(order, f.Name)
	}
	return types, defs, order
}

func compactJSON(t *testing.T, b []byte) string {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		t.Fatalf("compact %s: %v", b, err)
	}
	return buf.String()
}

func setField(name string, value any) func(context.Context, opencdc.Record) (opencdc.Record, error) {
	return func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		r.Payload.After.(opencdc.StructuredData)[name] = value
		return r, nil
	}
}

// TestSchemaEncodeExtend_KeepsCarriedTypes is the smoke reproduction: a
// field.set-like processor adds __op to a Postgres CDC row. With the extend
// strategy the registered schema keeps int, decimal(14,2) and date (and the
// nested record) exactly, and __op is a nullable string with a null default.
func TestSchemaEncodeExtend_KeepsCarriedTypes(t *testing.T) {
	is := is.New(t)
	p := newExtendProcessor(t, extendPayloadCfg(), setField("__op", "c"))

	out := mustSingle(t, processOne(t, p, extendFixture(t)))
	decoded, sch := decodeAttached(t, out, out.Payload.After)

	carried, carriedDefs, _ := schemaFields(t, schema.Schema{Bytes: []byte(extendSchemaText)})
	types, defs, order := schemaFields(t, sch)
	is.Equal(order, []string{"id", "amount", "day", "note", "addr", "__op"})
	for _, name := range []string{"id", "amount", "day", "note", "addr"} {
		is.Equal(defs[name], carriedDefs[name]) // carried field definition changed
	}
	is.Equal(types["id"], `"int"`)
	is.Equal(types["amount"], carried["amount"])
	is.Equal(types["day"], carried["day"])
	is.Equal(defs["__op"], `{"name":"__op","type":["null","string"],"default":null}`)

	want := extendRow()
	want["__op"] = "c"
	if path, ok := equivalentValues(map[string]any(want), map[string]any(decoded), ""); !ok {
		t.Fatalf("decoded value differs at %q: %v", path, decoded)
	}
	is.Equal(sch.Subject, fidelitySubject(t)+"-value") // registered under the carried subject
	is.Equal(sch.Version, 2)
}

// TestSchemaEncodeExtend_DefaultIsInfer pins the default: without the setting
// the middleware still infers, so the same pipeline widens int to long. If
// this changes, the default changed.
func TestSchemaEncodeExtend_DefaultIsInfer(t *testing.T) {
	is := is.New(t)
	p := newExtendProcessor(t, config.Config{}, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		after := r.Payload.After.(opencdc.StructuredData)
		after["__op"] = "c"
		delete(after, "addr") // inference cannot type a nested record with mixed field types
		return r, nil
	})

	out := mustSingle(t, processOne(t, p, extendFixture(t)))
	_, sch := decodeAttached(t, out, out.Payload.After)
	types, _, _ := schemaFields(t, sch)
	is.Equal(types["id"], `"long"`)
	is.True(strings.Contains(types["amount"], `"scale":10`))
}

func TestSchemaEncodeExtend_RemovedField(t *testing.T) {
	is := is.New(t)
	p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		delete(r.Payload.After.(opencdc.StructuredData), "note")
		return r, nil
	})

	out := mustSingle(t, processOne(t, p, extendFixture(t)))
	decoded, sch := decodeAttached(t, out, out.Payload.After)
	_, carriedDefs, _ := schemaFields(t, schema.Schema{Bytes: []byte(extendSchemaText)})
	_, defs, order := schemaFields(t, sch)
	is.Equal(order, []string{"id", "amount", "day", "addr"})
	for _, name := range order {
		is.Equal(defs[name], carriedDefs[name])
	}
	_, hasNote := decoded["note"]
	is.True(!hasNote)
}

// TestSchemaEncodeExtend_EmptyPartPassesThrough pins that an absent part (nil
// or zero-length raw data) is not encoded and that the record's metadata is
// left exactly as it came in, for the payload and for the key.
func TestSchemaEncodeExtend_EmptyPartPassesThrough(t *testing.T) {
	bothExtend := func() config.Config {
		return config.Config{
			configProcessorSchemaEncodePayloadStrategy: string(SchemaEncodeStrategyExtend),
			configProcessorSchemaEncodeKeyStrategy:     string(SchemaEncodeStrategyExtend),
		}
	}
	cases := []struct {
		name  string
		set   func(r *opencdc.Record)
		check func(is *is.I, out opencdc.Record)
	}{
		{
			"payload nil",
			func(r *opencdc.Record) { r.Payload.After = nil },
			func(is *is.I, out opencdc.Record) { is.Equal(out.Payload.After, nil) },
		},
		{
			"payload zero-length raw",
			func(r *opencdc.Record) { r.Payload.After = opencdc.RawData{} },
			func(is *is.I, out opencdc.Record) { is.Equal(out.Payload.After, opencdc.RawData{}) },
		},
		{
			"key nil",
			func(r *opencdc.Record) { r.Key = nil },
			func(is *is.I, out opencdc.Record) { is.Equal(out.Key, nil) },
		},
		{
			"key zero-length raw",
			func(r *opencdc.Record) { r.Key = opencdc.RawData{} },
			func(is *is.I, out opencdc.Record) { is.Equal(out.Key, opencdc.RawData{}) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			p := newExtendProcessor(t, bothExtend(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				tc.set(&r)
				return r, nil
			})
			in := extendFixture(t)
			wantMeta := maps.Clone(in.Metadata)
			out := mustSingle(t, processOne(t, p, in))
			tc.check(is, out)
			is.Equal(out.Metadata, wantMeta) // metadata changed for an empty part
			_, err := sdkschema.Get(context.Background(), fidelitySubject(t)+"-value", 2)
			is.True(errors.Is(err, sdkschema.ErrVersionNotFound)) // a payload version was registered
			_, err = sdkschema.Get(context.Background(), fidelitySubject(t)+"-key", 2)
			is.True(errors.Is(err, sdkschema.ErrVersionNotFound)) // a key version was registered
		})
	}
}

// zeroFieldSchema checks that sch is the carried record with every field
// removed: same name and namespace, zero fields, registered as version 2 of
// subject.
func zeroFieldSchema(t *testing.T, sch schema.Schema, subject, name string) {
	t.Helper()
	is := is.New(t)
	var top struct {
		Type      string            `json:"type"`
		Name      string            `json:"name"`
		Namespace string            `json:"namespace"`
		Fields    []json.RawMessage `json:"fields"`
	}
	is.NoErr(json.Unmarshal(sch.Bytes, &top))
	is.Equal(top.Type, "record")
	is.Equal(top.Name, name)
	is.Equal(top.Namespace, "pg.public")
	is.True(top.Fields != nil) // fields must be [] for the schema to parse
	is.Equal(len(top.Fields), 0)
	is.Equal(sch.Subject, subject)
	is.Equal(sch.Version, 2)
}

// TestSchemaEncodeExtend_AllFieldsRemoved covers a processor that removes
// every field of a part, so the structured data is {}. The removal rule
// applies to every field: the record gets a zero-field record schema, a new
// version of the carried subject, so its metadata matches the data instead of
// naming the carried schema.
func TestSchemaEncodeExtend_AllFieldsRemoved(t *testing.T) {
	t.Run("payload", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
			r.Payload.After = opencdc.StructuredData{}
			return r, nil
		})
		out := mustSingle(t, processOne(t, p, extendFixture(t)))
		decoded, sch := decodeAttached(t, out, out.Payload.After)
		zeroFieldSchema(t, sch, fidelitySubject(t)+"-value", "orders")
		is.Equal(len(decoded), 0)
		is.Equal(out.Metadata[opencdc.MetadataPayloadSchemaVersion], "2")
	})
	t.Run("payload before and after", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
			r.Payload.Before = opencdc.StructuredData{}
			r.Payload.After = opencdc.StructuredData{}
			return r, nil
		})
		in := extendFixture(t)
		in.Operation = opencdc.OperationUpdate
		in.Payload.Before = opencdc.RawData(bytes.Clone(in.Payload.After.(opencdc.RawData)))
		out := mustSingle(t, processOne(t, p, in))
		_, sch := decodeAttached(t, out, out.Payload.After)
		zeroFieldSchema(t, sch, fidelitySubject(t)+"-value", "orders")
		_, ok := out.Payload.Before.(opencdc.RawData)
		is.True(ok) // before was not encoded with the zero-field schema
	})
	t.Run("key", func(t *testing.T) {
		is := is.New(t)
		cfg := config.Config{configProcessorSchemaEncodeKeyStrategy: string(SchemaEncodeStrategyExtend)}
		p := newExtendProcessor(t, cfg, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
			r.Key = opencdc.StructuredData{}
			return r, nil
		})
		in := extendFixture(t)
		out := mustSingle(t, processOne(t, p, in))
		subject, err := out.Metadata.GetKeySchemaSubject()
		is.NoErr(err)
		v, err := out.Metadata.GetKeySchemaVersion()
		is.NoErr(err)
		sch, err := sdkschema.Get(context.Background(), subject, v)
		is.NoErr(err)
		zeroFieldSchema(t, sch, fidelitySubject(t)+"-key", "orders_key")
		raw, ok := out.Key.(opencdc.RawData)
		is.True(ok) // key was not encoded with the zero-field schema
		var decoded opencdc.StructuredData
		is.NoErr(sch.Unmarshal(raw.Bytes(), &decoded))
		is.Equal(len(decoded), 0)
		// the payload is unchanged and keeps its carried schema
		is.Equal(out.Metadata[opencdc.MetadataPayloadSchemaVersion], in.Metadata[opencdc.MetadataPayloadSchemaVersion])
	})
}

// TestSchemaEncodeExtend_ValueDoesNotFit covers invariant 6: a value that does
// not fit its carried type fails the record instead of being coerced or
// truncated, and no schema version is registered for it.
func TestSchemaEncodeExtend_ValueDoesNotFit(t *testing.T) {
	cases := []struct {
		name  string
		field string
		value any
	}{
		{"int to string", "id", "seven"},
		{"int out of range", "id", int64(1) << 40},
		{"decimal finer than scale", "amount", big.NewRat(1234, 1000)},
		{"date with a time of day", "day", time.Date(2023, 12, 16, 1, 0, 0, 0, time.UTC)},
		{"null into non-null", "id", nil},
		{"nested key added", "addr", map[string]any{"zip": int32(1), "since": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC), "extra": "x"}},
		{"nested key removed", "addr", map[string]any{"zip": int32(1)}},
		{"nested type changed", "addr", map[string]any{"zip": "x", "since": time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				after := r.Payload.After.(opencdc.StructuredData)
				after[tc.field] = tc.value
				after["__op"] = "u" // force the extend path for in-range changes too
				return r, nil
			})
			mustErrorIs(t, processOne(t, p, extendFixture(t)), ErrSchemaExtendMismatch)
			_, err := sdkschema.Get(context.Background(), fidelitySubject(t)+"-value", 2)
			is.True(errors.Is(err, sdkschema.ErrVersionNotFound)) // a schema was registered for a failing record
		})
	}
}

func TestSchemaEncodeExtend_Nullable(t *testing.T) {
	t.Run("null in carried nullable field", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
			after := r.Payload.After.(opencdc.StructuredData)
			after["note"] = nil
			after["__op"] = "c"
			return r, nil
		})
		out := mustSingle(t, processOne(t, p, extendFixture(t)))
		decoded, sch := decodeAttached(t, out, out.Payload.After)
		types, _, _ := schemaFields(t, sch)
		is.Equal(types["note"], `["null","string"]`)
		is.Equal(decoded["note"], nil)
	})
	t.Run("added field with a null value", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), setField("__deleted", nil))
		out := mustSingle(t, processOne(t, p, extendFixture(t)))
		decoded, sch := decodeAttached(t, out, out.Payload.After)
		_, defs, _ := schemaFields(t, sch)
		is.Equal(defs["__deleted"], `{"name":"__deleted","type":["null","string"],"default":null}`)
		v, ok := decoded["__deleted"]
		is.True(ok)
		is.Equal(v, nil)
	})
	t.Run("added field with a long value", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), setField("__ts_ms", int64(1700000000000)))
		out := mustSingle(t, processOne(t, p, extendFixture(t)))
		_, sch := decodeAttached(t, out, out.Payload.After)
		_, defs, _ := schemaFields(t, sch)
		is.Equal(defs["__ts_ms"], `{"name":"__ts_ms","type":["null","long"],"default":null}`)
	})
}

func TestSchemaEncodeExtend_BeforeAndAfter(t *testing.T) {
	// update records carry before and after, which share one schema
	withBefore := func(t *testing.T) opencdc.Record {
		t.Helper()
		rec := extendFixture(t)
		rec.Operation = opencdc.OperationUpdate
		rec.Payload.Before = opencdc.RawData(bytes.Clone(rec.Payload.After.(opencdc.RawData)))
		return rec
	}
	t.Run("both changed alike", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
			r.Payload.Before.(opencdc.StructuredData)["__op"] = "u"
			r.Payload.After.(opencdc.StructuredData)["__op"] = "u"
			return r, nil
		})
		out := mustSingle(t, processOne(t, p, withBefore(t)))
		before, sch := decodeAttached(t, out, out.Payload.Before)
		after, _ := decodeAttached(t, out, out.Payload.After)
		is.Equal(before["__op"], "u")
		is.Equal(after["__op"], "u")
		types, _, _ := schemaFields(t, sch)
		is.Equal(types["id"], `"int"`)
	})
	// Decided for the v0.22 opt-in (revisit before extend becomes the default
	// in v0.24): before and after with different top-level fields fail the
	// record, and nothing is registered. One schema describes both parts, and
	// filling the gap with a null or a default would invent data.
	differ := []struct {
		name string
		f    func(context.Context, opencdc.Record) (opencdc.Record, error)
	}{
		{"only after changed", setField("__op", "u")},
		{"after has every field removed", func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
			r.Payload.After = opencdc.StructuredData{}
			return r, nil
		}},
		{"before and after removed different fields", func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
			delete(r.Payload.Before.(opencdc.StructuredData), "note")
			delete(r.Payload.After.(opencdc.StructuredData), "day")
			return r, nil
		}},
	}
	for _, tc := range differ {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			p := newExtendProcessor(t, extendPayloadCfg(), tc.f)
			mustErrorIs(t, processOne(t, p, withBefore(t)), ErrSchemaExtendMismatch)
			_, err := sdkschema.Get(context.Background(), fidelitySubject(t)+"-value", 2)
			is.True(errors.Is(err, sdkschema.ErrVersionNotFound)) // a schema was registered for a failing record
		})
	}
	// The sameKeys guard gives the message an operator needs: which field and
	// which two parts. Without it the round trip still fails, but only with a
	// field count.
	t.Run("error names the field and both parts", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), differ[2].f) // before lacks note, after lacks day
		pr := processOne(t, p, withBefore(t))
		mustErrorIs(t, pr, ErrSchemaExtendMismatch)
		is.True(strings.Contains(pr.(ErrorRecord).Error.Error(),
			`field "day" is not in both payload.before and payload.after`)) // message does not name the field and parts
	})
	// addedValue infers an added field's type from after first. With an empty
	// map in before and a map of longs in after, inferring from after gives a
	// map of long, which holds both; inferring from before gives a map of
	// string, and after would not fit.
	t.Run("added field type is inferred from after", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
			r.Payload.Before.(opencdc.StructuredData)["__n"] = map[string]any{}
			r.Payload.After.(opencdc.StructuredData)["__n"] = map[string]any{"a": int64(1)}
			return r, nil
		})
		out := mustSingle(t, processOne(t, p, withBefore(t)))
		before, sch := decodeAttached(t, out, out.Payload.Before)
		after, _ := decodeAttached(t, out, out.Payload.After)
		types, _, _ := schemaFields(t, sch)
		is.True(strings.Contains(types["__n"], `"long"`)) // type was not inferred from after
		is.Equal(len(before["__n"].(map[string]any)), 0)
		is.Equal(after["__n"], map[string]any{"a": int64(1)})
	})
}

// TestSchemaEncodeExtend_AddedTimeFinerThanMicros pins that an added
// time.Time with nanoseconds (time.Now() on Linux) fails the record: the
// inferred type is timestamp-micros, and the round trip refuses to truncate.
// The error names the added field. infer truncates the value silently.
func TestSchemaEncodeExtend_AddedTimeFinerThanMicros(t *testing.T) {
	ts := time.Date(2026, 11, 10, 12, 0, 0, 123456789, time.UTC)
	t.Run("nanoseconds fail", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), setField("__ts", ts))
		pr := processOne(t, p, extendFixture(t))
		mustErrorIs(t, pr, ErrSchemaExtendMismatch)
		is.True(strings.Contains(pr.(ErrorRecord).Error.Error(), `"__ts"`)) // error does not name the added field
		_, err := sdkschema.Get(context.Background(), fidelitySubject(t)+"-value", 2)
		is.True(errors.Is(err, sdkschema.ErrVersionNotFound)) // a schema was registered for a failing record
	})
	t.Run("microseconds fit", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), setField("__ts", ts.Truncate(time.Microsecond)))
		out := mustSingle(t, processOne(t, p, extendFixture(t)))
		decoded, _ := decodeAttached(t, out, out.Payload.After)
		got, ok := decoded["__ts"].(time.Time)
		is.True(ok)
		is.True(got.Equal(ts.Truncate(time.Microsecond)))
	})
}

// TestSchemaEncodeExtend_NullableRecordAndMap pins that carried
// ["null", record] and ["null", map] fields work with extend on the pinned
// conduit-commons (conduit-commons#306 is fixed since v0.12.0): an unchanged
// record keeps its schema version, and a record with an added field keeps
// both carried definitions exactly.
func TestSchemaEncodeExtend_NullableRecordAndMap(t *testing.T) {
	const text = `{"type":"record","name":"t","namespace":"pg.public","fields":[` +
		`{"name":"id","type":"int"},` +
		`{"name":"in","type":["null",{"type":"record","name":"In","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}],"default":null},` +
		`{"name":"m","type":["null",{"type":"map","values":"long"}],"default":null}]}`
	fixture := func(t *testing.T) opencdc.Record {
		t.Helper()
		sch, err := sdkschema.Create(context.Background(), schema.TypeAvro, fidelitySubject(t)+"-value", []byte(text))
		if err != nil {
			t.Fatalf("create schema: %v", err)
		}
		after, err := sch.Marshal(opencdc.StructuredData{
			"id": int32(1),
			"in": map[string]any{"a": int32(2), "b": "x"},
			"m":  map[string]any{"k": int64(3)},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rec := opencdc.Record{Operation: opencdc.OperationCreate, Metadata: opencdc.Metadata{}, Payload: opencdc.Change{After: opencdc.RawData(after)}}
		schema.AttachPayloadSchemaToRecord(rec, sch)
		return rec
	}
	t.Run("unchanged keeps the carried version", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) { return r, nil })
		in := fixture(t)
		want := in.Clone()
		out := mustSingle(t, processOne(t, p, in))
		is.Equal(out.Metadata[opencdc.MetadataPayloadSchemaVersion], "1")
		is.Equal(out.Payload.After, want.Payload.After) // bytes changed
	})
	t.Run("added field keeps the carried definitions", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), setField("__op", "c"))
		out := mustSingle(t, processOne(t, p, fixture(t)))
		decoded, sch := decodeAttached(t, out, out.Payload.After)
		_, carriedDefs, _ := schemaFields(t, schema.Schema{Bytes: []byte(text)})
		_, defs, order := schemaFields(t, sch)
		is.Equal(order, []string{"id", "in", "m", "__op"})
		for _, name := range []string{"id", "in", "m"} {
			is.Equal(defs[name], carriedDefs[name]) // carried field definition changed
		}
		is.Equal(sch.Version, 2)
		is.Equal(decoded["__op"], "c")
		want := map[string]any{"id": int32(1), "in": map[string]any{"a": int32(2), "b": "x"}, "m": map[string]any{"k": int64(3)}, "__op": "c"}
		if path, ok := equivalentValues(want, map[string]any(decoded), ""); !ok {
			t.Fatalf("decoded value differs at %q: %v", path, decoded)
		}
	})
}

// TestSchemaEncodeExtend_RawPartFailsClosed is the regression test for a
// payload part the processor left as non-empty raw bytes next to a reshaped
// structured part. The raw bytes were written with the carried schema, so
// attaching the extended version would make them decode wrongly (or, for a
// removed leading field, into shifted values without an error). The record
// fails instead, and nothing is registered.
func TestSchemaEncodeExtend_RawPartFailsClosed(t *testing.T) {
	cases := []struct {
		name    string
		rawPart string
		f       func(raw opencdc.RawData) func(context.Context, opencdc.Record) (opencdc.Record, error)
	}{
		{"raw before, after with a removed field", partPayloadBefore, func(raw opencdc.RawData) func(context.Context, opencdc.Record) (opencdc.Record, error) {
			return func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				r.Payload.Before = raw
				delete(r.Payload.After.(opencdc.StructuredData), "id")
				return r, nil
			}
		}},
		{"raw before, after with an added field", partPayloadBefore, func(raw opencdc.RawData) func(context.Context, opencdc.Record) (opencdc.Record, error) {
			return func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				r.Payload.Before = raw
				r.Payload.After.(opencdc.StructuredData)["__op"] = "u"
				return r, nil
			}
		}},
		{"raw after, before with an added field", partPayloadAfter, func(raw opencdc.RawData) func(context.Context, opencdc.Record) (opencdc.Record, error) {
			return func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				r.Payload.Before.(opencdc.StructuredData)["__op"] = "u"
				r.Payload.After = raw
				return r, nil
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			in := extendFixture(t)
			in.Operation = opencdc.OperationUpdate
			raw := opencdc.RawData(bytes.Clone(in.Payload.After.(opencdc.RawData)))
			in.Payload.Before = opencdc.RawData(bytes.Clone(raw))
			p := newExtendProcessor(t, extendPayloadCfg(), tc.f(raw))

			pr := processOne(t, p, in)
			mustErrorIs(t, pr, ErrSchemaExtendMismatch)
			is.True(strings.Contains(pr.(ErrorRecord).Error.Error(), tc.rawPart)) // error names the raw part
			_, err := sdkschema.Get(context.Background(), fidelitySubject(t)+"-value", 2)
			is.True(errors.Is(err, sdkschema.ErrVersionNotFound)) // a schema was registered for a failing record
		})
	}
}

func TestSchemaEncodeExtend_Key(t *testing.T) {
	is := is.New(t)
	cfg := config.Config{configProcessorSchemaEncodeKeyStrategy: string(SchemaEncodeStrategyExtend)}
	p := newExtendProcessor(t, cfg, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		r.Key.(opencdc.StructuredData)["tenant"] = "acme"
		return r, nil
	})
	out := mustSingle(t, processOne(t, p, extendFixture(t)))

	v, err := out.Metadata.GetKeySchemaVersion()
	is.NoErr(err)
	subject, err := out.Metadata.GetKeySchemaSubject()
	is.NoErr(err)
	sch, err := sdkschema.Get(context.Background(), subject, v)
	is.NoErr(err)
	types, _, order := schemaFields(t, sch)
	is.Equal(order, []string{"id", "tenant"})
	is.Equal(types["id"], `"int"`)
	is.Equal(types["tenant"], `["null","string"]`)

	// the payload strategy is independent and stays infer: unchanged payload
	// keeps its schema
	is.Equal(out.Metadata[opencdc.MetadataPayloadSchemaVersion], "1")
}

// TestSchemaEncodeExtend_Deterministic checks that records with the same
// added fields produce the same schema text, so they share one version.
func TestSchemaEncodeExtend_Deterministic(t *testing.T) {
	is := is.New(t)
	p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		after := r.Payload.After.(opencdc.StructuredData)
		after["__op"] = "c"
		after["__source"] = "pg"
		return r, nil
	})
	in := extendFixture(t)
	got := p.Process(context.Background(), []opencdc.Record{in.Clone(), in.Clone()})
	is.Equal(len(got), 2)
	a, b := mustSingle(t, got[0]), mustSingle(t, got[1])
	is.Equal(a.Metadata[opencdc.MetadataPayloadSchemaVersion], b.Metadata[opencdc.MetadataPayloadSchemaVersion])
	_, sch := decodeAttached(t, a, a.Payload.After)
	_, _, order := schemaFields(t, sch)
	is.Equal(order[len(order)-2:], []string{"__op", "__source"}) // added fields in name order
}

// failingCreateService fails every CreateSchema call.
type failingCreateService struct{ pprocutils.SchemaService }

var errRegistryDown = errors.New("registry down")

func (failingCreateService) CreateSchema(context.Context, pprocutils.CreateSchemaRequest) (pprocutils.CreateSchemaResponse, error) {
	return pprocutils.CreateSchemaResponse{}, errRegistryDown
}

func TestSchemaEncodeExtend_RegistryFailure(t *testing.T) {
	rec := extendFixture(t) // registered before the registry fails
	orig := sdkschema.SchemaService
	sdkschema.SchemaService = failingCreateService{orig}
	t.Cleanup(func() { sdkschema.SchemaService = orig })

	p := newExtendProcessor(t, extendPayloadCfg(), setField("__op", "c"))
	mustErrorIs(t, processOne(t, p, rec), errRegistryDown)
}

func TestExtendSchema_Unsupported(t *testing.T) {
	part := []extendPart{{name: partPayloadAfter, data: opencdc.StructuredData{"a": 1, "b": "x"}}}
	cases := []struct {
		name string
		sch  schema.Schema
	}{
		{"not a record", schema.Schema{Type: schema.TypeAvro, Bytes: []byte(`{"type":"map","values":"long"}`)}},
		{"not avro", schema.Schema{Type: schema.TypeProtobuf, Bytes: []byte(`syntax = "proto3";`)}},
		{"references", schema.Schema{
			Type: schema.TypeAvro, Bytes: []byte(`{"type":"record","name":"r","fields":[{"name":"a","type":"long"}]}`),
			References: []schema.Reference{{Name: "x", Subject: "x", Version: 1}},
		}},
		{
			// field c defines the named type T that field a refers to; the
			// processor removed c, so the extended schema has a dangling name
			"named type defined in a removed field",
			schema.Schema{Type: schema.TypeAvro, Bytes: []byte(`{"type":"record","name":"r","fields":[` +
				`{"name":"c","type":{"type":"enum","name":"T","symbols":["X"]}},` +
				`{"name":"a","type":"long"},{"name":"b","type":["null","T"]}]}`)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := extendSchema(tc.sch, part)
			if !errors.Is(err, ErrSchemaExtendUnsupported) {
				t.Fatalf("expected ErrSchemaExtendUnsupported, got %v", err)
			}
		})
	}
}

func TestSchemaEncodeStrategy_Config(t *testing.T) {
	t.Run("specification", func(t *testing.T) {
		is := is.New(t)
		p := ProcessorWithMiddleware(NewProcessorFunc(Specification{Name: "extend"}, nil), DefaultProcessorMiddleware()...)
		spec, err := p.Specification()
		is.NoErr(err)
		for _, name := range []string{configProcessorSchemaEncodePayloadStrategy, configProcessorSchemaEncodeKeyStrategy} {
			param, ok := spec.Parameters[name]
			is.True(ok)
			is.Equal(param.Default, string(SchemaEncodeStrategyInfer))
			is.Equal(param.Type, config.ParameterTypeString)
			is.Equal(param.Validations, []config.Validation{config.ValidationInclusion{List: []string{"infer", "extend"}}})
		}
	})
	t.Run("invalid value", func(t *testing.T) {
		is := is.New(t)
		p := ProcessorWithMiddleware(NewProcessorFunc(Specification{Name: "extend"}, nil), DefaultProcessorMiddleware()...)
		err := p.Configure(context.Background(), config.Config{configProcessorSchemaEncodePayloadStrategy: "widen"})
		is.True(err != nil)
		is.True(strings.Contains(err.Error(), configProcessorSchemaEncodePayloadStrategy))
	})
	t.Run("invalid processor default", func(t *testing.T) {
		is := is.New(t)
		p := ProcessorWithMiddleware(NewProcessorFunc(Specification{Name: "extend"}, nil), DefaultProcessorMiddleware(
			ProcessorWithSchemaEncodeConfig{KeyStrategy: "widen"},
		)...)
		err := p.Configure(context.Background(), config.Config{})
		is.True(err != nil)
		is.True(strings.Contains(err.Error(), configProcessorSchemaEncodeKeyStrategy))
	})
	t.Run("settings are consumed by the middleware", func(t *testing.T) {
		is := is.New(t)
		var seen config.Config
		inner := &configCapture{seen: &seen}
		p := ProcessorWithMiddleware(inner, DefaultProcessorMiddleware()...)
		err := p.Configure(context.Background(), config.Config{
			configProcessorSchemaEncodePayloadStrategy: "extend",
			configProcessorSchemaEncodeKeyStrategy:     "infer",
			"other":                                    "x",
		})
		is.NoErr(err)
		is.Equal(seen, config.Config{"other": "x"})
	})
	t.Run("processor default via middleware options", func(t *testing.T) {
		is := is.New(t)
		f := NewProcessorFunc(Specification{Name: "extend"}, setField("__op", "c"))
		p := ProcessorWithMiddleware(f, DefaultProcessorMiddleware(
			ProcessorWithSchemaEncodeConfig{PayloadStrategy: SchemaEncodeStrategyExtend},
		)...)
		is.NoErr(p.Configure(context.Background(), config.Config{}))
		out := mustSingle(t, processOne(t, p, extendFixture(t)))
		_, sch := decodeAttached(t, out, out.Payload.After)
		types, _, _ := schemaFields(t, sch)
		is.Equal(types["id"], `"int"`)
	})
}

// configCapture records the config its Configure receives.
type configCapture struct {
	UnimplementedProcessor
	seen *config.Config
}

func (c *configCapture) Specification() (Specification, error) {
	return Specification{Name: "capture"}, nil
}

func (c *configCapture) Configure(_ context.Context, cfg config.Config) error {
	*c.seen = cfg
	return nil
}
