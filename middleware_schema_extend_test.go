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

func TestSchemaEncodeExtend_EmptyPartPassesThrough(t *testing.T) {
	t.Run("payload", func(t *testing.T) {
		is := is.New(t)
		p := newExtendProcessor(t, extendPayloadCfg(), func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
			r.Payload.After = opencdc.StructuredData{}
			return r, nil
		})
		in := extendFixture(t)
		out := mustSingle(t, processOne(t, p, in))
		is.Equal(out.Payload.After, opencdc.StructuredData{}) // empty part was encoded
		is.Equal(out.Metadata[opencdc.MetadataPayloadSchemaVersion], in.Metadata[opencdc.MetadataPayloadSchemaVersion])
		is.Equal(out.Metadata[opencdc.MetadataPayloadSchemaSubject], in.Metadata[opencdc.MetadataPayloadSchemaSubject])
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
		is.Equal(out.Key, opencdc.StructuredData{}) // empty key was encoded
		is.Equal(out.Metadata[opencdc.MetadataKeySchemaVersion], in.Metadata[opencdc.MetadataKeySchemaVersion])
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
	t.Run("only after changed", func(t *testing.T) {
		p := newExtendProcessor(t, extendPayloadCfg(), setField("__op", "u"))
		mustErrorIs(t, processOne(t, p, withBefore(t)), ErrSchemaExtendMismatch)
	})
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
