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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	sdkschema "github.com/conduitio/conduit-processor-sdk/schema"
	"github.com/matryer/is"
)

// TestSchemaEncode_ChangedSubjectDropsField is the regression test for the
// encode middleware silently dropping fields when a processor attaches a
// different schema subject or version (invariant 6). The Avro encoder ignores
// map keys that are not fields of the record schema, so the record must fail
// instead of leaving without them.
func TestSchemaEncode_ChangedSubjectDropsField(t *testing.T) {
	const target = `{"type":"record","name":"Out","namespace":"sdk.test","fields":[` +
		`{"name":"id","type":"long"},` +
		`{"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},` +
		`{"name":"inner","type":{"type":"record","name":"Inner","fields":[{"name":"n","type":"int"}]}},` +
		`{"name":"items","type":{"type":"array","items":{"type":"record","name":"Item","fields":[{"name":"q","type":"int"}]}}},` +
		`{"name":"opt","type":["null",{"type":"record","name":"Opt","fields":[{"name":"s","type":"string"}]}]},` +
		`{"name":"attrs","type":{"type":"map","values":{"type":"record","name":"Attr","fields":[{"name":"v","type":"string"}]}}},` +
		`{"name":"free","type":{"type":"map","values":"string"}}]}`
	valid := func() opencdc.StructuredData {
		return opencdc.StructuredData{
			"id":    int64(1),
			"at":    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			"inner": map[string]any{"n": 1},
			"items": []any{map[string]any{"q": 2}},
			"opt":   nil,
			"attrs": map[string]any{"a": map[string]any{"v": "x"}},
			"free":  map[string]any{"any": "key"},
		}
	}

	testCases := []struct {
		name string
		edit func(opencdc.StructuredData)
		// dropped is the path of the field that must fail the record, or ""
		// if the record must be encoded.
		dropped string
	}{
		{"fits", func(opencdc.StructuredData) {}, ""},
		{"new keys in a map type are fine", func(d opencdc.StructuredData) {
			d["free"] = map[string]any{"x": "1", "y": "2"}
		}, ""},
		{"nullable record set, named form", func(d opencdc.StructuredData) {
			d["opt"] = map[string]any{"sdk.test.Opt": map[string]any{"s": "v"}}
		}, ""},
		{"times are still truncated, not failed", func(d opencdc.StructuredData) {
			d["at"] = time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
		}, ""},
		{"top-level field", func(d opencdc.StructuredData) { d["extra"] = "lost" }, "extra"},
		{"nested record field", func(d opencdc.StructuredData) {
			d["inner"] = map[string]any{"n": 1, "extra": true}
		}, "inner.extra"},
		{"record field inside an array", func(d opencdc.StructuredData) {
			d["items"] = []any{map[string]any{"q": 2}, map[string]any{"q": 3, "extra": 1}}
		}, "items[1].extra"},
		{"record field inside a nullable union, named form", func(d opencdc.StructuredData) {
			d["opt"] = map[string]any{"sdk.test.Opt": map[string]any{"s": "v", "extra": 1}}
		}, "opt.extra"},
		{"record field in a map value", func(d opencdc.StructuredData) {
			d["attrs"] = map[string]any{"a": map[string]any{"v": "x", "extra": 1}}
		}, "attrs.a.extra"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			subject := strings.NewReplacer("/", ".", " ", "_").Replace(t.Name())

			in, err := sdkschema.Create(ctx, schema.TypeAvro, subject+".in",
				[]byte(`{"type":"record","name":"In","fields":[{"name":"id","type":"long"}]}`))
			is.NoErr(err)
			out, err := sdkschema.Create(ctx, schema.TypeAvro, subject+".out", []byte(target))
			is.NoErr(err)
			raw, err := in.Marshal(opencdc.StructuredData{"id": int64(1)})
			is.NoErr(err)

			for _, kind := range []string{"key", "payload"} {
				p := ProcessorWithMiddleware(
					NewProcessorFunc(Specification{Name: "fields"}, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
						d := valid()
						tc.edit(d)
						if kind == "key" {
							r.Key = d
							schema.AttachKeySchemaToRecord(r, out)
						} else {
							r.Payload.After = d
							schema.AttachPayloadSchemaToRecord(r, out)
						}
						return r, nil
					}),
					DefaultProcessorMiddleware()...,
				)
				is.NoErr(p.Configure(ctx, config.Config{}))

				rec := opencdc.Record{Metadata: opencdc.Metadata{}}
				if kind == "key" {
					rec.Key = opencdc.RawData(raw)
					schema.AttachKeySchemaToRecord(rec, in)
				} else {
					rec.Payload.After = opencdc.RawData(raw)
					schema.AttachPayloadSchemaToRecord(rec, in)
				}

				got := p.Process(ctx, []opencdc.Record{rec})
				is.Equal(len(got), 1)
				if tc.dropped == "" {
					if _, ok := got[0].(SingleRecord); !ok {
						t.Fatalf("%s: expected SingleRecord, got %#v", kind, got[0])
					}
					continue
				}
				errRec, ok := got[0].(ErrorRecord)
				if !ok {
					t.Fatalf("%s: field %q would be dropped silently, expected ErrorRecord, got %T", kind, tc.dropped, got[0])
				}
				if !errors.Is(errRec.Error, ErrFieldNotInSchema) {
					t.Fatalf("%s: expected ErrFieldNotInSchema, got %v", kind, errRec.Error)
				}
				if !strings.Contains(errRec.Error.Error(), `"`+tc.dropped+`"`) {
					t.Fatalf("%s: error does not name field %q: %v", kind, tc.dropped, errRec.Error)
				}
			}
		})
	}
}

func TestCheckNoDroppedFields(t *testing.T) {
	const avsc = `{"type":"record","name":"T","namespace":"sdk.test","fields":[` +
		`{"name":"a","type":{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}},` +
		`{"name":"b","type":"R"},` +
		`{"name":"u","type":["null","string",{"type":"record","name":"U","fields":[{"name":"x","type":"int"}]}]}]}`
	sch := schema.Schema{Subject: "check", Version: 1, Type: schema.TypeAvro, Bytes: []byte(avsc)}
	testCases := []struct {
		name    string
		data    any
		dropped string
	}{
		{"nil", nil, ""},
		{"fits", map[string]any{"a": map[string]any{"n": 1}, "b": map[string]any{"n": 2}, "u": nil}, ""},
		{"through a named type reference", map[string]any{"a": map[string]any{"n": 1}, "b": map[string]any{"n": 2, "z": 0}}, "b.z"},
		{"union with several branches, named form", map[string]any{"u": map[string]any{"sdk.test.U": map[string]any{"x": 1, "z": 0}}}, "u.z"},
		// the codec picks the branch of a bare value, so nothing is claimed
		{"union with several branches, bare value", map[string]any{"u": map[string]any{"x": 1, "z": 0}}, ""},
		{"StructuredData", opencdc.StructuredData{"zz": 1}, "zz"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkNoDroppedFields(sch, tc.data)
			switch {
			case tc.dropped == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.dropped != "" && !errors.Is(err, ErrFieldNotInSchema):
				t.Fatalf("expected ErrFieldNotInSchema for %q, got %v", tc.dropped, err)
			case tc.dropped != "" && !strings.Contains(err.Error(), `"`+tc.dropped+`"`):
				t.Fatalf("error does not name %q: %v", tc.dropped, err)
			}
		})
	}

	t.Run("non-Avro schema is not checked", func(t *testing.T) {
		if err := checkNoDroppedFields(schema.Schema{Type: schema.TypeProtobuf}, map[string]any{"x": 1}); err != nil {
			t.Fatal(err)
		}
	})
}

// TestCheckNoDroppedFields_TypedContainers: the encoder accepts typed Go
// slices and string-keyed maps for Avro arrays and maps of records, and drops
// unknown keys of the records inside them just as it does for []any and
// map[string]any. The walk must descend into them too.
func TestCheckNoDroppedFields_TypedContainers(t *testing.T) {
	const rec = `{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}`
	extra := func() map[string]any { return map[string]any{"a": int64(1), "x": 1} }
	testCases := []struct {
		name, field string
		value       any
		dropped     string
	}{
		{"[]map[string]any of a record", `{"type":"array","items":` + rec + `}`, []map[string]any{extra()}, "v[0].x"},
		{"[]StructuredData of a record", `{"type":"array","items":` + rec + `}`, []opencdc.StructuredData{extra()}, "v[0].x"},
		{"map[string]map[string]any of a record", `{"type":"map","values":` + rec + `}`, map[string]map[string]any{"k": extra()}, "v.k.x"},
		{"map[string]StructuredData of a record", `{"type":"map","values":` + rec + `}`, map[string]opencdc.StructuredData{"k": extra()}, "v.k.x"},
		{"typed slice nested in []any", `{"type":"array","items":{"type":"array","items":` + rec + `}}`, []any{[]map[string]any{extra()}}, "v[0][0].x"},
		{"typed slice in a nullable array", `["null",{"type":"array","items":` + rec + `}]`, []map[string]any{extra()}, "v[0].x"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			sch := schema.Schema{
				Subject: "typed", Version: 1, Type: schema.TypeAvro,
				Bytes: []byte(`{"type":"record","name":"P","namespace":"sdk.test","fields":[{"name":"v","type":` + tc.field + `}]}`),
			}
			data := opencdc.StructuredData{"v": tc.value}

			// the encoder accepts the value and drops the key: the premise
			encoded, err := sch.Marshal(data)
			is.NoErr(err)
			var decoded opencdc.StructuredData
			is.NoErr(sch.Unmarshal(encoded, &decoded))
			is.True(!strings.Contains(fmt.Sprintf("%v", decoded), "x:"))

			err = checkNoDroppedFields(sch, data)
			if !errors.Is(err, ErrFieldNotInSchema) {
				t.Fatalf("expected ErrFieldNotInSchema for %q, got %v", tc.dropped, err)
			}
			is.True(strings.Contains(err.Error(), `"`+tc.dropped+`"`))
		})
	}
}

// TestCheckNoDroppedFields_UnionShortName: a union branch given by its short
// name is not a dropped field; the codec rejects it with its own error, so the
// walk must not report the branch name as a field.
func TestCheckNoDroppedFields_UnionShortName(t *testing.T) {
	sch := schema.Schema{
		Subject: "short", Version: 1, Type: schema.TypeAvro,
		Bytes: []byte(`{"type":"record","name":"P","namespace":"sdk.test","fields":[{"name":"v","type":["null",{"type":"record","name":"Opt","fields":[{"name":"s","type":"string"}]}]}]}`),
	}
	data := opencdc.StructuredData{"v": map[string]any{"Opt": map[string]any{"s": "x"}}}
	if err := checkNoDroppedFields(sch, data); err != nil {
		t.Fatalf("expected the codec to report this, got %v", err)
	}
	if _, err := sch.Marshal(data); err == nil {
		t.Fatal("expected the codec to reject a union branch given by its short name")
	}
}
