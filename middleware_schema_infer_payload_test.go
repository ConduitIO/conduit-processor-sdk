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
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	sdkschema "github.com/conduitio/conduit-processor-sdk/schema"
	"github.com/matryer/is"
)

// nullableRowSchema has the shape of a Postgres CDC payload schema: a non-null
// key column and nullable columns typed ["null", T], as the Postgres connector
// attaches them.
const nullableRowSchema = `{"type":"record","name":"items_payload","fields":[` +
	`{"name":"id","type":"int"},` +
	`{"name":"qty","type":["null","int"],"default":null},` +
	`{"name":"note","type":["null","string"],"default":null},` +
	`{"name":"big","type":["null","string"],"default":null}]}`

// nullableRowRecord registers nullableRowSchema under subject and returns an
// update record whose before and after are Avro bytes of before and after,
// with the schema attached, as the Postgres source produces it.
func nullableRowRecord(t *testing.T, subject string, before, after opencdc.StructuredData) opencdc.Record {
	t.Helper()
	sch, err := sdkschema.Create(context.Background(), schema.TypeAvro, subject, []byte(nullableRowSchema))
	if err != nil {
		t.Fatal(err)
	}
	encode := func(d opencdc.StructuredData) opencdc.RawData {
		b, err := sch.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	rec := opencdc.Record{
		Operation: opencdc.OperationUpdate,
		Metadata:  opencdc.Metadata{},
		Payload:   opencdc.Change{Before: encode(before), After: encode(after)},
	}
	schema.AttachPayloadSchemaToRecord(rec, sch)
	return rec
}

// removeField returns a processor that deletes field from the given parts of
// the payload, as field.exclude does.
func removeField(field string, fromBefore, fromAfter bool) func(context.Context, opencdc.Record) (opencdc.Record, error) {
	return func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		if fromBefore {
			delete(r.Payload.Before.(opencdc.StructuredData), field)
		}
		if fromAfter {
			delete(r.Payload.After.(opencdc.StructuredData), field)
		}
		return r, nil
	}
}

// TestSchemaMiddleware_InferNullFlipKeepsBefore is the regression test for an
// update where a nullable column changes between a value and NULL and the
// processor reshapes the payload (the SMT oracle's items@7: qty 7 -> NULL,
// field.exclude). The infer strategy extracted the new schema from after
// alone, so a nil in after became ["null","string"] (or a value in after became
// a non-null type), and before could not be encoded with it: the record failed
// with "failed to encode before payload". Before and after must both come back
// with exactly the values the processor returned (invariant 6).
func TestSchemaMiddleware_InferNullFlipKeepsBefore(t *testing.T) {
	testCases := []struct {
		name          string
		before, after opencdc.StructuredData
	}{{
		name:   "value to null",
		before: opencdc.StructuredData{"id": int32(1), "qty": int32(7), "note": "n1", "big": "x"},
		after:  opencdc.StructuredData{"id": int32(1), "qty": nil, "note": nil, "big": "x"},
	}, {
		name:   "null to value",
		before: opencdc.StructuredData{"id": int32(1), "qty": nil, "note": nil, "big": "x"},
		after:  opencdc.StructuredData{"id": int32(1), "qty": int32(7), "note": "n1", "big": "x"},
	}, {
		name:   "null on both sides",
		before: opencdc.StructuredData{"id": int32(1), "qty": nil, "note": "n1", "big": "x"},
		after:  opencdc.StructuredData{"id": int32(1), "qty": nil, "note": "n2", "big": "x"},
	}}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			in := nullableRowRecord(t, fidelitySubject(t), tc.before, tc.after)
			p := newSchemaMiddlewareProcessor(t, removeField("big", true, true))

			got := p.Process(context.Background(), []opencdc.Record{in})
			is.Equal(len(got), 1)
			out, ok := got[0].(SingleRecord)
			if !ok {
				t.Fatalf("expected SingleRecord, got %T: %+v", got[0], got[0])
			}

			for _, part := range []struct {
				name string
				want opencdc.StructuredData
				data opencdc.Data
			}{
				{"before", tc.before, out.Payload.Before},
				{"after", tc.after, out.Payload.After},
			} {
				want := part.want.Clone().(opencdc.StructuredData)
				delete(want, "big")
				decoded, _ := decodeAttached(t, opencdc.Record(out), part.data)
				if path, ok := equivalentValues(map[string]any(want), map[string]any(decoded), ""); !ok {
					t.Fatalf("%s changed at %q: got %#v, want %#v", part.name, path, decoded, want)
				}
			}
		})
	}
}

// TestSchemaMiddleware_InferFieldOnlyInBefore: a processor that removes a
// field from after but leaves it in before (field.exclude on
// .Payload.After.x of an update). One schema describes both parts, and the
// schema inferred from after lacks the field, so the Avro encoder would drop
// it from before without an error. The record fails with ErrFieldNotInSchema
// instead (invariant 6); before is never silently changed.
func TestSchemaMiddleware_InferFieldOnlyInBefore(t *testing.T) {
	is := is.New(t)
	row := opencdc.StructuredData{"id": int32(1), "qty": int32(7), "note": "n1", "big": "x"}
	in := nullableRowRecord(t, fidelitySubject(t), row, row)
	p := newSchemaMiddlewareProcessor(t, removeField("big", false, true))

	got := p.Process(context.Background(), []opencdc.Record{in})
	is.Equal(len(got), 1)
	errRec, ok := got[0].(ErrorRecord)
	if !ok {
		t.Fatalf("expected ErrorRecord, got %T: %+v", got[0], got[0])
	}
	is.True(errors.Is(errRec.Error, ErrFieldNotInSchema))
	is.True(strings.Contains(errRec.Error.Error(), "before payload"))
	is.True(strings.Contains(errRec.Error.Error(), `"big"`))
}

// TestSchemaMiddleware_InferNullFieldOnlyInAfter: a processor that adds a nil
// field to after only. Its inferred type is nullable, so the encoder would
// write before with that field as null, adding a field before never had. The
// record fails instead (invariant 6).
func TestSchemaMiddleware_InferNullFieldOnlyInAfter(t *testing.T) {
	is := is.New(t)
	row := opencdc.StructuredData{"id": int32(1), "qty": int32(7), "note": "n1", "big": "x"}
	in := nullableRowRecord(t, fidelitySubject(t), row, row)
	p := newSchemaMiddlewareProcessor(t, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		r.Payload.After.(opencdc.StructuredData)["added"] = nil
		return r, nil
	})

	got := p.Process(context.Background(), []opencdc.Record{in})
	is.Equal(len(got), 1)
	errRec, ok := got[0].(ErrorRecord)
	if !ok {
		t.Fatalf("expected ErrorRecord, got %T: %+v", got[0], got[0])
	}
	is.True(strings.Contains(errRec.Error.Error(), "before payload"))
	is.True(strings.Contains(errRec.Error.Error(), "added"))
}
