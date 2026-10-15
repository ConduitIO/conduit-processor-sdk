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
	"strconv"
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-commons/schema/avro"
	sdkschema "github.com/conduitio/conduit-processor-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

// absentTestCase is an input record with an empty raw payload part and the record the
// processor is expected to see (decode) or emit (decode + encode).
type absentTestCase struct {
	name string
	in   opencdc.Record
	// wantDecoded is what the wrapped processor sees after schema decoding.
	wantDecoded opencdc.Change
	// wantEmitted is what the default middleware chain emits after a
	// pass-through processor, i.e. after decoding and re-encoding.
	wantEmitted opencdc.Change
}

// absentTestCases builds records whose payload.before or payload.after is a
// non-nil, zero-length RawData. Conduit does not send that shape today (its
// sanitizeRecord is a no-op), but nothing in the protocol rules it out.
// Decoding those zero bytes with the payload schema produced an invented
// all-null value such as {"id": nil, "name": nil} (invariant 6). Keys here
// carry no key schema; empty keys with a key schema are covered by
// TestDefaultProcessorMiddleware_EmptyKeyRoundTrip.
func absentTestCases(t *testing.T) []absentTestCase {
	t.Helper()
	ctx := context.Background()

	value := opencdc.StructuredData{"id": int64(1), "name": "alice"}
	srd, err := avro.SerdeForType(value)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := sdkschema.Create(ctx, schema.TypeAvro, t.Name(), []byte(srd.String()))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := sch.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	updated := opencdc.StructuredData{"id": int64(1), "name": "bob"}
	encodedUpdated, err := sch.Marshal(updated)
	if err != nil {
		t.Fatal(err)
	}

	md := func() opencdc.Metadata {
		return opencdc.Metadata{
			opencdc.MetadataPayloadSchemaSubject: sch.Subject,
			opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
		}
	}

	return []absentTestCase{{
		name: "create with empty raw before: before not decoded",
		in: opencdc.Record{
			Operation: opencdc.OperationCreate,
			Metadata:  md(),
			Key:       opencdc.RawData{},
			Payload:   opencdc.Change{Before: opencdc.RawData{}, After: opencdc.RawData(encoded)},
		},
		wantDecoded: opencdc.Change{Before: opencdc.RawData{}, After: value},
		wantEmitted: opencdc.Change{Before: opencdc.RawData{}, After: opencdc.RawData(encoded)},
	}, {
		name: "nil-slice raw data is absent too",
		in: opencdc.Record{
			Operation: opencdc.OperationSnapshot,
			Metadata:  md(),
			Key:       opencdc.RawData{},
			Payload:   opencdc.Change{Before: opencdc.RawData(nil), After: opencdc.RawData(encoded)},
		},
		wantDecoded: opencdc.Change{Before: opencdc.RawData(nil), After: value},
		wantEmitted: opencdc.Change{Before: opencdc.RawData(nil), After: opencdc.RawData(encoded)},
	}, {
		name: "delete with empty raw after: after not decoded",
		in: opencdc.Record{
			Operation: opencdc.OperationDelete,
			Metadata:  md(),
			Key:       opencdc.RawData{},
			Payload:   opencdc.Change{Before: opencdc.RawData(encoded), After: opencdc.RawData{}},
		},
		wantDecoded: opencdc.Change{Before: value, After: opencdc.RawData{}},
		wantEmitted: opencdc.Change{Before: opencdc.RawData(encoded), After: opencdc.RawData{}},
	}, {
		name: "update with a real before still decodes",
		in: opencdc.Record{
			Operation: opencdc.OperationUpdate,
			Metadata:  md(),
			Key:       opencdc.RawData{},
			Payload:   opencdc.Change{Before: opencdc.RawData(encoded), After: opencdc.RawData(encodedUpdated)},
		},
		wantDecoded: opencdc.Change{Before: value, After: updated},
		wantEmitted: opencdc.Change{Before: opencdc.RawData(encoded), After: opencdc.RawData(encodedUpdated)},
	}}
}

func TestProcessorWithSchemaDecode_Process_EmptyRawDataIsAbsent(t *testing.T) {
	ctx := context.Background()
	for _, tc := range absentTestCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			var seen opencdc.Record
			inner := NewProcessorFunc(Specification{Name: "seen"}, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				seen = r
				return r, nil
			})
			p := (&ProcessorWithSchemaDecode{}).Wrap(inner)
			is.NoErr(p.Configure(ctx, config.Config{}))

			out := p.Process(ctx, []opencdc.Record{tc.in.Clone()})
			is.Equal(len(out), 1)
			if errRec, ok := out[0].(ErrorRecord); ok {
				t.Fatalf("unexpected error record: %v", errRec.Error)
			}
			// cmp tells RawData{} from RawData(nil), so this also pins that
			// an absent part is passed on exactly as received.
			if diff := cmp.Diff(tc.wantDecoded, seen.Payload); diff != "" {
				t.Errorf("payload seen by processor mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.in.Key, seen.Key); diff != "" {
				t.Errorf("key seen by processor mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestDefaultProcessorMiddleware_EmptyRawDataIsAbsent runs the full default
// chain (decode, pass-through processor, encode). This is the mirror case on
// the encode side: an invented Before would be re-encoded into real bytes and
// sent downstream as if the record had always carried it.
func TestDefaultProcessorMiddleware_EmptyRawDataIsAbsent(t *testing.T) {
	ctx := context.Background()
	for _, tc := range absentTestCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			inner := NewProcessorFunc(Specification{Name: "passthrough"}, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				return r, nil
			})
			p := ProcessorWithMiddleware(inner, DefaultProcessorMiddleware()...)
			is.NoErr(p.Configure(ctx, config.Config{}))

			out := p.Process(ctx, []opencdc.Record{tc.in.Clone()})
			is.Equal(len(out), 1)
			got, ok := out[0].(SingleRecord)
			if !ok {
				t.Fatalf("expected SingleRecord, got %T: %+v", out[0], out[0])
			}
			if diff := cmp.Diff(tc.wantEmitted, got.Payload); diff != "" {
				t.Errorf("emitted payload mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestDefaultProcessorMiddleware_EmptyBeforeNullableSchema is the silent form
// of the bug: with nullable fields the invented all-null Before encodes
// cleanly, so before the fix the record left the processor with Before set to
// real bytes ({0x00, 0x00}, two null union branches) and no error.
func TestDefaultProcessorMiddleware_EmptyBeforeNullableSchema(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	sch, err := sdkschema.Create(ctx, schema.TypeAvro, t.Name(), []byte(`{
		"type": "record", "name": "row",
		"fields": [
			{"name": "id", "type": ["null", "long"]},
			{"name": "name", "type": ["null", "string"]}
		]}`))
	is.NoErr(err)
	encoded, err := sch.Marshal(opencdc.StructuredData{"id": int64(1), "name": "alice"})
	is.NoErr(err)

	inner := NewProcessorFunc(Specification{Name: "passthrough"}, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
		return r, nil
	})
	p := ProcessorWithMiddleware(inner, DefaultProcessorMiddleware()...)
	is.NoErr(p.Configure(ctx, config.Config{}))

	out := p.Process(ctx, []opencdc.Record{{
		Operation: opencdc.OperationCreate,
		Metadata: opencdc.Metadata{
			opencdc.MetadataPayloadSchemaSubject: sch.Subject,
			opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
		},
		Key:     opencdc.RawData{},
		Payload: opencdc.Change{Before: opencdc.RawData{}, After: opencdc.RawData(encoded)},
	}})
	is.Equal(len(out), 1)
	got, ok := out[0].(SingleRecord)
	if !ok {
		t.Fatalf("expected SingleRecord, got %T: %+v", out[0], out[0])
	}
	want := opencdc.Change{Before: opencdc.RawData{}, After: opencdc.RawData(encoded)}
	if diff := cmp.Diff(want, got.Payload); diff != "" {
		t.Errorf("emitted payload mismatch (-want +got):\n%s", diff)
	}
}

// TestDefaultProcessorMiddleware_EmptyKeyRoundTrip pins that a zero-length key
// is decoded and re-encoded unchanged, not skipped. Zero bytes is the valid
// encoding of a zero-field record and of a "null" schema; postgres emits
// StructuredData{} keys with a zero-field key schema for relations without a
// key. Treating such a key as absent would change what the processor sees
// and, downstream, the bytes a destination keys on (a Kafka key going from
// "{}" to empty moves the record to another partition, invariant 4).
func TestDefaultProcessorMiddleware_EmptyKeyRoundTrip(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name     string
		schema   string
		wantSeen opencdc.Data
	}{{
		name:     "zero-field record key schema",
		schema:   `{"type":"record","name":"key","fields":[]}`,
		wantSeen: opencdc.StructuredData{},
	}, {
		name:     "null key schema",
		schema:   `"null"`,
		wantSeen: opencdc.StructuredData(nil),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			sch, err := sdkschema.Create(ctx, schema.TypeAvro, t.Name(), []byte(tc.schema))
			is.NoErr(err)
			// the premise of the test: this key encodes to zero bytes
			b, err := sch.Marshal(opencdc.StructuredData{})
			is.NoErr(err)
			is.Equal(len(b), 0)

			var seen opencdc.Record
			inner := NewProcessorFunc(Specification{Name: "seen"}, func(_ context.Context, r opencdc.Record) (opencdc.Record, error) {
				seen = r
				return r, nil
			})
			p := ProcessorWithMiddleware(inner, DefaultProcessorMiddleware()...)
			is.NoErr(p.Configure(ctx, config.Config{}))

			in := opencdc.Record{
				Operation: opencdc.OperationCreate,
				Metadata: opencdc.Metadata{
					opencdc.MetadataKeySchemaSubject: sch.Subject,
					opencdc.MetadataKeySchemaVersion: strconv.Itoa(sch.Version),
				},
				Key:     opencdc.RawData(b),
				Payload: opencdc.Change{After: opencdc.RawData("not schema encoded")},
			}
			out := p.Process(ctx, []opencdc.Record{in})
			is.Equal(len(out), 1)
			got, ok := out[0].(SingleRecord)
			if !ok {
				t.Fatalf("expected SingleRecord, got %T: %+v", out[0], out[0])
			}
			if diff := cmp.Diff(tc.wantSeen, seen.Key); diff != "" {
				t.Errorf("key seen by processor mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(opencdc.RawData(b), got.Key); diff != "" {
				t.Errorf("emitted key mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
