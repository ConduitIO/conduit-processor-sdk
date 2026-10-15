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
	"fmt"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

// Regression tests for conduit#2968: when record i of a batch fails in the
// schema middleware, the ErrorRecord must sit at index i, after results for
// records 0..i-1. Conduit matches results to inputs by index, so an
// ErrorRecord at index 0 nacks the wrong record.

// plainRecord has no schema metadata, so both middlewares pass it through.
func plainRecord(n int) opencdc.Record {
	return opencdc.Record{
		Position: opencdc.Position(fmt.Sprintf("pos-%d", n)),
		Key:      opencdc.RawData(fmt.Sprintf("key-%d", n)),
		Payload:  opencdc.Change{After: opencdc.RawData(fmt.Sprintf("after-%d", n))},
	}
}

// undecodablePayload points at a schema that does not exist, so payload
// decoding fails.
func undecodablePayload(n int) opencdc.Record {
	r := plainRecord(n)
	r.Metadata = opencdc.Metadata{
		opencdc.MetadataPayloadSchemaSubject: "conduit-2968-missing-subject",
		opencdc.MetadataPayloadSchemaVersion: "1",
	}
	return r
}

// undecodableKey is the same for the key.
func undecodableKey(n int) opencdc.Record {
	r := plainRecord(n)
	r.Metadata = opencdc.Metadata{
		opencdc.MetadataKeySchemaSubject: "conduit-2968-missing-subject",
		opencdc.MetadataKeySchemaVersion: "1",
	}
	return r
}

// passThrough returns each input as a SingleRecord and records how many
// records it was given.
func passThrough(got *[]int) func(context.Context, []opencdc.Record) []ProcessedRecord {
	return func(_ context.Context, recs []opencdc.Record) []ProcessedRecord {
		*got = append(*got, len(recs))
		out := make([]ProcessedRecord, len(recs))
		for i, r := range recs {
			out[i] = SingleRecord(r)
		}
		return out
	}
}

func assertErrorAt(t *testing.T, out []ProcessedRecord, want int, inputs []opencdc.Record) {
	t.Helper()
	is := is.New(t)
	is.Equal(len(out), want+1) // results for 0..want-1, then the error
	for j := 0; j < want; j++ {
		rec, ok := out[j].(SingleRecord)
		if !ok {
			t.Fatalf("result %d: got %T, want SingleRecord", j, out[j])
		}
		is.Equal(string(rec.Position), string(inputs[j].Position))
	}
	errRec, ok := out[want].(ErrorRecord)
	if !ok {
		t.Fatalf("result %d: got %T, want ErrorRecord", want, out[want])
	}
	if !strings.Contains(errRec.Error.Error(), fmt.Sprintf("record %d:", want)) {
		t.Fatalf("error %q does not name record %d", errRec.Error, want)
	}
}

func TestProcessorWithSchemaDecode_Process_ErrorAtFailingIndex(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name    string
		records []opencdc.Record
		wantErr int
		wantIn  []int // batch sizes the wrapped processor saw
	}{{
		name:    "payload fails at 1 of 3",
		records: []opencdc.Record{plainRecord(0), undecodablePayload(1), plainRecord(2)},
		wantErr: 1,
		wantIn:  []int{1},
	}, {
		name:    "key fails at 2 of 3",
		records: []opencdc.Record{plainRecord(0), plainRecord(1), undecodableKey(2)},
		wantErr: 2,
		wantIn:  []int{2},
	}, {
		// The old code decoded every key before any payload, so it reported
		// the key failure at 2 instead of the earlier payload failure at 1.
		name:    "payload fails at 1, key fails at 2",
		records: []opencdc.Record{plainRecord(0), undecodablePayload(1), undecodableKey(2)},
		wantErr: 1,
		wantIn:  []int{1},
	}, {
		name:    "fails at 0, wrapped processor not called",
		records: []opencdc.Record{undecodablePayload(0), plainRecord(1)},
		wantErr: 0,
		wantIn:  nil,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctrl := gomock.NewController(t)
			proc := NewMockProcessor(ctrl)
			d := (&ProcessorWithSchemaDecode{}).Wrap(proc)

			proc.EXPECT().Configure(ctx, gomock.Any()).Return(nil)
			is.NoErr(d.Configure(ctx, config.Config{}))

			var gotIn []int
			proc.EXPECT().Process(ctx, gomock.Any()).DoAndReturn(passThrough(&gotIn)).AnyTimes()

			inputs := make([]opencdc.Record, len(tc.records))
			copy(inputs, tc.records)
			out := d.Process(ctx, tc.records)

			assertErrorAt(t, out, tc.wantErr, inputs)
			is.Equal(gotIn, tc.wantIn)
		})
	}
}

// If the wrapped processor itself returns fewer results than it was given,
// the decode error must not be appended at a lower index. The engine retries
// everything past the returned results, the failing record included.
func TestProcessorWithSchemaDecode_Process_WrappedStopsEarly(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	proc := NewMockProcessor(ctrl)
	d := (&ProcessorWithSchemaDecode{}).Wrap(proc)

	proc.EXPECT().Configure(ctx, gomock.Any()).Return(nil)
	is.NoErr(d.Configure(ctx, config.Config{}))

	wrappedErr := ErrorRecord{Error: fmt.Errorf("wrapped failed on record 0")}
	proc.EXPECT().Process(ctx, gomock.Any()).DoAndReturn(
		func(_ context.Context, recs []opencdc.Record) []ProcessedRecord {
			is.Equal(len(recs), 2)
			return []ProcessedRecord{wrappedErr}
		},
	)

	out := d.Process(ctx, []opencdc.Record{plainRecord(0), plainRecord(1), undecodablePayload(2)})
	is.Equal(out, []ProcessedRecord{wrappedErr})
}

func TestProcessorWithSchemaEncode_Process_ErrorAtFailingIndex(t *testing.T) {
	ctx := context.Background()

	badKeyVersion := func(n int) opencdc.Record {
		r := plainRecord(n)
		r.Metadata = opencdc.Metadata{
			opencdc.MetadataKeySchemaSubject: "s",
			opencdc.MetadataKeySchemaVersion: "not-a-number",
		}
		return r
	}
	badPayloadVersion := func(n int) opencdc.Record {
		r := plainRecord(n)
		r.Metadata = opencdc.Metadata{
			opencdc.MetadataPayloadSchemaSubject: "s",
			opencdc.MetadataPayloadSchemaVersion: "not-a-number",
		}
		return r
	}

	testCases := []struct {
		name    string
		records []opencdc.Record
		wantErr int
		wantIn  []int
	}{{
		name:    "key metadata bad at 1 of 3",
		records: []opencdc.Record{plainRecord(0), badKeyVersion(1), plainRecord(2)},
		wantErr: 1,
		wantIn:  []int{1},
	}, {
		name:    "payload metadata bad at 2 of 3",
		records: []opencdc.Record{plainRecord(0), plainRecord(1), badPayloadVersion(2)},
		wantErr: 2,
		wantIn:  []int{2},
	}, {
		name:    "bad at 0, wrapped processor not called",
		records: []opencdc.Record{badKeyVersion(0), plainRecord(1)},
		wantErr: 0,
		wantIn:  nil,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctrl := gomock.NewController(t)
			proc := NewMockProcessor(ctrl)
			e := (&ProcessorWithSchemaEncode{}).Wrap(proc)

			proc.EXPECT().Configure(ctx, gomock.Any()).Return(nil)
			is.NoErr(e.Configure(ctx, config.Config{}))

			var gotIn []int
			proc.EXPECT().Process(ctx, gomock.Any()).DoAndReturn(passThrough(&gotIn)).AnyTimes()

			inputs := make([]opencdc.Record, len(tc.records))
			copy(inputs, tc.records)
			out := e.Process(ctx, tc.records)

			assertErrorAt(t, out, tc.wantErr, inputs)
			is.Equal(gotIn, tc.wantIn)
		})
	}
}

func TestProcessorWithSchemaEncode_Process_WrappedStopsEarly(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	proc := NewMockProcessor(ctrl)
	e := (&ProcessorWithSchemaEncode{}).Wrap(proc)

	proc.EXPECT().Configure(ctx, gomock.Any()).Return(nil)
	is.NoErr(e.Configure(ctx, config.Config{}))

	proc.EXPECT().Process(ctx, gomock.Any()).DoAndReturn(
		func(_ context.Context, recs []opencdc.Record) []ProcessedRecord {
			is.Equal(len(recs), 2)
			return []ProcessedRecord{SingleRecord(recs[0])}
		},
	)

	bad := plainRecord(2)
	bad.Metadata = opencdc.Metadata{
		opencdc.MetadataKeySchemaSubject: "s",
		opencdc.MetadataKeySchemaVersion: "not-a-number",
	}
	out := e.Process(ctx, []opencdc.Record{plainRecord(0), plainRecord(1), bad})
	is.Equal(len(out), 1)
	_, ok := out[0].(SingleRecord)
	is.True(ok)
}
