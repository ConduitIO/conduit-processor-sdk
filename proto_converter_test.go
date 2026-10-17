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
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	opencdcv2 "github.com/conduitio/conduit-commons/proto/opencdc/v2"
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"github.com/matryer/is"
)

// unconvertible returns a record that conduit-commons' ToProto refuses: a
// time.Time in structured data is not a type google.protobuf.Struct can hold.
// (From conduit-commons v0.9.0, an int64 above 2^53-1 is refused the same way.)
func unconvertible() opencdc.Record {
	return opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"t": time.Now()}}}
}

func good(id string) opencdc.Record {
	return opencdc.Record{Position: opencdc.Position(id), Payload: opencdc.Change{After: opencdc.RawData(id)}}
}

// TestProtoConverter_ProcessedRecords_ConversionErrorAtItsIndex is the
// regression test for ConduitIO/conduit#3127 on the guest side: one result
// that cannot be converted used to fail the whole Process command, which the
// host turned into a single ErrorRecord for the first input record. Now it is
// an ErrorRecord at its own index and the other results are delivered.
func TestProtoConverter_ProcessedRecords_ConversionErrorAtItsIndex(t *testing.T) {
	is := is.New(t)
	in := []ProcessedRecord{
		SingleRecord(good("0")),
		SingleRecord(unconvertible()),
		FilterRecord{},
		MultiRecord{good("3a"), unconvertible()},
		ErrorRecord{Error: errors.New("processor error")},
		SingleRecord(good("5")),
	}

	out := protoConverter{}.processedRecords(in, false)
	is.Equal(len(out), len(in))

	is.Equal(string(out[0].GetSingleRecord().GetPosition()), "0")

	errRec := out[1].GetErrorRecord()
	is.True(errRec != nil)
	is.True(strings.HasPrefix(errRec.GetError().GetMessage(), "failed to convert processed record 1: "))

	is.True(out[2].GetFilterRecord() != nil)

	// A MultiRecord with one bad record fails as a whole, at its own index:
	// the input record it came from is nacked, and no partial fan-out is sent.
	errRec = out[3].GetErrorRecord()
	is.True(errRec != nil)
	is.True(strings.HasPrefix(errRec.GetError().GetMessage(), "failed to convert processed record 3: "))

	is.Equal(out[4].GetErrorRecord().GetError().GetMessage(), "processor error")

	is.Equal(string(out[5].GetSingleRecord().GetPosition()), "5")
}

func TestProtoConverter_ProcessedRecords_Nil(t *testing.T) {
	is := is.New(t)
	is.Equal(protoConverter{}.processedRecords(nil, false), []*processorv1.Process_ProcessedRecord(nil))
}

// TestProtoConverter_SpecifyResponse_DeclaresStructuredValuesV2 is the
// capability test: every guest built against this SDK declares
// structured_values_v2, so the host will send it opencdc.v2 records (ADR
// 20261015-lossless-structured-values-protocol-v3.md, decision item 4).
func TestProtoConverter_SpecifyResponse_DeclaresStructuredValuesV2(t *testing.T) {
	is := is.New(t)

	resp := protoConverter{}.specifyResponse(Specification{Name: "x"})
	is.True(resp.GetStructuredValuesV2())
}

// TestProtoConverter_RecordsV2_RoundTrip pushes a record whose structured data
// holds values the v1 shape cannot carry — a wide int64, a full-range uint64,
// raw bytes, and an exact decimal — through the SDK's v2 toProto and fromProto
// wiring and asserts every value survives exactly. This is the lossless path
// the v0.21 v1 refusal (#3100) was the floor for.
func TestProtoConverter_RecordsV2_RoundTrip(t *testing.T) {
	is := is.New(t)
	c := protoConverter{}

	wideInt := int64(9007199254740993)         // 2^53 + 1, refused by the v1 encoder
	wideUint := uint64(18446744073709551615)   // max uint64
	rawBytes := []byte{0x00, 0x01, 0xfe, 0xff} // arbitrary bytes, base64-corrupted in v1
	exactDecimal := json.Number("1.234567890123456789")

	in := opencdc.Record{
		Key: opencdc.StructuredData{
			"wide":   wideInt,
			"u":      wideUint,
			"b":      rawBytes,
			"d":      exactDecimal,
			"nested": map[string]any{"wide": wideInt},
			"list":   []any{wideUint, rawBytes},
			"bool":   true,
			"null":   nil,
			"string": "s",
			"double": 1.5,
		},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{"wide": wideInt},
		},
	}

	// toProto in v2 mode.
	processed := c.processedRecords([]ProcessedRecord{SingleRecord(in)}, true)
	is.Equal(len(processed), 1)
	singleV2 := processed[0].GetSingleRecordV2()
	is.True(singleV2 != nil)

	// fromProto in v2 mode.
	records, err := c.recordsV2([]*opencdcv2.Record{singleV2})
	is.NoErr(err)
	is.Equal(len(records), 1)
	got := records[0]

	key := got.Key.(opencdc.StructuredData)
	is.Equal(key["wide"], wideInt)
	is.Equal(key["u"], wideUint)
	is.Equal(key["b"], rawBytes)
	is.Equal(key["d"], exactDecimal)
	is.Equal(key["nested"], map[string]any{"wide": wideInt})
	is.Equal(key["list"], []any{wideUint, rawBytes})
	is.Equal(key["bool"], true)
	is.Equal(key["null"], nil)
	is.Equal(key["string"], "s")
	is.Equal(key["double"], 1.5)

	after := got.Payload.After.(opencdc.StructuredData)
	is.Equal(after["wide"], wideInt)
}
