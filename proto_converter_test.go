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
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
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

	out := protoConverter{}.processedRecords(in)
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
	is.Equal(protoConverter{}.processedRecords(nil), []*processorv1.Process_ProcessedRecord(nil))
}
