// Copyright © 2024 Meroxa, Inc.
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
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/goccy/go-json"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestProcessedRecord_MarshalJSON(t *testing.T) {
	testCases := []struct {
		name string
		have ProcessedRecord
		want string
	}{{
		name: "SingleRecord",
		have: SingleRecord{
			Operation: opencdc.OperationCreate,
			Position:  opencdc.Position("test-position"),
			Metadata:  opencdc.Metadata{"foo": "bar"},
			Key:       opencdc.RawData("test-key"),
			Payload: opencdc.Change{
				Before: opencdc.StructuredData{"foo": "before"},
				After:  opencdc.StructuredData{"foo": "after"},
			},
		},
		want: `{
  "position": "dGVzdC1wb3NpdGlvbg==",
  "operation": "create",
  "metadata": {
    "foo": "bar"
  },
  "key": "test-key",
  "payload": {
    "before": {
      "foo": "before"
    },
    "after": {
      "foo": "after"
    }
  }
}`,
	}, {
		name: "FilterRecord",
		have: FilterRecord{},
		want: `{}`,
	}, {
		name: "ErrorRecord",
		have: ErrorRecord{
			Error: errors.New("error"),
		},
		want: `{
  "error": "error"
}`,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()

			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			enc.SetIndent("", "  ")

			ctx = opencdc.WithJSONMarshalOptions(ctx, &opencdc.JSONMarshalOptions{RawDataAsString: true})
			err := enc.EncodeContext(ctx, tc.have)
			is.NoErr(err)

			got := buf.String()
			got = got[:len(got)-1] // remove trailing newline
			is.Equal("", cmp.Diff(tc.want, got))
		})
	}
}
