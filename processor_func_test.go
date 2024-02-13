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
	"context"
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestProcessorFunc_Process_Success(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	proc := NewProcessorFunc(
		Specification{Name: "test"},
		func(_ context.Context, record opencdc.Record) (opencdc.Record, error) {
			record.Metadata = opencdc.Metadata{"foo": "bar"}
			return record, nil
		},
	)

	// process 2 records
	processedRecords := proc.Process(ctx, []opencdc.Record{{}, {}})

	is.Equal(len(processedRecords), 2)
	for _, rec := range processedRecords {
		got, ok := rec.(SingleRecord)
		is.True(ok)
		is.Equal(got.Metadata, opencdc.Metadata{"foo": "bar"})
	}
}

func TestProcessorFunc_Process_Error(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	wantErr := errors.New("test error")

	proc := NewProcessorFunc(
		Specification{Name: "test"},
		func(context.Context, opencdc.Record) (opencdc.Record, error) {
			return opencdc.Record{}, wantErr
		},
	)

	// process 2 records
	processedRecords := proc.Process(ctx, []opencdc.Record{{}, {}})

	is.Equal(len(processedRecords), 1)
	gotErr, ok := processedRecords[0].(ErrorRecord)
	is.True(ok)
	is.Equal(gotErr.Error, wantErr)
}
