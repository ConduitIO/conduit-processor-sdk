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

	"github.com/conduitio/conduit-commons/opencdc"
)

// ProcessorFunc is an adapter allowing use of a function as a Processor.
type ProcessorFunc struct {
	UnimplementedProcessor

	specs Specification
	f     func(context.Context, opencdc.Record) (opencdc.Record, error)
}

var _ Processor = ProcessorFunc{} // Ensure ProcessorFunc implements Processor

// NewProcessorFunc creates a ProcessorFunc from a function and specifications.
// This is useful for creating simple processors without needing to implement
// the full Processor interface.
func NewProcessorFunc(specs Specification, f func(context.Context, opencdc.Record) (opencdc.Record, error)) ProcessorFunc {
	return ProcessorFunc{
		specs: specs,
		f:     f,
	}
}

func (f ProcessorFunc) Specification() (Specification, error)            { return f.specs, nil }
func (ProcessorFunc) Configure(context.Context, map[string]string) error { return nil }
func (ProcessorFunc) Open(context.Context) error {
	return nil
}

func (f ProcessorFunc) Process(ctx context.Context, records []opencdc.Record) []ProcessedRecord {
	outRecs := make([]ProcessedRecord, len(records))
	for i, inRec := range records {
		outRec, err := f.f(ctx, inRec)
		switch {
		case errors.Is(err, ErrFilterRecord):
			outRecs[i] = FilterRecord{}
		case err != nil:
			outRecs[i] = ErrorRecord{Error: err}
		default:
			outRecs[i] = SingleRecord(outRec)
		}
	}
	return outRecs
}

func (ProcessorFunc) Teardown(context.Context) error {
	return nil
}
