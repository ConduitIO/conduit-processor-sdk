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
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// batchProcessor is a test Processor that also implements BatchProcessor,
// returning one SingleRecord per input record and an ErrorRecord at the index
// of any record whose payload is the string "err".
type batchProcessor struct {
	UnimplementedProcessor

	// processBatchCalls counts ProcessBatch invocations so tests can assert the
	// batch surface was (or was not) taken.
	processBatchCalls int
	// processCalls counts Process invocations for the same reason.
	processCalls int
}

func (b *batchProcessor) Specification() (Specification, error) {
	return Specification{Name: "batch", Stateless: true}, nil
}

func (b *batchProcessor) Process(_ context.Context, records []opencdc.Record) []ProcessedRecord {
	b.processCalls++
	out := make([]ProcessedRecord, len(records))
	for i, r := range records {
		out[i] = SingleRecord(r)
	}
	return out
}

func (b *batchProcessor) ProcessBatch(_ context.Context, records []opencdc.Record) ([]ProcessedRecord, error) {
	b.processBatchCalls++
	out := make([]ProcessedRecord, len(records))
	for i, r := range records {
		if string(r.Payload.After.Bytes()) == "err" {
			out[i] = ErrorRecord{Error: errors.New("boom")}
			continue
		}
		out[i] = SingleRecord(r)
	}
	return out, nil
}

func rec(payload string) opencdc.Record {
	return opencdc.Record{Payload: opencdc.Change{After: opencdc.RawData(payload)}}
}

func TestBatchProcessor_IsAProcessor(_ *testing.T) {
	// A BatchProcessor is a Processor, so it can be passed anywhere a Processor
	// is expected without the author changing anything else.
	var _ Processor = (*batchProcessor)(nil)
	var _ BatchProcessor = (*batchProcessor)(nil)
}

func TestProcessBatchOrProcess_TakesBatchSurface(t *testing.T) {
	is := is.New(t)
	p := &batchProcessor{}

	out, err := ProcessBatchOrProcess(context.Background(), p, []opencdc.Record{rec("a"), rec("b")})
	is.NoErr(err)
	is.Equal(len(out), 2)
	is.Equal(p.processBatchCalls, 1)
	is.Equal(p.processCalls, 0)

	// 1:1 by index: an ErrorRecord lands at the failing record's index only.
	_, ok := out[0].(SingleRecord)
	is.True(ok)
	_, ok = out[1].(SingleRecord)
	is.True(ok)
}

func TestProcessBatchOrProcess_FallsBackToProcess(t *testing.T) {
	is := is.New(t)
	// A Processor that does not implement BatchProcessor runs through Process.
	p := &funcProcessor{calls: new(int)}

	out, err := ProcessBatchOrProcess(context.Background(), p, []opencdc.Record{rec("a")})
	is.NoErr(err)
	is.Equal(len(out), 1)
	is.Equal(*p.calls, 1)
}

func TestProcessBatchOrProcess_WrongLengthRejected(t *testing.T) {
	is := is.New(t)
	p := &shortBatchProcessor{}

	_, err := ProcessBatchOrProcess(context.Background(), p, []opencdc.Record{rec("a"), rec("b")})
	is.True(err != nil)
	is.True(errors.Is(err, ErrNotBatchProcessor) == false)
}

func TestProcessBatchOrProcess_WholeBatchErrorRejected(t *testing.T) {
	is := is.New(t)
	p := &errorBatchProcessor{}

	_, err := ProcessBatchOrProcess(context.Background(), p, []opencdc.Record{rec("a"), rec("b")})
	is.True(err != nil)
	is.True(errors.Is(err, errBatchFail))
}

// errBatchFail is the whole-batch error returned by errorBatchProcessor.
var errBatchFail = errors.New("whole batch failed")

type errorBatchProcessor struct {
	UnimplementedProcessor
}

func (e *errorBatchProcessor) Specification() (Specification, error) { return Specification{}, nil }
func (e *errorBatchProcessor) Process(context.Context, []opencdc.Record) []ProcessedRecord {
	return nil
}

func (e *errorBatchProcessor) ProcessBatch(context.Context, []opencdc.Record) ([]ProcessedRecord, error) {
	return nil, errBatchFail
}

type shortBatchProcessor struct {
	UnimplementedProcessor
}

func (s *shortBatchProcessor) Specification() (Specification, error) { return Specification{}, nil }
func (s *shortBatchProcessor) Process(context.Context, []opencdc.Record) []ProcessedRecord {
	return nil
}

func (s *shortBatchProcessor) ProcessBatch(_ context.Context, records []opencdc.Record) ([]ProcessedRecord, error) {
	// Return one result for two inputs: the wrong-length contract is violated.
	return []ProcessedRecord{SingleRecord(records[0])}, nil
}

// funcProcessor is a minimal single-record Processor for the fallback test.
type funcProcessor struct {
	UnimplementedProcessor
	calls *int
}

func (f *funcProcessor) Specification() (Specification, error) { return Specification{}, nil }
func (f *funcProcessor) Process(_ context.Context, records []opencdc.Record) []ProcessedRecord {
	*f.calls++
	out := make([]ProcessedRecord, len(records))
	for i, r := range records {
		out[i] = SingleRecord(r)
	}
	return out
}

func TestProcessBatchOrProcess_MiddlewareSentinelFallsBack(t *testing.T) {
	is := is.New(t)
	calls := new(int)
	// Wrap a non-batch processor in the schema middleware: the wrapper
	// implements BatchProcessor but returns ErrNotBatchProcessor, so the caller
	// must fall back to Process (the wrapped processor runs unchanged).
	inner := &funcProcessor{calls: calls}
	wrapped := ProcessorWithMiddleware(inner, DefaultProcessorMiddleware()...)

	_, ok := wrapped.(BatchProcessor)
	is.True(ok) // the wrapper presents the batch surface

	out, err := ProcessBatchOrProcess(context.Background(), wrapped, []opencdc.Record{rec("a")})
	is.NoErr(err)
	is.Equal(len(out), 1)
	is.Equal(*calls, 1) // Process ran, ProcessBatch did not reach the inner processor
}

func TestSpecification_StatelessRoundTrip(t *testing.T) {
	is := is.New(t)
	spec := Specification{Name: "s", Stateless: true}

	protoSpec := (protoConverter{}).specifyResponse(spec)
	is.True(protoSpec.GetStateless())

	// A stateful (default) specification does not set the flag.
	is.True(!(protoConverter{}).specifyResponse(Specification{Name: "s"}).GetStateless())
}
