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

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// This file is the conformance check for the stateless declaration (design:
// 20261016-processor-batch-api.md). A processor that declares Stateless but
// keeps state is a bug: the engine will run its stage concurrently and reorder
// which instance handles which record, so a stateful processor produces wrong
// output. The check below feeds a processor type a state-dependent sequence and
// compares the serial result with the result of interleaving the same records
// across two instances. A stateless processor matches; a stateful one does not.

// checkStateless runs the stateless conformance scenario against a processor
// factory. It returns nil when the processor honours the stateless contract
// (its per-record output is independent of call order and instance), and an
// error naming the first record that diverged when the records were interleaved
// across two instances.
func checkStateless(newProc func() Processor, records []opencdc.Record) error {
	ctx := context.Background()

	serial := newProc()
	if err := serial.Open(ctx); err != nil {
		return fmt.Errorf("open serial instance: %w", err)
	}
	want := serial.Process(ctx, records)
	_ = serial.Teardown(ctx)

	a := newProc()
	if err := a.Open(ctx); err != nil {
		return fmt.Errorf("open instance a: %w", err)
	}
	defer a.Teardown(ctx)
	b := newProc()
	if err := b.Open(ctx); err != nil {
		return fmt.Errorf("open instance b: %w", err)
	}
	defer b.Teardown(ctx)

	for i, r := range records {
		proc := a
		if i%2 == 1 {
			proc = b
		}
		out := proc.Process(ctx, []opencdc.Record{r})
		if len(out) != 1 {
			return fmt.Errorf("record %d: processor returned %d results for 1 record", i, len(out))
		}
		if got, want := single(out[0]), single(want[i]); got != want {
			return fmt.Errorf("record %d: interleaved output %q != serial output %q (stateful processor declared stateless)", i, got, want)
		}
	}
	return nil
}

// single returns the payload of a SingleRecord result as a string.
func single(rec ProcessedRecord) string {
	sr, ok := rec.(SingleRecord)
	if !ok {
		return fmt.Sprintf("<%T>", rec)
	}
	return string(sr.Payload.After.Bytes())
}

// uppercaseProcessor is a genuinely stateless processor: each record's output
// depends only on that record.
type uppercaseProcessor struct {
	UnimplementedProcessor
}

func (*uppercaseProcessor) Specification() (Specification, error) {
	return Specification{Name: "uppercase", Stateless: true}, nil
}

func (*uppercaseProcessor) Process(_ context.Context, records []opencdc.Record) []ProcessedRecord {
	out := make([]ProcessedRecord, len(records))
	for i, r := range records {
		r.Payload.After = opencdc.RawData(strings.ToUpper(string(r.Payload.After.Bytes())))
		out[i] = SingleRecord(r)
	}
	return out
}

// countingProcessor is a stateful processor that (incorrectly) declares
// Stateless: its output depends on how many records it has seen before, so
// interleaving records across instances changes the result.
type countingProcessor struct {
	UnimplementedProcessor
	count int
}

func (*countingProcessor) Specification() (Specification, error) {
	return Specification{Name: "counting", Stateless: true}, nil
}

func (c *countingProcessor) Process(_ context.Context, records []opencdc.Record) []ProcessedRecord {
	out := make([]ProcessedRecord, len(records))
	for i, r := range records {
		c.count++
		r.Payload.After = opencdc.RawData(fmt.Sprintf("%s#%d", string(r.Payload.After.Bytes()), c.count))
		out[i] = SingleRecord(r)
	}
	return out
}

func testRecords() []opencdc.Record {
	return []opencdc.Record{
		{Payload: opencdc.Change{After: opencdc.RawData("a")}},
		{Payload: opencdc.Change{After: opencdc.RawData("b")}},
		{Payload: opencdc.Change{After: opencdc.RawData("c")}},
		{Payload: opencdc.Change{After: opencdc.RawData("d")}},
	}
}

func TestCheckStateless_StatelessProcessorPasses(t *testing.T) {
	is := is.New(t)
	is.NoErr(checkStateless(func() Processor { return &uppercaseProcessor{} }, testRecords()))
}

func TestCheckStateless_StatefulProcessorDeclaredStatelessIsCaught(t *testing.T) {
	is := is.New(t)
	err := checkStateless(func() Processor { return &countingProcessor{} }, testRecords())
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "stateful processor declared stateless"))
}
