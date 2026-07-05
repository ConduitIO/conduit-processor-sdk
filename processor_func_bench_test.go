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
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
)

// benchBatchSize is the number of records processed per Process call in the
// benchmarks below. It is representative of a typical pipeline batch, not a
// single record; ns/op and allocs/op should be divided by this constant to
// get a per-record figure.
const benchBatchSize = 100

func benchBatchRaw(n int) []opencdc.Record {
	records := make([]opencdc.Record, n)
	for i := range records {
		records[i] = opencdc.Record{
			Position:  opencdc.Position("bench-position"),
			Operation: opencdc.OperationUpdate,
			Metadata: opencdc.Metadata{
				"collection": "orders",
				"version":    "v1",
			},
			Key: opencdc.RawData("order-12345"),
			Payload: opencdc.Change{
				Before: opencdc.RawData(`{"id":12345,"status":"pending"}`),
				After:  opencdc.RawData(`{"id":12345,"status":"shipped"}`),
			},
		}
	}
	return records
}

func benchBatchStructured(n int) []opencdc.Record {
	records := make([]opencdc.Record, n)
	for i := range records {
		records[i] = opencdc.Record{
			Position:  opencdc.Position("bench-position"),
			Operation: opencdc.OperationUpdate,
			Metadata: opencdc.Metadata{
				"collection": "orders",
				"version":    "v1",
			},
			Key: opencdc.StructuredData{
				"id": 12345,
			},
			Payload: opencdc.Change{
				Before: opencdc.StructuredData{
					"id":     12345,
					"status": "pending",
					"customer": map[string]any{
						"id":   987,
						"name": "Jane Doe",
					},
				},
				After: opencdc.StructuredData{
					"id":     12345,
					"status": "shipped",
					"customer": map[string]any{
						"id":   987,
						"name": "Jane Doe",
					},
				},
			},
		}
	}
	return records
}

// BenchmarkProcessorFunc_Process_MetadataFieldSet benchmarks a representative
// field-set style processor (built with the ProcessorFunc adapter, the SDK's
// concrete, reusable Processor implementation) that resolves a reference once
// per record and sets a metadata field on it. This is the same
// resolve-then-set pattern used by real field-set/field-rename processors,
// exercised through the full Processor.Process call including the
// ProcessedRecord wrapping. The reference works against both raw and
// structured payloads since it targets metadata, not the payload itself.
func BenchmarkProcessorFunc_Process_MetadataFieldSet(b *testing.B) {
	resolver, err := NewReferenceResolver(".Metadata.processed_by")
	if err != nil {
		b.Fatal(err)
	}

	proc := NewProcessorFunc(
		Specification{Name: "bench-metadata-field-set"},
		func(_ context.Context, record opencdc.Record) (opencdc.Record, error) {
			ref, err := resolver.Resolve(&record)
			if err != nil {
				return record, fmt.Errorf("failed to resolve reference: %w", err)
			}
			if err := ref.Set("bench-processor"); err != nil {
				return record, fmt.Errorf("failed to set field: %w", err)
			}
			return record, nil
		},
	)

	ctx := context.Background()

	b.Run("Raw", func(b *testing.B) {
		records := benchBatchRaw(benchBatchSize)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			out := proc.Process(ctx, records)
			if len(out) != benchBatchSize {
				b.Fatalf("expected %d processed records, got %d", benchBatchSize, len(out))
			}
		}
	})

	b.Run("Structured", func(b *testing.B) {
		records := benchBatchStructured(benchBatchSize)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			out := proc.Process(ctx, records)
			if len(out) != benchBatchSize {
				b.Fatalf("expected %d processed records, got %d", benchBatchSize, len(out))
			}
		}
	})
}

// BenchmarkProcessorFunc_Process_StructuredFieldSet benchmarks a processor
// that resolves a nested structured-payload reference and overwrites its
// value on every record in the batch, the pattern used by processors that
// rewrite a specific field deep in a record's payload (e.g. masking,
// normalization, enrichment).
func BenchmarkProcessorFunc_Process_StructuredFieldSet(b *testing.B) {
	resolver, err := NewReferenceResolver(".Payload.After.customer.name")
	if err != nil {
		b.Fatal(err)
	}

	proc := NewProcessorFunc(
		Specification{Name: "bench-structured-field-set"},
		func(_ context.Context, record opencdc.Record) (opencdc.Record, error) {
			ref, err := resolver.Resolve(&record)
			if err != nil {
				return record, fmt.Errorf("failed to resolve reference: %w", err)
			}
			if err := ref.Set("REDACTED"); err != nil {
				return record, fmt.Errorf("failed to set field: %w", err)
			}
			return record, nil
		},
	)

	ctx := context.Background()
	records := benchBatchStructured(benchBatchSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := proc.Process(ctx, records)
		if len(out) != benchBatchSize {
			b.Fatalf("expected %d processed records, got %d", benchBatchSize, len(out))
		}
	}
}
