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

package reference

import (
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
)

// benchRecordRaw and benchRecordStructured mirror the shapes processors see in
// production: a handful of metadata entries plus a raw or structured payload.
// They are used as the fixed starting point for every benchmark iteration so
// the measured cost is only reference resolution/get/set, not record setup.

func benchRecordRaw() opencdc.Record {
	return opencdc.Record{
		Position:  opencdc.Position("bench-position-0000000000"),
		Operation: opencdc.OperationUpdate,
		Metadata: opencdc.Metadata{
			"collection":         "orders",
			"version":            "v1",
			"opencdc.collection": "orders", // dotted key, only reachable via bracket syntax
		},
		Key: opencdc.RawData("order-12345"),
		Payload: opencdc.Change{
			Before: opencdc.RawData(`{"id":12345,"status":"pending"}`),
			After:  opencdc.RawData(`{"id":12345,"status":"shipped"}`),
		},
	}
}

func benchRecordStructured() opencdc.Record {
	return opencdc.Record{
		Position:  opencdc.Position("bench-position-0000000000"),
		Operation: opencdc.OperationUpdate,
		Metadata: opencdc.Metadata{
			"collection":         "orders",
			"version":            "v1",
			"opencdc.collection": "orders", // dotted key, only reachable via bracket syntax
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

// BenchmarkNewResolver measures the one-time cost of parsing a reference
// string into a Resolver (lexing + validation walk). Processors typically pay
// this cost once at Configure time, not per record, but it is included as a
// baseline to contrast against the per-record Resolve/Get/Set cost below.
func BenchmarkNewResolver(b *testing.B) {
	inputs := []string{
		".Key",
		".Metadata.collection",
		`.Metadata["opencdc.collection"]`,
		".Payload.After.status",
		".Payload.After.customer.name",
	}

	for _, input := range inputs {
		b.Run(input, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := NewResolver(input)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkResolver_Resolve measures the per-record cost of resolving an
// already-parsed reference against a record and reading the field's value.
// This is the hot path: a processor resolves the same reference for every
// record it processes.
//
// References to a whole top-level field (.Key, .Payload.After) are valid
// against both raw and structured payloads, so those run against both record
// shapes. References into a structured sub-field (e.g.
// .Payload.After.status) only resolve against a structured payload -- a
// processor configured that way against a raw-payload record would get a
// resolve error on every record, which is exercised separately.
func BenchmarkResolver_Resolve(b *testing.B) {
	wholeFieldCases := []struct {
		name string
		ref  string
	}{
		{"Key", ".Key"},
		{"Metadata/Field", ".Metadata.collection"},
		{"Metadata/BracketField", `.Metadata["opencdc.collection"]`},
		{"Payload.After", ".Payload.After"},
	}

	for _, bc := range wholeFieldCases {
		resolver, err := NewResolver(bc.ref)
		if err != nil {
			b.Fatal(err)
		}

		b.Run("Raw/"+bc.name, func(b *testing.B) {
			rec := benchRecordRaw()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref, err := resolver.Resolve(&rec)
				if err != nil {
					b.Fatal(err)
				}
				_ = ref.Get()
			}
		})

		b.Run("Structured/"+bc.name, func(b *testing.B) {
			rec := benchRecordStructured()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref, err := resolver.Resolve(&rec)
				if err != nil {
					b.Fatal(err)
				}
				_ = ref.Get()
			}
		})
	}

	structuredFieldCases := []struct {
		name string
		ref  string
	}{
		{"Payload.After/StructuredField", ".Payload.After.status"},
		{"Payload.After/NestedStructuredField", ".Payload.After.customer.name"},
	}

	for _, bc := range structuredFieldCases {
		resolver, err := NewResolver(bc.ref)
		if err != nil {
			b.Fatal(err)
		}

		b.Run("Structured/"+bc.name, func(b *testing.B) {
			rec := benchRecordStructured()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref, err := resolver.Resolve(&rec)
				if err != nil {
					b.Fatal(err)
				}
				_ = ref.Get()
			}
		})
	}
}

// BenchmarkResolver_ResolveAndSet measures resolving a reference and writing a
// new value through it, the pattern used by field-set/field-rename style
// processors on every record.
func BenchmarkResolver_ResolveAndSet(b *testing.B) {
	metadataCase := struct {
		name string
		ref  string
		val  any
	}{"Metadata/Field", ".Metadata.processed_by", "bench-processor"}

	{
		resolver, err := NewResolver(metadataCase.ref)
		if err != nil {
			b.Fatal(err)
		}

		b.Run("Raw/"+metadataCase.name, func(b *testing.B) {
			rec := benchRecordRaw()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref, err := resolver.Resolve(&rec)
				if err != nil {
					b.Fatal(err)
				}
				if err := ref.Set(metadataCase.val); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("Structured/"+metadataCase.name, func(b *testing.B) {
			rec := benchRecordStructured()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref, err := resolver.Resolve(&rec)
				if err != nil {
					b.Fatal(err)
				}
				if err := ref.Set(metadataCase.val); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	structuredFieldCases := []struct {
		name string
		ref  string
		val  any
	}{
		{"Payload.After/StructuredField", ".Payload.After.status", "processed"},
		{"Payload.After/NestedStructuredField", ".Payload.After.customer.name", "John Smith"},
	}

	for _, bc := range structuredFieldCases {
		resolver, err := NewResolver(bc.ref)
		if err != nil {
			b.Fatal(err)
		}

		b.Run("Structured/"+bc.name, func(b *testing.B) {
			rec := benchRecordStructured()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ref, err := resolver.Resolve(&rec)
				if err != nil {
					b.Fatal(err)
				}
				if err := ref.Set(bc.val); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
