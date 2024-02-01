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

package reference

import (
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestReference_Get_RawData(t *testing.T) {
	rec := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Metadata: map[string]string{
			"foo": "bar",
		},
		Key: opencdc.RawData("baz"),
		Payload: opencdc.Change{
			Before: opencdc.RawData("before"),
			After:  opencdc.RawData("after"),
		},
	}

	testCases := []struct {
		reference string
		want      any
	}{
		{".Position", rec.Position},
		{".Operation", rec.Operation},
		{".Metadata.foo", rec.Metadata["foo"]},
		{".Metadata.bar", ""},
		{".Key", rec.Key},
		{".Payload.Before", rec.Payload.Before},
		{".Payload.After", rec.Payload.After},
	}

	for _, tc := range testCases {
		t.Run(tc.reference, func(t *testing.T) {
			is := is.New(t)
			resolver, err := NewReferenceResolver(tc.reference)
			is.NoErr(err)

			ref, err := resolver.Resolve(&rec)
			is.NoErr(err)

			is.Equal(ref.Get(), tc.want)
		})
	}
}

func TestReference_Get_StructuredData(t *testing.T) {
	rec := opencdc.Record{
		Key: opencdc.StructuredData{
			"foo1": "baz",
			"nested1": map[string]any{
				"bar1": "qux",
			},
		},
		Payload: opencdc.Change{
			Before: opencdc.StructuredData{
				"foo2": "baz",
				"nested2": map[string]any{
					"bar2": "qux",
				},
			},
			After: opencdc.StructuredData{
				"foo3": "baz",
				"nested3": map[string]any{
					"bar3": "qux",
				},
			},
		},
	}

	testCases := []struct {
		reference string
		want      any
	}{
		{".Key.foo1", rec.Key.(opencdc.StructuredData)["foo1"]},
		{".Key.nested1.bar1", rec.Key.(opencdc.StructuredData)["nested1"].(map[string]any)["bar1"]},
		{".Payload.Before.foo2", rec.Payload.Before.(opencdc.StructuredData)["foo2"]},
		{".Payload.Before.nested2.bar2", rec.Payload.Before.(opencdc.StructuredData)["nested2"].(map[string]any)["bar2"]},
		{".Payload.After.foo3", rec.Payload.After.(opencdc.StructuredData)["foo3"]},
		{".Payload.After.nested3.bar3", rec.Payload.After.(opencdc.StructuredData)["nested3"].(map[string]any)["bar3"]},
	}

	for _, tc := range testCases {
		t.Run(tc.reference, func(t *testing.T) {
			is := is.New(t)
			resolver, err := NewReferenceResolver(tc.reference)
			is.NoErr(err)

			ref, err := resolver.Resolve(&rec)
			is.NoErr(err)

			is.Equal(ref.Get(), tc.want)
		})
	}
}

func TestReference_Get_NoData(t *testing.T) {
	rec := opencdc.Record{}

	testCases := []struct {
		reference string
		want      any
	}{
		{".Position", nil},
		{".Operation", opencdc.Operation(0)},
		{".Metadata.foo", ""},
		{".Metadata.bar", ""},
		{".Key.foo1", nil},
		{".Key.nested1.bar1", nil},
		{".Payload.Before.foo2", nil},
		{".Payload.Before.nested2.bar2", nil},
		{".Payload.After.foo3", nil},
		{".Payload.After.nested3.bar3", nil},
	}

	for _, tc := range testCases {
		t.Run(tc.reference, func(t *testing.T) {
			is := is.New(t)
			resolver, err := NewReferenceResolver(tc.reference)
			is.NoErr(err)

			ref, err := resolver.Resolve(&rec)
			is.NoErr(err)

			is.Equal(ref.Get(), tc.want)
		})
	}
}

func TestReference_Set(t *testing.T) {
	testCases := []struct {
		reference  string
		getFieldFn func(opencdc.Record) any
	}{
		{".", func(r opencdc.Record) any { return r }},
		{".Position", func(r opencdc.Record) any { return r.Position }},
		{".Operation", func(r opencdc.Record) any { return r.Operation }},
		{".Metadata.foo", func(r opencdc.Record) any { return r.Metadata["foo"] }},
		{".Metadata.bar", func(r opencdc.Record) any { return r.Metadata["bar"] }},
		{".Key", func(r opencdc.Record) any { return r.Key }},
		{".Payload.Before", func(r opencdc.Record) any { return r.Payload.Before }},
		{".Payload.After", func(r opencdc.Record) any { return r.Payload.After }},
	}

	for _, tc := range testCases {
		t.Run(tc.reference, func(t *testing.T) {
			is := is.New(t)
			resolver, err := NewReferenceResolver(tc.reference)
			is.NoErr(err)

			rec := opencdc.Record{}

			ref, err := resolver.Resolve(&rec)
			is.NoErr(err)

			err = ref.Set("create")
			is.NoErr(err)

			is.Equal(ref.Get(), tc.getFieldFn(rec))
		})
	}
}
