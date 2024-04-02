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
	"errors"
	"fmt"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestNewResolver_Fail(t *testing.T) {
	testCases := []string{
		"foo",
		"(.Key)",
		".foo",
		".Position.foo",
		".Operation.foo",
		".Metadata.foo.bar",
		`.Metadata["foo"`,
		`.Metadata["foo"]["bar"]`,
		`.Metadata[]`,
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			is := is.New(t)
			_, err := NewResolver(tc)
			is.True(err != nil)
		})
	}
}

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
		// uppercase
		{".", rec},
		{".Position", rec.Position},
		{".Operation", rec.Operation},
		{".Metadata.foo", rec.Metadata["foo"]},
		{".Metadata.bar", nil},
		{".Key", rec.Key},
		{".Payload", rec.Payload},
		{".Payload.Before", rec.Payload.Before},
		{".Payload.After", rec.Payload.After},
		// lowercase
		{".position", rec.Position},
		{".operation", rec.Operation},
		{".metadata.foo", rec.Metadata["foo"]},
		{".metadata.bar", nil},
		{".key", rec.Key},
		{".payload", rec.Payload},
		{".payload.before", rec.Payload.Before},
		{".payload.after", rec.Payload.After},
	}

	for _, tc := range testCases {
		t.Run(tc.reference, func(t *testing.T) {
			is := is.New(t)
			resolver, err := NewResolver(tc.reference)
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
		// uppercase
		{".Key.foo1", rec.Key.(opencdc.StructuredData)["foo1"]},
		{".Key.nested1.bar1", rec.Key.(opencdc.StructuredData)["nested1"].(map[string]any)["bar1"]},
		{".Payload.Before.foo2", rec.Payload.Before.(opencdc.StructuredData)["foo2"]},
		{".Payload.Before.nested2.bar2", rec.Payload.Before.(opencdc.StructuredData)["nested2"].(map[string]any)["bar2"]},
		{".Payload.After.foo3", rec.Payload.After.(opencdc.StructuredData)["foo3"]},
		{".Payload.After.nested3.bar3", rec.Payload.After.(opencdc.StructuredData)["nested3"].(map[string]any)["bar3"]},
		// lowercase
		{".key.foo1", rec.Key.(opencdc.StructuredData)["foo1"]},
		{".key.nested1.bar1", rec.Key.(opencdc.StructuredData)["nested1"].(map[string]any)["bar1"]},
		{".payload.before.foo2", rec.Payload.Before.(opencdc.StructuredData)["foo2"]},
		{".payload.before.nested2.bar2", rec.Payload.Before.(opencdc.StructuredData)["nested2"].(map[string]any)["bar2"]},
		{".payload.after.foo3", rec.Payload.After.(opencdc.StructuredData)["foo3"]},
		{".payload.after.nested3.bar3", rec.Payload.After.(opencdc.StructuredData)["nested3"].(map[string]any)["bar3"]},
	}

	for _, tc := range testCases {
		t.Run(tc.reference, func(t *testing.T) {
			is := is.New(t)
			resolver, err := NewResolver(tc.reference)
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
		// uppercase
		{".Position", nil},
		{".Operation", opencdc.Operation(0)},
		{".Metadata.foo", nil},
		{".Metadata.bar", nil},
		{".Key.foo1", nil},
		{".Key.nested1.bar1", nil},
		{".Payload.Before.foo2", nil},
		{".Payload.Before.nested2.bar2", nil},
		{".Payload.After.foo3", nil},
		{".Payload.After.nested3.bar3", nil},
		// lowercase
		{".position", nil},
		{".operation", opencdc.Operation(0)},
		{".metadata.foo", nil},
		{".metadata.bar", nil},
		{".key.foo1", nil},
		{".key.nested1.bar1", nil},
		{".payload.before.foo2", nil},
		{".payload.before.nested2.bar2", nil},
		{".payload.after.foo3", nil},
		{".payload.after.nested3.bar3", nil},
	}

	for _, tc := range testCases {
		t.Run(tc.reference, func(t *testing.T) {
			is := is.New(t)
			resolver, err := NewResolver(tc.reference)
			is.NoErr(err)

			ref, err := resolver.Resolve(&rec)
			is.NoErr(err)

			is.Equal(ref.Get(), tc.want)
		})
	}
}

type testReferenceSetCase[T any] struct {
	value   any
	want    T
	wantErr bool
}

func testSet[T any](t *testing.T, resolver Resolver, tc testReferenceSetCase[T]) {
	t.Helper()

	is := is.New(t)
	is.Helper()

	rec := opencdc.Record{}

	ref, err := resolver.Resolve(&rec)
	is.NoErr(err)

	err = ref.Set(tc.value)
	if tc.wantErr {
		is.True(err != nil)
	} else {
		is.NoErr(err)
		is.Equal(ref.Get(), tc.want)
	}
}

func testRename(t *testing.T, resolver Resolver, rec opencdc.Record, want any, wantErr bool, immutable bool) {
	t.Helper()

	is := is.New(t)
	is.Helper()

	ref, err := resolver.Resolve(&rec)
	is.NoErr(err)

	renamedRef, err := ref.Rename("newName")
	if wantErr {
		if immutable {
			is.True(errors.Is(err, ErrRenameImmutableReference))
		} else {
			is.True(errors.Is(err, ErrFieldExists))
		}
		return
	}
	is.NoErr(err)
	is.Equal(renamedRef.Get(), want)
	is.Equal(ref.Get(), nil)
}

func testDelete(t *testing.T, resolver Resolver, rec opencdc.Record, emptyVal any, allowed bool) {
	t.Helper()

	is := is.New(t)
	is.Helper()

	ref, err := resolver.Resolve(&rec)
	is.NoErr(err)

	is.True(ref.Get() != nil)

	err = ref.Delete()
	if allowed {
		is.NoErr(err)
		is.Equal(ref.Get(), emptyVal)
	} else {
		is.True(err != nil)
	}
}

func TestReference_Position(t *testing.T) {
	// all test cases should fail, position can not be set
	testCases := []testReferenceSetCase[opencdc.Data]{
		{"", opencdc.RawData(""), true},
		{"foo", opencdc.RawData("foo"), true},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), true},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, true},
		{map[string]any{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, true},
		{nil, nil, true},
		{0, nil, true},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase": MustResolver(is)(NewResolver(".Position")),
		"lowercase": MustResolver(is)(NewResolver(".position")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Position: opencdc.Position("foo")},
					"",
					true,
					true,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Position: opencdc.Position("foo")},
					nil,
					false,
				)
			})
		})
	}
}

func TestReference_Operation(t *testing.T) {
	testCases := []testReferenceSetCase[opencdc.Operation]{
		{"create", opencdc.OperationCreate, false},
		{"update", opencdc.OperationUpdate, false},
		{"delete", opencdc.OperationDelete, false},
		{"snapshot", opencdc.OperationSnapshot, false},
		{opencdc.OperationCreate, opencdc.OperationCreate, false},
		{opencdc.OperationUpdate, opencdc.OperationUpdate, false},
		{opencdc.OperationDelete, opencdc.OperationDelete, false},
		{opencdc.OperationSnapshot, opencdc.OperationSnapshot, false},
		{0, 0, true},
		{1, opencdc.OperationCreate, false},
		{2, opencdc.OperationUpdate, false},
		{3, opencdc.OperationDelete, false},
		{4, opencdc.OperationSnapshot, false},
		{5, 0, true},
		{"", 0, true},
		{"foo", 0, true},
		{nil, 0, true},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase": MustResolver(is)(NewResolver(".Operation")),
		"lowercase": MustResolver(is)(NewResolver(".operation")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Operation: opencdc.OperationCreate},
					"",
					true,
					true,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Operation: opencdc.OperationCreate},
					nil,
					false,
				)
			})
		})
	}
}

func TestReference_Metadata(t *testing.T) {
	testCases := []testReferenceSetCase[opencdc.Metadata]{
		{opencdc.Metadata{}, opencdc.Metadata{}, false},
		{map[string]string{}, opencdc.Metadata{}, false},
		{nil, opencdc.Metadata{}, false},
		{"", nil, true},
		{"foo", nil, true},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase": MustResolver(is)(NewResolver(".Metadata")),
		"lowercase": MustResolver(is)(NewResolver(".metadata")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Metadata: opencdc.Metadata{"foo": "bar"}},
					"",
					true,
					true,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Metadata: opencdc.Metadata{"foo": "bar"}},
					opencdc.Metadata{},
					true,
				)
			})
		})
	}
}

func TestReference_MetadataField(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{nil, nil, false},
		{0, "", true},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase": MustResolver(is)(NewResolver(".Metadata.foo")),
		"lowercase": MustResolver(is)(NewResolver(".metadata.foo")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Metadata: opencdc.Metadata{"foo": "bar"}},
					"bar",
					false,
					false,
				)
			})
			t.Run("Rename:fieldExists", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Metadata: opencdc.Metadata{"foo": "bar", "newName": "baz"}}, // field newName already exists
					"bar",
					true,
					false,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Metadata: opencdc.Metadata{"foo": "bar"}},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_MetadataField_MapIndex(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{nil, nil, false},
		{0, "", true},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase": MustResolver(is)(NewResolver(`.Metadata["map key with spaces and symbols @$%^&*()_+"]`)),
		"lowercase": MustResolver(is)(NewResolver(`.metadata["map key with spaces and symbols @$%^&*()_+"]`)),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Metadata: opencdc.Metadata{"map key with spaces and symbols @$%^&*()_+": "bar"}},
					"bar",
					false,
					true,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Metadata: opencdc.Metadata{"map key with spaces and symbols @$%^&*()_+": "bar"}},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_Key(t *testing.T) {
	testCases := []testReferenceSetCase[opencdc.Data]{
		{"", opencdc.RawData(""), false},
		{"foo", opencdc.RawData("foo"), false},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), false},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{map[string]any{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{nil, nil, false},
		{0, nil, true},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase": MustResolver(is)(NewResolver(".Key")),
		"lowercase": MustResolver(is)(NewResolver(".key")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Key: opencdc.RawData("foo")},
					"",
					true,
					true,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Key: opencdc.RawData("foo")},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_KeyField(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), false},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{map[string]any{"foo": "bar"}, map[string]any{"foo": "bar"}, false},
		{nil, nil, false},
		{0, 0, false},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase": MustResolver(is)(NewResolver(".Key.foo")),
		"lowercase": MustResolver(is)(NewResolver(".key.foo")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Key: opencdc.StructuredData{"foo": "bar"}},
					"bar",
					false,
					false,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Key: opencdc.StructuredData{"foo": "bar"}},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_KeyField_MapIndex(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), false},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{map[string]any{"foo": "bar"}, map[string]any{"foo": "bar"}, false},
		{nil, nil, false},
		{0, 0, false},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase": MustResolver(is)(NewResolver(`.Key["map key with spaces and symbols @$%^&*()_+"]`)),
		"lowercase": MustResolver(is)(NewResolver(`.key["map key with spaces and symbols @$%^&*()_+"]`)),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Key: opencdc.StructuredData{"map key with spaces and symbols @$%^&*()_+": "bar"}},
					"bar",
					false,
					false,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Key: opencdc.StructuredData{"map key with spaces and symbols @$%^&*()_+": "bar"}},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_PayloadBefore(t *testing.T) {
	testCases := []testReferenceSetCase[opencdc.Data]{
		{"", opencdc.RawData(""), false},
		{"foo", opencdc.RawData("foo"), false},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), false},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{map[string]any{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{nil, nil, false},
		{0, nil, true},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase1": MustResolver(is)(NewResolver(".payload.Before")),
		"uppercase2": MustResolver(is)(NewResolver(".Payload.Before")),
		"lowercase1": MustResolver(is)(NewResolver(".payload.before")),
		"lowercase2": MustResolver(is)(NewResolver(".Payload.before")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{Before: opencdc.RawData("foo")}},
					"",
					true,
					true,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{Before: opencdc.RawData("foo")}},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_PayloadBeforeField(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), false},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{map[string]any{"foo": "bar"}, map[string]any{"foo": "bar"}, false},
		{nil, nil, false},
		{0, 0, false},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase1": MustResolver(is)(NewResolver(".payload.Before.foo")),
		"uppercase2": MustResolver(is)(NewResolver(".Payload.Before.foo")),
		"lowercase1": MustResolver(is)(NewResolver(".payload.before.foo")),
		"lowercase2": MustResolver(is)(NewResolver(".Payload.before.foo")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{Before: opencdc.StructuredData{"foo": "bar"}}},
					"bar",
					false,
					false,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{Before: opencdc.StructuredData{"foo": "bar"}}},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_PayloadBeforeField_MapIndex(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), false},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{map[string]any{"foo": "bar"}, map[string]any{"foo": "bar"}, false},
		{nil, nil, false},
		{0, 0, false},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase1": MustResolver(is)(NewResolver(`.payload.Before["map key with spaces and symbols @$%^&*()_+"]`)),
		"uppercase2": MustResolver(is)(NewResolver(`.Payload.Before["map key with spaces and symbols @$%^&*()_+"]`)),
		"lowercase1": MustResolver(is)(NewResolver(`.payload.before["map key with spaces and symbols @$%^&*()_+"]`)),
		"lowercase2": MustResolver(is)(NewResolver(`.Payload.before["map key with spaces and symbols @$%^&*()_+"]`)),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{Before: opencdc.StructuredData{"map key with spaces and symbols @$%^&*()_+": "bar"}}},
					"bar",
					false,
					false,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{Before: opencdc.StructuredData{"map key with spaces and symbols @$%^&*()_+": "bar"}}},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_PayloadAfter(t *testing.T) {
	testCases := []testReferenceSetCase[opencdc.Data]{
		{"", opencdc.RawData(""), false},
		{"foo", opencdc.RawData("foo"), false},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), false},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{map[string]any{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{nil, nil, false},
		{0, nil, true},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase1": MustResolver(is)(NewResolver(".payload.After")),
		"uppercase2": MustResolver(is)(NewResolver(".Payload.After")),
		"lowercase1": MustResolver(is)(NewResolver(".payload.after")),
		"lowercase2": MustResolver(is)(NewResolver(".Payload.after")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{After: opencdc.RawData("foo")}},
					"",
					true,
					true,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{After: opencdc.RawData("foo")}},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_PayloadAfterField(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), false},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{map[string]any{"foo": "bar"}, map[string]any{"foo": "bar"}, false},
		{nil, nil, false},
		{0, 0, false},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase1": MustResolver(is)(NewResolver(".payload.After.foo")),
		"uppercase2": MustResolver(is)(NewResolver(".Payload.After.foo")),
		"lowercase1": MustResolver(is)(NewResolver(".payload.after.foo")),
		"lowercase2": MustResolver(is)(NewResolver(".Payload.after.foo")),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"foo": "bar"}}},
					"bar",
					false,
					false,
				)
			})
			t.Run("Rename:fieldExists", func(t *testing.T) {
				testRename(
					t,
					resolver,
					// field newName already exists
					opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"foo": "bar", "newName": "baz"}}},
					"",
					true,
					false,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"foo": "bar"}}},
					nil,
					true,
				)
			})
		})
	}
}

func TestReference_PayloadAfterField_MapIndex(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{opencdc.RawData("bar"), opencdc.RawData("bar"), false},
		{opencdc.StructuredData{"foo": "bar"}, opencdc.StructuredData{"foo": "bar"}, false},
		{map[string]any{"foo": "bar"}, map[string]any{"foo": "bar"}, false},
		{nil, nil, false},
		{0, 0, false},
	}

	is := is.New(t)
	resolvers := map[string]Resolver{
		"uppercase1": MustResolver(is)(NewResolver(`.payload.After["map key with spaces and symbols @$%^&*()_+"]`)),
		"uppercase2": MustResolver(is)(NewResolver(`.Payload.After["map key with spaces and symbols @$%^&*()_+"]`)),
		"lowercase1": MustResolver(is)(NewResolver(`.payload.after["map key with spaces and symbols @$%^&*()_+"]`)),
		"lowercase2": MustResolver(is)(NewResolver(`.Payload.after["map key with spaces and symbols @$%^&*()_+"]`)),
	}

	for testName, resolver := range resolvers {
		t.Run(testName, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("Set %v", tc.value), func(t *testing.T) {
					testSet(t, resolver, tc)
				})
			}
			t.Run("Rename", func(t *testing.T) {
				testRename(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"map key with spaces and symbols @$%^&*()_+": "bar"}}},
					"bar",
					false,
					false,
				)
			})
			t.Run("Delete", func(t *testing.T) {
				testDelete(
					t,
					resolver,
					opencdc.Record{Payload: opencdc.Change{After: opencdc.StructuredData{"map key with spaces and symbols @$%^&*()_+": "bar"}}},
					nil,
					true,
				)
			})
		})
	}
}

func MustResolver(is *is.I) func(Resolver, error) Resolver {
	return func(resolver Resolver, err error) Resolver {
		is.NoErr(err)
		return resolver
	}
}
