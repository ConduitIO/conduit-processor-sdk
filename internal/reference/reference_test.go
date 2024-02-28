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
		{".", rec},
		{".Position", rec.Position},
		{".Operation", rec.Operation},
		{".Metadata.foo", rec.Metadata["foo"]},
		{".Metadata.bar", nil},
		{".Key", rec.Key},
		{".Payload", rec.Payload},
		{".Payload.Before", rec.Payload.Before},
		{".Payload.After", rec.Payload.After},
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
	resolver, err := NewResolver(".Position")
	is.NoErr(err)

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
	resolver, err := NewResolver(".Operation")
	is.NoErr(err)

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
	resolver, err := NewResolver(".Metadata")
	is.NoErr(err)

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
}

func TestReference_MetadataField(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{nil, nil, false},
		{0, "", true},
	}

	is := is.New(t)
	resolver, err := NewResolver(".Metadata.foo")
	is.NoErr(err)

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
}

func TestReference_MetadataField_MapIndex(t *testing.T) {
	testCases := []testReferenceSetCase[any]{
		{"", "", false},
		{"foo", "foo", false},
		{nil, nil, false},
		{0, "", true},
	}

	is := is.New(t)
	resolver, err := NewResolver(`.Metadata["map key with spaces and symbols @$%^&*()_+"]`)
	is.NoErr(err)

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
	resolver, err := NewResolver(".Key")
	is.NoErr(err)

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
	resolver, err := NewResolver(".Key.foo")
	is.NoErr(err)

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
	resolver, err := NewResolver(`.Key["map key with spaces and symbols @$%^&*()_+"]`)
	is.NoErr(err)

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
	resolver, err := NewResolver(".Payload.Before")
	is.NoErr(err)

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
	resolver, err := NewResolver(".Payload.Before.foo")
	is.NoErr(err)

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
	resolver, err := NewResolver(`.Payload.Before["map key with spaces and symbols @$%^&*()_+"]`)
	is.NoErr(err)

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
	resolver, err := NewResolver(".Payload.After")
	is.NoErr(err)

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
	resolver, err := NewResolver(".Payload.After.foo")
	is.NoErr(err)

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
	resolver, err := NewResolver(`.Payload.After["map key with spaces and symbols @$%^&*()_+"]`)
	is.NoErr(err)

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
}
