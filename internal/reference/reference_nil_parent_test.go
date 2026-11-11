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
	"reflect"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// nilParentRecords are records where a reference's parent is missing: a nil
// or typed-nil key or payload part, and a nested field that is absent or nil.
func nilParentRecords() map[string]func() opencdc.Record {
	return map[string]func() opencdc.Record{
		"nil parts": func() opencdc.Record {
			return opencdc.Record{Operation: opencdc.OperationDelete}
		},
		"typed-nil parts": func() opencdc.Record {
			return opencdc.Record{
				Key:     opencdc.StructuredData(nil),
				Payload: opencdc.Change{Before: opencdc.StructuredData(nil), After: opencdc.StructuredData(nil)},
			}
		},
		"nested absent or nil": func() opencdc.Record {
			return opencdc.Record{
				Key:     opencdc.StructuredData{"id": 1},
				Payload: opencdc.Change{Before: opencdc.StructuredData{"n": nil}, After: opencdc.StructuredData{"n": map[string]any(nil)}},
			}
		},
	}
}

var nilParentReferences = []string{
	".Key.foo", ".Key.n.foo",
	".Payload.Before.foo", ".Payload.Before.n.foo", ".Payload.Before.n.m.foo",
	".Payload.After.foo", ".Payload.After.n.foo", ".Payload.After.n.m.foo",
}

// TestReference_NilParent_GetDeleteDoNotCreate is the regression test for
// resolving a field under a nil key or payload part creating an empty
// StructuredData. field.exclude on .Payload.After.x of a delete (which has no
// after) turned its after into {}, and the schema middleware then encoded a
// value the record never had. Resolving, reading and deleting a field must
// leave the record exactly as it was (invariant 6).
func TestReference_NilParent_GetDeleteDoNotCreate(t *testing.T) {
	for name, mk := range nilParentRecords() {
		for _, refStr := range nilParentReferences {
			t.Run(name+"/"+refStr, func(t *testing.T) {
				is := is.New(t)
				rec, want := mk(), mk()
				ref := MustResolver(is)(NewResolver(refStr))

				r, err := ref.Resolve(&rec)
				is.NoErr(err)
				is.True(reflect.DeepEqual(rec, want)) // resolve changed the record
				is.Equal(r.Get(), nil)
				is.NoErr(r.Delete())
				is.True(reflect.DeepEqual(rec, want)) // delete changed the record
			})
		}
	}
}

// TestReference_NilParent_SetCreates: setting a field under a missing parent
// still creates the parents, as before, including under a typed-nil part
// (which used to panic on the write to a nil map).
func TestReference_NilParent_SetCreates(t *testing.T) {
	for name, mk := range nilParentRecords() {
		for _, refStr := range nilParentReferences {
			t.Run(name+"/"+refStr, func(t *testing.T) {
				is := is.New(t)
				rec := mk()
				ref := MustResolver(is)(NewResolver(refStr))

				r, err := ref.Resolve(&rec)
				is.NoErr(err)
				is.NoErr(r.Set("v"))
				is.Equal(r.Get(), "v")

				// resolving again finds the value that was set
				r2, err := ref.Resolve(&rec)
				is.NoErr(err)
				is.Equal(r2.Get(), "v")
			})
		}
	}
}

// TestReference_NilParent_Rename: renaming a field under a missing parent
// behaves as before: the parents are created and the new field holds nil.
func TestReference_NilParent_Rename(t *testing.T) {
	is := is.New(t)
	rec := opencdc.Record{}
	r, err := MustResolver(is)(NewResolver(".Payload.After.n.foo")).Resolve(&rec)
	is.NoErr(err)
	renamed, err := r.Rename("bar")
	is.NoErr(err)
	is.Equal(renamed.Get(), nil)
	is.Equal(rec.Payload.After, opencdc.StructuredData{"n": map[string]any{"bar": nil}})
}

// TestReference_NilParent_RawStillFails: a part that holds raw data is not
// structured, and resolving a field in it still fails.
func TestReference_NilParent_RawStillFails(t *testing.T) {
	is := is.New(t)
	rec := opencdc.Record{Payload: opencdc.Change{After: opencdc.RawData("x")}}
	_, err := MustResolver(is)(NewResolver(".Payload.After.foo")).Resolve(&rec)
	is.True(err != nil)
}
