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
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
)

func ExampleReferenceResolver_simple() {
	rec := opencdc.Record{
		Position: []byte("my position"),
	}

	resolver, err := NewReferenceResolver(".Position")
	if err != nil {
		panic(err)
	}

	ref, err := resolver.Resolve(&rec)
	if err != nil {
		panic(err)
	}

	fmt.Println("ref value:", ref.Get())

	fmt.Println("setting the position is not allowed, let's try it")
	err = ref.Set("foo")
	fmt.Println(err)

	// Output:
	// ref value: my position
	// setting the position is not allowed, let's try it
	// cannot set position: cannot set immutable reference
}

func ExampleReferenceResolver_nested() {
	rec := opencdc.Record{
		Key: opencdc.StructuredData{
			"foo": map[string]any{
				"bar": "baz",
			},
		},
	}

	resolver, err := NewReferenceResolver(".Key.foo.bar")
	if err != nil {
		panic(err)
	}

	ref, err := resolver.Resolve(&rec)
	if err != nil {
		panic(err)
	}

	fmt.Println("ref value:", ref.Get())

	fmt.Println("setting the field now ...")
	err = ref.Set("qux")
	if err != nil {
		panic(err)
	}

	fmt.Println("new value:", rec.Key)

	// Output:
	// ref value: baz
	// setting the field now ...
	// new value: map[foo:map[bar:qux]]
}

func ExampleReferenceResolver_setNonExistingField() {
	rec := opencdc.Record{} // empty record

	resolver, err := NewReferenceResolver(".Payload.After.foo.bar")
	if err != nil {
		panic(err)
	}

	ref, err := resolver.Resolve(&rec)
	if err != nil {
		panic(err)
	}

	fmt.Println("ref value:", ref.Get())

	fmt.Println("setting the field now ...")
	err = ref.Set("hello")
	if err != nil {
		panic(err)
	}

	fmt.Println("new value:", rec.Payload.After)

	// Output:
	// ref value: <nil>
	// setting the field now ...
	// new value: map[foo:map[bar:hello]]
}
