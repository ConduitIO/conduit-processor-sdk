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
	"fmt"
	"reflect"
	"strconv"

	"github.com/conduitio/conduit-commons/opencdc"
)

type Reference struct {
	val reflect.Value
}

func (r Reference) Get() any {
	return r.val.Interface()
}

func (r Reference) Set(val string) error {
	switch r.val.Kind() {
	case reflect.String:
		r.val.SetString(val)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(val, 10, r.val.Type().Bits())
		if err != nil {
			return err
		}
		r.val.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i, err := strconv.ParseUint(val, 10, r.val.Type().Bits())
		if err != nil {
			return err
		}
		r.val.SetUint(i)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(val, r.val.Type().Bits())
		if err != nil {
			return err
		}
		r.val.SetFloat(f)
	case reflect.Bool:
		b, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		r.val.SetBool(b)
	case reflect.Map:
		// TODO support map
		fallthrough
	case reflect.Slice:
		if r.val.Type().Elem().Kind() == reflect.Uint8 {
			r.val.SetBytes([]byte(val))
			return nil
		}
		fallthrough
	default:
		return fmt.Errorf("unsupported type %s", r.val.Type())
	}
	return nil
}

// ReferenceResolver is a type that knows how to resolve a reference to a field
// in a record. It is used to specify the target of a processor's output.
type ReferenceResolver struct {
	// Raw is the raw string that was parsed to create this reference resolver.
	Raw string

	walker referenceWalker
}

func NewReferenceResolver(ref string) (ReferenceResolver, error) {
	l := newLexer(ref)

	i := l.Next()
	if i.typ == itemVariable {
		// skip variable token, if any
		i = l.Next()
	}

	var walkers []referenceWalker
	for i.typ != itemEOF && i.typ != itemError {
		// for now all we accept are chained fields
		if i.typ != itemField {
			return ReferenceResolver{}, fmt.Errorf("invalid reference %q: unexpected token %s", ref, i)
		}
		field := i.val[1:] // skip leading dot
		walkers = append(walkers, fieldReferenceWalker{field: field})
		i = l.Next()
	}
	if i.typ == itemError {
		return ReferenceResolver{}, fmt.Errorf("invalid reference %q: %s", ref, i.val)
	}

	return ReferenceResolver{
		Raw:    ref,
		walker: chainedReferenceWalker(walkers),
	}, nil
}

// Resolve resolves the reference to a field in the record. If the reference
// cannot be resolved an error is returned. If the reference is valid but the
// field does not exist in the record, the field will be created.
// The returned reference can be used to set the value of the field.
func (rr ReferenceResolver) Resolve(rec *opencdc.Record) (Reference, error) {
	recVal := reflect.ValueOf(rec)
	val, err := rr.walker.walk(recVal)
	if err != nil {
		return Reference{}, err
	}
	return Reference{val: val}, nil
}

type referenceWalker interface {
	walk(val reflect.Value) (reflect.Value, error)
}

// chainedReferenceWalker is a referenceWalker that chains multiple
// referenceWalkers together.
type chainedReferenceWalker []referenceWalker

func (rw chainedReferenceWalker) walk(val reflect.Value) (reflect.Value, error) {
	var err error
	for _, w := range rw {
		val, err = w.walk(val)
		if err != nil {
			return reflect.Value{}, err
		}
	}
	return val, nil
}

type fieldReferenceWalker struct {
	field string
}

func (rw fieldReferenceWalker) walk(val reflect.Value) (reflect.Value, error) {
	if val.Kind() == reflect.Ptr {
		// dereference pointer
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.Struct:
		fieldVal := val.FieldByName(rw.field)
		if !fieldVal.IsValid() {
			return reflect.Value{}, fmt.Errorf("field %q does not exist in struct %s", rw.field, val.Type())
		}
		return fieldVal, nil
	case reflect.Map:
		v := val.MapIndex(reflect.ValueOf(rw.field))
		if !v.IsValid() {
			// TODO create map entry
			return reflect.Value{}, fmt.Errorf("field %q does not exist in map %s", rw.field, val.Type())
		}
		return v, nil
	default:
		return reflect.Value{}, fmt.Errorf("unexpected value kind, got %s", val.Kind())
	}
}
