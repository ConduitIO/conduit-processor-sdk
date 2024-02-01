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

	"github.com/conduitio/conduit-commons/opencdc"
)

type Reference interface {
	Get() any
	Set(string) error

	walk(field string) (Reference, error)
}

// ReferenceResolver is a type that knows how to resolve a reference to a field
// in a record. It is used to specify the target of a processor's output.
type ReferenceResolver struct {
	// Raw is the raw string that was parsed to create this reference resolver.
	Raw string

	fields []string
}

func NewReferenceResolver(ref string) (ReferenceResolver, error) {
	l := newLexer(ref)

	i := l.Next()
	if i.typ == itemVariable {
		// skip variable token, if any
		i = l.Next()
	}

	var fields []string
	for i.typ != itemEOF && i.typ != itemError {
		// for now all we accept are chained fields
		if i.typ != itemField {
			return ReferenceResolver{}, fmt.Errorf("invalid reference %q: unexpected token %s", ref, i)
		}
		// TODO validate field name in context of the record
		field := i.val[1:] // skip leading dot
		fields = append(fields, field)
		i = l.Next()
	}
	if i.typ == itemError {
		return ReferenceResolver{}, fmt.Errorf("invalid reference %q: %s", ref, i.val)
	}

	return ReferenceResolver{
		Raw:    ref,
		fields: fields,
	}, nil
}

// Resolve resolves the reference to a field in the record. If the reference
// cannot be resolved an error is returned. If the reference is valid but the
// field does not exist in the record, the field will be created.
// The returned reference can be used to set the value of the field.
func (rr ReferenceResolver) Resolve(rec *opencdc.Record) (Reference, error) {
	var ref Reference = recordReference{rec: rec}

	for _, field := range rr.fields {
		var err error
		ref, err = ref.walk(field)
		if err != nil {
			return nil, err
		}
	}

	return ref, nil
}

type recordReference struct {
	rec *opencdc.Record
}

func (r recordReference) Get() any {
	return *r.rec
}

func (r recordReference) Set(val string) error {
	// TODO
	return errors.New("not implemented")
}

func (r recordReference) walk(field string) (Reference, error) {
	switch field {
	case "Position":
		return positionReference{rec: r.rec}, nil
	case "Operation":
		return operationReference{rec: r.rec}, nil
	case "Metadata":
		return metadataReference{rec: r.rec}, nil
	case "Key":
		return keyReference{rec: r.rec}, nil
	case "Payload":
		return payloadReference{rec: r.rec}, nil
	default:
		return nil, fmt.Errorf("unexpected field %q", field)
	}
}

type positionReference struct {
	rec *opencdc.Record
}

func (r positionReference) Get() any {
	return r.rec.Position
}

func (r positionReference) Set(string) error {
	return errors.New("cannot set position")
}

func (r positionReference) walk(string) (Reference, error) {
	return nil, errors.New("position is not a structured field, can't address a field inside position")
}

type operationReference struct {
	rec *opencdc.Record
}

func (r operationReference) Get() any {
	return r.rec.Operation
}

func (r operationReference) Set(val string) error {
	var op opencdc.Operation
	err := op.UnmarshalText([]byte(val))
	if err != nil {
		return fmt.Errorf("failed to set operation: %w", err)
	}
	r.rec.Operation = op
	return nil
}

func (r operationReference) walk(string) (Reference, error) {
	return nil, errors.New("operation is not a structured field, can't address a field inside operation")
}

type metadataReference struct {
	rec *opencdc.Record
}

func (r metadataReference) Get() any {
	return r.rec.Metadata
}

func (r metadataReference) Set(val string) error {
	// TODO
	return errors.New("not implemented")
}

func (r metadataReference) walk(field string) (Reference, error) {
	if r.rec.Metadata == nil {
		// create new metadata map so we can set the field
		r.rec.Metadata = make(map[string]string)
	}

	return metadataFieldReference{rec: r.rec, field: field}, nil
}

type metadataFieldReference struct {
	rec   *opencdc.Record
	field string
}

func (r metadataFieldReference) Get() any {
	return r.rec.Metadata[r.field]
}

func (r metadataFieldReference) Set(val string) error {
	r.rec.Metadata[r.field] = val
	return nil
}

func (r metadataFieldReference) walk(string) (Reference, error) {
	return nil, errors.New("metadata fields are not structured, can't address a field inside a metadata field")
}

type keyReference struct {
	rec *opencdc.Record
}

func (r keyReference) Get() any {
	return r.rec.Key
}

func (r keyReference) Set(val string) error {
	r.rec.Key = opencdc.RawData(val)
	return nil
}

func (r keyReference) walk(field string) (Reference, error) {
	switch r.rec.Key.(type) {
	case opencdc.StructuredData:
		// all good
	case nil:
		// create new structured data
		r.rec.Key = opencdc.StructuredData{}
	default:
		return nil, errors.New("key does not contain structured data, can't address a field inside key")
	}
	return dataFieldReference{
		data:  r.rec.Key.(opencdc.StructuredData),
		field: field,
	}, nil
}

type payloadReference struct {
	rec *opencdc.Record
}

func (r payloadReference) Get() any {
	return r.rec.Payload
}

func (r payloadReference) Set(val string) error {
	// TODO
	return errors.New("not implemented")
}

func (r payloadReference) walk(field string) (Reference, error) {
	switch field {
	case "Before":
		return payloadBeforeReference{rec: r.rec}, nil
	case "After":
		return payloadAfterReference{rec: r.rec}, nil
	default:
		return nil, fmt.Errorf("unexpected field %q", field)
	}
}

type payloadBeforeReference struct {
	rec *opencdc.Record
}

func (r payloadBeforeReference) Get() any {
	return r.rec.Payload.Before
}

func (r payloadBeforeReference) Set(val string) error {
	r.rec.Payload.Before = opencdc.RawData(val)
	return nil
}

func (r payloadBeforeReference) walk(field string) (Reference, error) {
	switch r.rec.Payload.Before.(type) {
	case opencdc.StructuredData:
		// all good
	case nil:
		// create new structured data
		r.rec.Payload.Before = opencdc.StructuredData{}
	default:
		return nil, errors.New("payload before does not contain structured data, can't address a field inside payload before")
	}
	return dataFieldReference{
		data:  r.rec.Payload.Before.(opencdc.StructuredData),
		field: field,
	}, nil
}

type payloadAfterReference struct {
	rec *opencdc.Record
}

func (r payloadAfterReference) Get() any {
	return r.rec.Payload.After
}

func (r payloadAfterReference) Set(val string) error {
	r.rec.Payload.After = opencdc.RawData(val)
	return nil
}

func (r payloadAfterReference) walk(field string) (Reference, error) {
	switch r.rec.Payload.After.(type) {
	case opencdc.StructuredData:
		// all good
	case nil:
		// create new structured data
		r.rec.Payload.After = opencdc.StructuredData{}
	default:
		return nil, errors.New("payload after does not contain structured data, can't address a field inside payload after")
	}
	return dataFieldReference{
		data:  r.rec.Payload.After.(opencdc.StructuredData),
		field: field,
	}, nil
}

type dataFieldReference struct {
	data  map[string]any
	field string
}

func (r dataFieldReference) Get() any {
	return r.data[r.field]
}

func (r dataFieldReference) Set(val string) error {
	r.data[r.field] = val
	return nil
}

func (r dataFieldReference) walk(field string) (Reference, error) {
	if r.data[r.field] == nil {
		// create new structured data for field, so we can set the field
		r.data[r.field] = make(map[string]any)
	}

	data, ok := r.data[r.field].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("data field %[1]s does not contain structured data, can't address a field inside %[1]s", r.field)
	}
	return dataFieldReference{data: data, field: field}, nil
}
