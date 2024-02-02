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

// Reference is an interface that represents a reference to a field in a record.
// It can be used to get and set the value of the field dynamically using input
// provided by the user.
type Reference interface {
	// Get returns the value of the reference. The type of the returned value
	// depends on the reference.
	Get() any
	// Set sets the value of the reference. The type of the value depends on the
	// reference. If the value is not of the expected type, an error is returned.
	Set(any) error

	walk(field string) (Reference, error)
}

// Resolver is a type that knows how to resolve a reference to a field
// in a record. It is used to specify the target of a processor's output.
type Resolver struct {
	raw    string
	fields []string
}

func NewResolver(input string) (Resolver, error) {
	l := newLexer(input)

	i := l.Next()
	if i.typ == itemVariable || i.typ == itemDot {
		// skip variable token, if any
		i = l.Next()
	}

	// dummy record reference to validate if fields can be resolved
	var ref Reference = recordReference{rec: &opencdc.Record{}}

	var fields []string
	for i.typ != itemEOF && i.typ != itemError {
		var field string

		switch i.typ {
		case itemField:
			field = i.val[1:] // skip leading dot
		case itemLeftBracket:
			if len(fields) == 0 {
				// no field to index
				return Resolver{}, fmt.Errorf("invalid reference %q: unexpected token %s", input, i)
			}
			i = l.Next()
			if i.typ != itemString {
				return Resolver{}, fmt.Errorf("invalid reference %q: unexpected token %s", input, i)
			}

			field = i.val[1 : len(i.val)-1] // remove quotes

			i = l.Next()
			if i.typ != itemRightBracket {
				return Resolver{}, fmt.Errorf("invalid reference %q: unexpected token %s", input, i)
			}
		default:
			return Resolver{}, fmt.Errorf("invalid reference %q: unexpected token %s", input, i)
		}

		// validate field name in context of the record
		var err error
		ref, err = ref.walk(field)
		if err != nil {
			return Resolver{}, fmt.Errorf("invalid reference %q: %w", input, err)
		}

		fields = append(fields, field)
		i = l.Next()
	}

	if i.typ == itemError {
		return Resolver{}, fmt.Errorf("invalid reference %q: %s", input, i.val)
	}

	return Resolver{
		raw:    input,
		fields: fields,
	}, nil
}

// Resolve resolves the reference to a field in the record. If the reference
// cannot be resolved an error is returned. If the reference is valid but the
// field does not exist in the record, the field will be created.
// The returned reference can be used to set the value of the field.
func (rr Resolver) Resolve(rec *opencdc.Record) (Reference, error) {
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

func (r recordReference) Set(any) error {
	return errors.New("cannot set entire record")
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

func (r positionReference) Set(any) error {
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

func (r operationReference) Set(val any) error {
	var op opencdc.Operation

	switch val := val.(type) {
	case opencdc.Operation, int:
		op = opencdc.Operation(val.(int))
	case string:
		err := op.UnmarshalText([]byte(val))
		if err != nil {
			return fmt.Errorf("failed to set operation: %w", err)
		}
	default:
		return fmt.Errorf("unexpected type %T for operation", val)
	}

	if op < opencdc.OperationCreate || op > opencdc.OperationSnapshot {
		return fmt.Errorf("operation %q: %w", op, opencdc.ErrUnknownOperation)
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

func (r metadataReference) Set(val any) error {
	switch val := val.(type) {
	case opencdc.Metadata:
		r.rec.Metadata = val
	case map[string]string:
		r.rec.Metadata = val
	case nil:
		r.rec.Metadata = nil
	default:
		return fmt.Errorf("unexpected type %T for metadata", val)
	}
	return nil
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

func (r metadataFieldReference) Set(val any) error {
	switch val := val.(type) {
	case string:
		r.rec.Metadata[r.field] = val
	default:
		return fmt.Errorf("unexpected type %T for metadata", val)
	}
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

func (r keyReference) Set(val any) error {
	switch val := val.(type) {
	case opencdc.RawData, opencdc.StructuredData:
		r.rec.Key = val.(opencdc.Data)
	case string:
		r.rec.Key = opencdc.RawData(val)
	case []byte:
		r.rec.Key = opencdc.RawData(val)
	case map[string]any:
		r.rec.Key = opencdc.StructuredData(val)
	case nil:
		r.rec.Key = nil
	default:
		return fmt.Errorf("unexpected type %T for key", val)
	}
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

func (r payloadReference) Set(val any) error {
	switch val := val.(type) {
	case opencdc.Change:
		r.rec.Payload = val
	default:
		return fmt.Errorf("unexpected type %T for payload", val)
	}
	return nil
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

func (r payloadBeforeReference) Set(val any) error {
	switch val := val.(type) {
	case opencdc.RawData, opencdc.StructuredData:
		r.rec.Payload.Before = val.(opencdc.Data)
	case string:
		r.rec.Payload.Before = opencdc.RawData(val)
	case []byte:
		r.rec.Payload.Before = opencdc.RawData(val)
	case map[string]any:
		r.rec.Payload.Before = opencdc.StructuredData(val)
	case nil:
		r.rec.Payload.Before = nil
	default:
		return fmt.Errorf("unexpected type %T for payload before", val)
	}
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

func (r payloadAfterReference) Set(val any) error {
	switch val := val.(type) {
	case opencdc.RawData, opencdc.StructuredData:
		r.rec.Payload.After = val.(opencdc.Data)
	case string:
		r.rec.Payload.After = opencdc.RawData(val)
	case []byte:
		r.rec.Payload.After = opencdc.RawData(val)
	case map[string]any:
		r.rec.Payload.After = opencdc.StructuredData(val)
	case nil:
		r.rec.Payload.After = nil
	default:
		return fmt.Errorf("unexpected type %T for payload after", val)
	}
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

func (r dataFieldReference) Set(val any) error {
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
