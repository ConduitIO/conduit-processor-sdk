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
	"strings"

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
	// Delete deletes the value of the reference. If the reference is not
	// deletable, an error is returned.
	Delete() error
	// Rename renames the referenced field, the old field value is moved to the
	// new field. The reference is still pointing to the old field, while the returned
	// reference points to the new field.
	Rename(string) (Reference, error)

	walk(field string) (Reference, error)
}

// Resolver is a type that knows how to resolve a reference to a field
// in a record. It is used to specify the target of a processor's output.
type Resolver struct {
	raw    string
	fields []string
}

func NewResolver(input string) (Resolver, error) {
	l := newLexer(cleanInput(input))

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

		switch i.typ { //nolint:exhaustive // we only care about these tokens
		case itemField:
			field = i.val[1:] // skip leading dot
		case itemLeftBracket:
			if len(fields) == 0 {
				// no field to index
				return Resolver{}, fmt.Errorf("invalid reference %q: %w", input, UnexpectedTokenError{i: i})
			}
			i = l.Next()
			if i.typ != itemString {
				return Resolver{}, fmt.Errorf("invalid reference %q: %w", input, UnexpectedTokenError{i: i})
			}

			field = i.val[1 : len(i.val)-1] // remove quotes

			i = l.Next()
			if i.typ != itemRightBracket {
				return Resolver{}, fmt.Errorf("invalid reference %q: %w", input, UnexpectedTokenError{i: i})
			}
		default:
			return Resolver{}, fmt.Errorf("invalid reference %q: %w", input, UnexpectedTokenError{i: i})
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

func cleanInput(input string) string {
	replacePrefix := func(s string, replacements map[string]string) string {
		for prefix, replacement := range replacements {
			if strings.HasPrefix(s, prefix) {
				return replacement + s[len(prefix):]
			}
		}
		return s
	}
	input = replacePrefix(input, map[string]string{
		".payload":   ".Payload",
		".key":       ".Key",
		".metadata":  ".Metadata",
		".position":  ".Position",
		".operation": ".Operation",
	})
	if strings.HasPrefix(input, ".Payload") {
		input = replacePrefix(input, map[string]string{
			".Payload.before": ".Payload.Before",
			".Payload.after":  ".Payload.After",
		})
	}
	return input
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
			return nil, fmt.Errorf("could not resolve field %q: %w", field, err)
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
	return fmt.Errorf("cannot set record: %w", ErrImmutableReference)
}

func (r recordReference) Delete() error {
	return fmt.Errorf("cannot delete record: %w", ErrImmutableReference)
}

func (r recordReference) Rename(string) (Reference, error) {
	return nil, fmt.Errorf("cannot rename record: %w", ErrRenameImmutableReference)
}

func (r recordReference) walk(field string) (Reference, error) {
	switch field {
	case "Position":
		return positionReference(r), nil
	case "Operation":
		return operationReference(r), nil
	case "Metadata":
		return metadataReference(r), nil
	case "Key":
		return keyReference(r), nil
	case "Payload":
		return payloadReference(r), nil
	default:
		return nil, fmt.Errorf("unexpected field %q: %w", field, ErrNotResolvable)
	}
}

type positionReference struct {
	rec *opencdc.Record
}

func (r positionReference) Get() any {
	return r.rec.Position
}

func (r positionReference) Set(any) error {
	return fmt.Errorf("cannot set .Position: %w", ErrImmutableReference)
}

func (r positionReference) Delete() error {
	return fmt.Errorf("cannot delete .Position: %w", ErrImmutableReference)
}

func (r positionReference) Rename(string) (Reference, error) {
	return nil, fmt.Errorf("cannot rename .Position: %w", ErrRenameImmutableReference)
}

func (r positionReference) walk(string) (Reference, error) {
	return nil, fmt.Errorf("position is not a structured field: %w", ErrNotResolvable)
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
	case opencdc.Operation:
		op = val
	case int:
		op = opencdc.Operation(val)
	case string:
		err := op.UnmarshalText([]byte(val))
		if err != nil {
			return fmt.Errorf("failed to set .Operation: %w", err)
		}
	default:
		return fmt.Errorf("can't set type %T for .Operation: %w", val, ErrUnexpectedType)
	}

	if op < opencdc.OperationCreate || op > opencdc.OperationSnapshot {
		return fmt.Errorf("operation %q: %w", op, opencdc.ErrUnknownOperation)
	}
	r.rec.Operation = op

	return nil
}

func (r operationReference) Delete() error {
	return fmt.Errorf("cannot delete operation: %w", ErrImmutableReference)
}

func (r operationReference) Rename(string) (Reference, error) {
	return nil, fmt.Errorf("cannot rename operation: %w", ErrRenameImmutableReference)
}

func (r operationReference) walk(string) (Reference, error) {
	return nil, fmt.Errorf(".Operation is not a structured field: %w", ErrNotResolvable)
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
		r.rec.Metadata = opencdc.Metadata{} // reset metadata
	default:
		return fmt.Errorf("can't set type %T for .Metadata: %w", val, ErrUnexpectedType)
	}
	return nil
}

func (r metadataReference) Delete() error {
	r.rec.Metadata = opencdc.Metadata{} // reset metadata
	return nil
}

func (r metadataReference) Rename(string) (Reference, error) {
	return nil, fmt.Errorf("cannot rename .Metadata: %w", ErrRenameImmutableReference)
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
	val, ok := r.rec.Metadata[r.field]
	if !ok {
		return nil
	}
	return val
}

func (r metadataFieldReference) Set(val any) error {
	switch val := val.(type) {
	case string:
		r.rec.Metadata[r.field] = val
	case nil:
		delete(r.rec.Metadata, r.field) // delete field
	default:
		return fmt.Errorf("can't set type %T for .Metadata: %w", val, ErrUnexpectedType)
	}
	return nil
}

func (r metadataFieldReference) Delete() error {
	delete(r.rec.Metadata, r.field)
	return nil
}

func (r metadataFieldReference) Rename(name string) (Reference, error) {
	_, ok := r.rec.Metadata[name]
	if ok {
		return nil, fmt.Errorf("cannot rename, field %q: %w", name, ErrFieldExists)
	}
	r.rec.Metadata[name] = r.rec.Metadata[r.field]
	delete(r.rec.Metadata, r.field)
	return metadataFieldReference{
		rec:   r.rec,
		field: name,
	}, nil
}

func (r metadataFieldReference) walk(string) (Reference, error) {
	return nil, fmt.Errorf(".Metadata fields are not structured: %w", ErrNotResolvable)
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
		r.rec.Key = val.(opencdc.Data) //nolint:forcetypeassert // we know it's the right type
	case string:
		r.rec.Key = opencdc.RawData(val)
	case []byte:
		r.rec.Key = opencdc.RawData(val)
	case map[string]any:
		r.rec.Key = opencdc.StructuredData(val)
	case nil:
		r.rec.Key = nil
	default:
		return fmt.Errorf("can't set type %T for .Key: %w", val, ErrUnexpectedType)
	}
	return nil
}

func (r keyReference) Delete() error {
	r.rec.Key = nil
	return nil
}

func (r keyReference) Rename(string) (Reference, error) {
	return nil, fmt.Errorf("cannot rename .Key: %w", ErrRenameImmutableReference)
}

func (r keyReference) walk(field string) (Reference, error) {
	switch r.rec.Key.(type) {
	case opencdc.StructuredData:
		// all good
	case nil:
		// create new structured data
		r.rec.Key = opencdc.StructuredData{}
	default:
		return nil, fmt.Errorf(".Key does not contain structured data: %w", ErrNotResolvable)
	}
	return dataFieldReference{
		data:  r.rec.Key.(opencdc.StructuredData), //nolint:forcetypeassert // we know it's the right type
		field: field,
		path:  ".Key",
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
		return fmt.Errorf("can't set type %T for .Payload: %w", val, ErrUnexpectedType)
	}
	return nil
}

func (r payloadReference) Delete() error {
	r.rec.Payload = opencdc.Change{}
	return nil
}

func (r payloadReference) Rename(string) (Reference, error) {
	return nil, fmt.Errorf("cannot rename .Payload: %w", ErrRenameImmutableReference)
}

func (r payloadReference) walk(field string) (Reference, error) {
	switch field {
	case "Before":
		return payloadBeforeReference(r), nil
	case "After":
		return payloadAfterReference(r), nil
	default:
		return nil, fmt.Errorf("unexpected field %q: %w", field, ErrNotResolvable)
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
		r.rec.Payload.Before = val.(opencdc.Data) //nolint:forcetypeassert // we know it's the right type
	case string:
		r.rec.Payload.Before = opencdc.RawData(val)
	case []byte:
		r.rec.Payload.Before = opencdc.RawData(val)
	case map[string]any:
		r.rec.Payload.Before = opencdc.StructuredData(val)
	case nil:
		r.rec.Payload.Before = nil
	default:
		return fmt.Errorf("can't set type %T for .Payload.Before: %w", val, ErrUnexpectedType)
	}
	return nil
}

func (r payloadBeforeReference) Delete() error {
	r.rec.Payload.Before = nil
	return nil
}

func (r payloadBeforeReference) Rename(string) (Reference, error) {
	return nil, fmt.Errorf("cannot rename .Payload.Before: %w", ErrRenameImmutableReference)
}

func (r payloadBeforeReference) walk(field string) (Reference, error) {
	switch r.rec.Payload.Before.(type) {
	case opencdc.StructuredData:
		// all good
	case nil:
		// create new structured data
		r.rec.Payload.Before = opencdc.StructuredData{}
	default:
		return nil, fmt.Errorf(".Payload.Before does not contain structured data: %w", ErrNotResolvable)
	}
	return dataFieldReference{
		data:  r.rec.Payload.Before.(opencdc.StructuredData), //nolint:forcetypeassert // we know it's the right type
		field: field,
		path:  ".Payload.Before",
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
		r.rec.Payload.After = val.(opencdc.Data) //nolint:forcetypeassert // we know it's the right type
	case string:
		r.rec.Payload.After = opencdc.RawData(val)
	case []byte:
		r.rec.Payload.After = opencdc.RawData(val)
	case map[string]any:
		r.rec.Payload.After = opencdc.StructuredData(val)
	case nil:
		r.rec.Payload.After = nil
	default:
		return fmt.Errorf("can't set type %T for .Payload.After: %w", val, ErrUnexpectedType)
	}
	return nil
}

func (r payloadAfterReference) Delete() error {
	r.rec.Payload.After = nil
	return nil
}

func (r payloadAfterReference) Rename(string) (Reference, error) {
	return nil, fmt.Errorf("cannot rename .Payload.After: %w", ErrRenameImmutableReference)
}

func (r payloadAfterReference) walk(field string) (Reference, error) {
	switch r.rec.Payload.After.(type) {
	case opencdc.StructuredData:
		// all good
	case nil:
		// create new structured data
		r.rec.Payload.After = opencdc.StructuredData{}
	default:
		return nil, fmt.Errorf(".Payload.After does not contain structured data: %w", ErrNotResolvable)
	}
	return dataFieldReference{
		data:  r.rec.Payload.After.(opencdc.StructuredData), //nolint:forcetypeassert // we know it's the right type
		field: field,
		path:  ".Payload.After",
	}, nil
}

type dataFieldReference struct {
	data  map[string]any
	field string
	path  string
}

func (r dataFieldReference) Get() any {
	return r.data[r.field]
}

func (r dataFieldReference) Set(val any) error {
	r.data[r.field] = val
	return nil
}

func (r dataFieldReference) Delete() error {
	delete(r.data, r.field)
	return nil
}

func (r dataFieldReference) Rename(name string) (Reference, error) {
	_, ok := r.data[name]
	if ok {
		return nil, fmt.Errorf("cannot rename, field %q: %w", name, ErrFieldExists)
	}
	r.data[name] = r.data[r.field]
	delete(r.data, r.field)
	return dataFieldReference{
		data:  r.data,
		field: name,
		path:  r.path + "." + r.field,
	}, nil
}

func (r dataFieldReference) walk(field string) (Reference, error) {
	if r.data[r.field] == nil {
		// create new structured data for field, so we can set the field
		r.data[r.field] = make(map[string]any)
	}

	data, ok := r.data[r.field].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s.%s does not contain structured data: %w", r.path, r.field, ErrNotResolvable)
	}
	return dataFieldReference{
		data:  data,
		field: field,
		path:  r.path + "." + r.field,
	}, nil
}
