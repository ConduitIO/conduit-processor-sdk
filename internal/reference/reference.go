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

const (
	refKey           = ".Key"
	refPosition      = ".Position"
	refOperation     = ".Operation"
	refPayloadBefore = ".Payload.Before"
	refPayloadAfter  = ".Payload.After"
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
		".key":       refKey,
		".metadata":  ".Metadata",
		".position":  refPosition,  //nolint:goconst // canonicalization token, clearest inline
		".operation": refOperation, //nolint:goconst // canonicalization token, clearest inline
	})
	if strings.HasPrefix(input, ".Payload") {
		input = replacePrefix(input, map[string]string{
			".Payload.before": refPayloadBefore,
			".Payload.after":  refPayloadAfter,
		})
	}
	return input
}

// Resolve resolves the reference to a field in the record. If the reference
// cannot be resolved an error is returned. Resolving does not change the
// record. If the field or its parents (a nil key or payload part, or a nil
// nested field) do not exist, Get returns nil and Delete does nothing, and
// Set or Rename create them as empty structured data first.
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
	return structuredFieldReference(&r.rec.Key, refKey, field)
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
	return structuredFieldReference(&r.rec.Payload.Before, refPayloadBefore, field)
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
	return structuredFieldReference(&r.rec.Payload.After, refPayloadAfter, field)
}

// structuredFieldReference returns a reference to field in *data, the key or
// a payload part of a record, found at path. If *data is nil (or a nil
// StructuredData), the reference is pending: resolving it does not change the
// record, and *data is created as an empty StructuredData only when the field
// is set or renamed.
//
// Invariant 6: a nil key, before or after means the record has none (a
// delete has no after, a create no before). Resolving a field in it to read
// or delete it must not turn it into {}, a value the record never had.
func structuredFieldReference(data *opencdc.Data, path, field string) (Reference, error) {
	notStructured := func() error {
		switch (*data).(type) {
		case opencdc.StructuredData, nil:
			return nil
		default:
			return fmt.Errorf("%s does not contain structured data: %w", path, ErrNotResolvable)
		}
	}
	// existing returns *data if it is non-nil structured data, otherwise nil.
	existing := func() opencdc.StructuredData {
		d, _ := (*data).(opencdc.StructuredData)
		return d
	}
	if err := notStructured(); err != nil {
		return nil, err
	}
	if d := existing(); d != nil {
		return dataFieldReference{data: d, field: field, path: path}, nil
	}
	return pendingFieldReference{
		find: func() map[string]any {
			return existing()
		},
		create: func() (map[string]any, error) {
			if err := notStructured(); err != nil {
				return nil, err
			}
			if d := existing(); d != nil {
				return d, nil
			}
			d := opencdc.StructuredData{}
			*data = d
			return d, nil
		},
		field: field,
		path:  path,
	}, nil
}

// pendingFieldReference refers to a field whose parent does not exist yet:
// a nil key or payload part, or a nil nested field. Get and Delete never
// change the record: while the parent does not exist, Get returns nil and
// Delete does nothing. Set and Rename first create the missing parents as
// empty structured data, then act like dataFieldReference.
type pendingFieldReference struct {
	// find returns the parent map if it exists now, or nil.
	find func() map[string]any
	// create returns the parent map, creating it and its missing parents.
	create func() (map[string]any, error)
	field  string
	path   string
}

func (r pendingFieldReference) Get() any {
	if parent := r.find(); parent != nil {
		return parent[r.field]
	}
	return nil
}

func (r pendingFieldReference) Set(val any) error {
	ref, err := r.materialize()
	if err != nil {
		return err
	}
	return ref.Set(val)
}

func (r pendingFieldReference) Delete() error {
	if parent := r.find(); parent != nil {
		delete(parent, r.field)
	}
	return nil
}

func (r pendingFieldReference) Rename(name string) (Reference, error) {
	ref, err := r.materialize()
	if err != nil {
		return nil, err
	}
	return ref.Rename(name)
}

func (r pendingFieldReference) materialize() (dataFieldReference, error) {
	data, err := r.create()
	if err != nil {
		return dataFieldReference{}, err
	}
	return dataFieldReference{data: data, field: r.field, path: r.path}, nil
}

func (r pendingFieldReference) walk(field string) (Reference, error) {
	return pendingFieldReference{
		find: func() map[string]any {
			parent := r.find()
			if parent == nil {
				return nil
			}
			child, _ := parent[r.field].(map[string]any)
			return child
		},
		create: func() (map[string]any, error) {
			ref, err := r.materialize()
			if err != nil {
				return nil, err
			}
			return ref.child()
		},
		field: field,
		path:  r.path + "." + r.field,
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
	if v := r.data[r.field]; v == nil || isNilMap(v) {
		// Invariant 6: a nested field that does not exist (or is nil) is
		// created only if the reference is set or renamed, not on a read or
		// a delete (see pendingFieldReference).
		return pendingFieldReference{
			find: func() map[string]any {
				child, _ := r.data[r.field].(map[string]any)
				return child
			},
			create: r.child,
			field:  field,
			path:   r.path + "." + r.field,
		}, nil
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

// child returns the map stored in the referenced field, creating an empty one
// if the field does not exist or is nil.
func (r dataFieldReference) child() (map[string]any, error) {
	if v := r.data[r.field]; v == nil || isNilMap(v) {
		r.data[r.field] = make(map[string]any)
	}
	data, ok := r.data[r.field].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s.%s does not contain structured data: %w", r.path, r.field, ErrNotResolvable)
	}
	return data, nil
}

// isNilMap reports whether v is a nil map[string]any, which holds no fields
// and cannot be written to.
func isNilMap(v any) bool {
	m, ok := v.(map[string]any)
	return ok && m == nil
}
