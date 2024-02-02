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
	"context"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-processor-sdk/internal"
	"github.com/conduitio/conduit-processor-sdk/internal/reference"
	"github.com/rs/zerolog"
)

// Logger returns the logger for the current context. Please provide the context
// that is passed to any of the processor's methods (Configure, Open, Process,
// Teardown).
func Logger(ctx context.Context) *zerolog.Logger {
	// TODO if there is no util return a default logger
	return internal.UtilFromContext(ctx).Logger(ctx)
}

// Reference is an interface that represents a reference to a field in a record.
// It can be used to get and set the value of the field dynamically using input
// provided by the user.
type Reference reference.Reference

// ReferenceResolver is a type that knows how to resolve a reference to a field
// in a record. It is used to specify the target of a processor's output.
type ReferenceResolver reference.Resolver

// Resolve resolves the reference to a field in the record. If the reference
// cannot be resolved an error is returned. If the reference is valid but the
// field does not exist in the record, the field will be created.
// The returned reference can be used to set the value of the field.
func (r ReferenceResolver) Resolve(rec *opencdc.Record) (Reference, error) {
	return reference.Resolver(r).Resolve(rec)
}

// NewReferenceResolver creates a new reference resolver from the input string.
// The input string is a reference to a field in a record. It can be a simple
// field name or a path to a nested field. The returned resolver can be used to
// resolve a reference to the specified field in a record and manipulate that
// field (get or set the value).
//
// Examples of valid references include:
//   - .Position
//   - .Operation
//   - .Key
//   - .Metadata.foo (to access a simple metadata value)
//   - .Metadata["f.o.123"] (to access a metadata value via a key containing non-alpha-numeric characters)
//   - .Payload.Before.foo (to access a nested field in payload before)
//   - .Payload.After["1"]["2"] (to access nested fields in payload after containing non-alpha-numeric characters)
func NewReferenceResolver(input string) (ReferenceResolver, error) {
	resolver, err := reference.NewResolver(input)
	return ReferenceResolver(resolver), err
}
