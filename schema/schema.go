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

package schema

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
)

// TypeAvro is the Avro schema type. It is currently the only type accepted by
// [Create].
const TypeAvro = schema.TypeAvro

// SchemaService is the service backing [Get] and [Create]. In a standalone
// (WebAssembly) processor the engine overwrites this at startup with a client
// for Conduit's schema registry; otherwise it defaults to an in-process,
// cache-wrapped in-memory store. Replace it in tests to stub schema resolution.
var SchemaService pprocutils.SchemaService = newCachedSchemaService(NewInMemoryService())

var (
	// ErrSubjectNotFound is returned by [Get] when no schema exists for the
	// requested subject.
	ErrSubjectNotFound = pprocutils.ErrSubjectNotFound
	// ErrVersionNotFound is returned by [Get] when the subject exists but the
	// requested version does not.
	ErrVersionNotFound = pprocutils.ErrVersionNotFound
	// ErrInvalidSchema is returned by [Create] when the supplied bytes are not a
	// valid schema of the requested type.
	ErrInvalidSchema = pprocutils.ErrInvalidSchema
)

// Get fetches the schema registered under the given subject and version.
// Versions start at 1. It returns [ErrSubjectNotFound] or [ErrVersionNotFound]
// (wrapped) if the schema is not registered.
func Get(ctx context.Context, subject string, version int) (schema.Schema, error) {
	resp, err := SchemaService.GetSchema(ctx, pprocutils.GetSchemaRequest{
		Subject: subject,
		Version: version,
	})
	if err != nil {
		return schema.Schema{}, fmt.Errorf("error getting schema: %w", err)
	}
	return resp.Schema, nil
}

// Create registers a new version of the schema for the given subject and
// returns it with its assigned ID and version. Each call to Create appends a new
// version; there is no deduplication of identical bytes. Only [TypeAvro] is
// currently accepted — other types return [ErrInvalidSchema].
func Create(ctx context.Context, typ schema.Type, subject string, bytes []byte) (schema.Schema, error) {
	resp, err := SchemaService.CreateSchema(ctx, pprocutils.CreateSchemaRequest{
		Subject: subject,
		Type:    typ,
		Bytes:   bytes,
	})
	if err != nil {
		return schema.Schema{}, fmt.Errorf("error creating schema: %w", err)
	}
	return resp.Schema, nil
}
