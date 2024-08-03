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

const TypeAvro = schema.TypeAvro

var SchemaService pprocutils.SchemaService = newCachedSchemaService(NewInMemoryService())

var (
	ErrSubjectNotFound = pprocutils.ErrSubjectNotFound
	ErrVersionNotFound = pprocutils.ErrVersionNotFound
	ErrInvalidSchema   = pprocutils.ErrInvalidSchema
)

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
