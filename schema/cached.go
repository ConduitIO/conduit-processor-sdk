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
	"time"

	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	"github.com/twmb/go-cache/cache"
)

func newCachedSchemaService(service pprocutils.SchemaService) *cachedSchemaService {
	return &cachedSchemaService{
		SchemaService: service,

		getSchemaCache: cache.New[pprocutils.GetSchemaRequest, pprocutils.GetSchemaResponse](
			cache.MaxAge(15 * time.Minute), // expire entries after 15 minutes
		),
		createSchemaCache: cache.New[comparableCreateSchemaRequest, pprocutils.CreateSchemaResponse](
			cache.MaxAge(15 * time.Minute), // expire entries after 15 minutes
		),
	}
}

type cachedSchemaService struct {
	pprocutils.SchemaService

	getSchemaCache    *cache.Cache[pprocutils.GetSchemaRequest, pprocutils.GetSchemaResponse]
	createSchemaCache *cache.Cache[comparableCreateSchemaRequest, pprocutils.CreateSchemaResponse]
}

type comparableCreateSchemaRequest struct {
	Subject string
	Type    schema.Type
	Bytes   string
}

func (c *cachedSchemaService) GetSchema(ctx context.Context, request pprocutils.GetSchemaRequest) (pprocutils.GetSchemaResponse, error) {
	resp, err, _ := c.getSchemaCache.Get(request, func() (pprocutils.GetSchemaResponse, error) {
		return c.SchemaService.GetSchema(ctx, request)
	})
	return resp, err //nolint:wrapcheck // cached service should not wrap errors
}

func (c *cachedSchemaService) CreateSchema(ctx context.Context, request pprocutils.CreateSchemaRequest) (pprocutils.CreateSchemaResponse, error) {
	creq := comparableCreateSchemaRequest{
		Subject: request.Subject,
		Type:    request.Type,
		Bytes:   string(request.Bytes),
	}
	resp, err, _ := c.createSchemaCache.Get(creq, func() (pprocutils.CreateSchemaResponse, error) {
		return c.SchemaService.CreateSchema(ctx, request)
	})
	return resp, err //nolint:wrapcheck // cached service should not wrap errors
}
