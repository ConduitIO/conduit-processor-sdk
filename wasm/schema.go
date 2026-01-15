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

//go:build wasm

package wasm

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	"github.com/conduitio/conduit-processor-sdk/pprocutils/v1/fromproto"
	"github.com/conduitio/conduit-processor-sdk/pprocutils/v1/toproto"
	procutilsv1 "github.com/conduitio/conduit-processor-sdk/proto/procutils/v1"
)

type schemaService struct{}

func (*schemaService) CreateSchema(_ context.Context, req pprocutils.CreateSchemaRequest) (pprocutils.CreateSchemaResponse, error) {
	protoReq := toproto.CreateSchemaRequest(req)
	var resp procutilsv1.CreateSchemaResponse

	err := handleImportedCall(_createSchema, protoReq, &resp)
	if err != nil {
		return pprocutils.CreateSchemaResponse{}, fmt.Errorf("error calling createSchema: %w", err)
	}

	return fromproto.CreateSchemaResponse(&resp), nil
}

func (*schemaService) GetSchema(_ context.Context, req pprocutils.GetSchemaRequest) (pprocutils.GetSchemaResponse, error) {
	protoReq := toproto.GetSchemaRequest(req)
	var resp procutilsv1.GetSchemaResponse

	err := handleImportedCall(_getSchema, protoReq, &resp)
	if err != nil {
		return pprocutils.GetSchemaResponse{}, fmt.Errorf("error calling getSchema: %w", err)
	}

	return fromproto.GetSchemaResponse(&resp), nil
}
