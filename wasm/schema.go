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

	"github.com/conduitio/conduit-processor-sdk/conduit"
	"github.com/conduitio/conduit-processor-sdk/conduit/v1/fromproto"
	"github.com/conduitio/conduit-processor-sdk/conduit/v1/toproto"

	conduitv1 "github.com/conduitio/conduit-processor-sdk/proto/conduit/v1"
	"google.golang.org/protobuf/proto"
)

type schemaService struct{}

func (*schemaService) CreateSchema(_ context.Context, req conduit.CreateSchemaRequest) (conduit.CreateSchemaResponse, error) {
	protoReq := toproto.CreateSchemaRequest(req)

	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)

	buffer, err := proto.MarshalOptions{}.MarshalAppend(buffer[:0], protoReq)
	if err != nil {
		return conduit.CreateSchemaResponse{}, fmt.Errorf("error marshalling request: %w", err)
	}

	buffer, cmdSize, err := hostCall(_createSchema, buffer)
	if cmdSize >= ErrorCodeStart {
		return conduit.CreateSchemaResponse{}, NewErrorFromCode(cmdSize)
	}

	var resp *conduitv1.CreateSchemaResponse
	err = proto.Unmarshal(buffer[:cmdSize], resp)
	if err != nil {
		return conduit.CreateSchemaResponse{}, fmt.Errorf("failed unmarshalling %v bytes into proto type: %w", cmdSize, err)
	}

	return fromproto.CreateSchemaResponse(resp), nil

}

// TODO update the same as CreateSchema
func (*schemaService) GetSchema(_ context.Context, req conduit.GetSchemaRequest) (conduit.GetSchemaResponse, error) {
	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)

	buffer, err := proto.MarshalOptions{}.MarshalAppend(buffer[:0], req)
	if err != nil {
		return nil, fmt.Errorf("error marshalling request: %w", err)
	}

	buffer, cmdSize, err := hostCall(_getSchema, buffer)
	if cmdSize >= ErrorCodeStart {
		return nil, NewErrorFromCode(cmdSize)
	}

	var resp conduitv1.GetSchemaResponse
	err = proto.Unmarshal(buffer[:cmdSize], &resp)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling %v bytes into proto type: %w", cmdSize, err)
	}
	return &resp, nil
}
