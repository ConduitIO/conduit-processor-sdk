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
	"fmt"

	conduitv1 "github.com/conduitio/conduit-processor-sdk/proto/conduit/v1"
	"google.golang.org/protobuf/proto"
)

func CreateSchema(req *conduitv1.CreateSchemaRequest) (*conduitv1.CreateSchemaResponse, error) {
	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)

	buffer, err := proto.MarshalOptions{}.MarshalAppend(buffer[:0], req)
	if err != nil {
		return nil, fmt.Errorf("error marshalling request: %w", err)
	}

	buffer, cmdSize, err := hostCall(_createSchema, buffer)
	if cmdSize >= ErrorCodeStart {
		return nil, NewErrorFromCode(cmdSize)
	}

	var resp conduitv1.CreateSchemaResponse
	err = proto.Unmarshal(buffer[:cmdSize], &resp)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling %v bytes into proto type: %w", cmdSize, err)
	}
	return &resp, nil

}

func GetSchema(req *conduitv1.GetSchemaRequest) (*conduitv1.GetSchemaResponse, error) {
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
