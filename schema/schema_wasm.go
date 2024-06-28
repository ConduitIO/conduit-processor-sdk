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

package schema

import (
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"github.com/conduitio/conduit-processor-sdk/wasm"
)

// todo: change these method's fields to accept schema subject and version and so, instead of the proto requests.
// will be done after the proto conversion methods are added to conduit-commons

func Get(req *processorv1.GetSchemaRequest) (*processorv1.GetSchemaResponse, error) {
	schema, err := wasm.GetSchema(req)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func Create(req *processorv1.CreateSchemaRequest) (*processorv1.CreateSchemaResponse, error) {
	schema, err := wasm.CreateSchema(req)
	if err != nil {
		return nil, err
	}
	return schema, nil
}
