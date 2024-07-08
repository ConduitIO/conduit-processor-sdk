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
	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-processor-sdk/wasm"
)

func Get(subject string, version int) (GetResponse, error) {
	req := GetSchemaRequest(subject, version)
	resp, err := wasm.GetSchema(req)
	if err != nil {
		return GetResponse{}, err
	}
	return ToGetResponse(resp), nil
}

func Create(subject string, typ cschema.Type, bytes []byte) (CreateResponse, error) {
	req := CreateSchemaRequest(subject, typ, bytes)
	resp, err := wasm.CreateSchema(req)
	if err != nil {
		return CreateResponse{}, err
	}
	return ToCreateResponse(resp), nil
}
