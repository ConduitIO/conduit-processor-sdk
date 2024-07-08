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
	cschema "github.com/conduitio/conduit-commons/schema"
	conduitv1 "github.com/conduitio/conduit-processor-sdk/proto/conduit/v1"
)

func ToCreateRequest(req *conduitv1.CreateSchemaRequest) CreateRequest {
	return CreateRequest{
		Subject: req.Subject,
		Type:    cschema.Type(req.Type),
		Bytes:   req.Bytes,
	}
}

func ToCreateResponse(resp *conduitv1.CreateSchemaResponse) CreateResponse {
	return CreateResponse{
		Schema: cschema.Schema{
			Subject: resp.Schema.Subject,
			Version: int(resp.Schema.Version),
			Type:    cschema.Type(resp.Schema.Type),
			Bytes:   resp.Schema.Bytes,
		},
	}
}

func ToGetResponse(resp *conduitv1.GetSchemaResponse) GetResponse {
	return GetResponse{
		Schema: cschema.Schema{
			Subject: resp.Schema.Subject,
			Version: int(resp.Schema.Version),
			Type:    cschema.Type(resp.Schema.Type),
			Bytes:   resp.Schema.Bytes,
		},
	}
}
