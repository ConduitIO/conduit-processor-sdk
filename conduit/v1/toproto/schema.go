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

package toproto

import (
	schemav1 "github.com/conduitio/conduit-commons/proto/schema/v1"
	"github.com/conduitio/conduit-processor-sdk/conduit"
	conduitv1 "github.com/conduitio/conduit-processor-sdk/proto/conduit/v1"
)

func GetSchemaRequest(in conduit.GetSchemaRequest) *conduitv1.GetSchemaRequest {
	return &conduitv1.GetSchemaRequest{
		Subject: in.Subject,
		Version: int32(in.Version),
	}
}

func GetSchemaResponse(in conduit.GetSchemaResponse) *conduitv1.GetSchemaResponse {
	return &conduitv1.GetSchemaResponse{
		Schema: &schemav1.Schema{
			Subject: in.Schema.Subject,
			Version: int32(in.Schema.Version),
			Type:    schemav1.Schema_Type(in.Schema.Type),
			Bytes:   in.Schema.Bytes,
		},
	}
}

func CreateSchemaRequest(in conduit.CreateSchemaRequest) *conduitv1.CreateSchemaRequest {
	return &conduitv1.CreateSchemaRequest{
		Subject: in.Subject,
		Type:    schemav1.Schema_Type(in.Type),
		Bytes:   in.Bytes,
	}
}

func CreateSchemaResponse(in conduit.CreateSchemaResponse) *conduitv1.CreateSchemaResponse {
	return &conduitv1.CreateSchemaResponse{
		Schema: &schemav1.Schema{
			Subject: in.Schema.Subject,
			Version: int32(in.Schema.Version),
			Type:    schemav1.Schema_Type(in.Schema.Type),
			Bytes:   in.Schema.Bytes,
		},
	}
}
