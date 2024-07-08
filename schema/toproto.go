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
	schemav1 "github.com/conduitio/conduit-commons/proto/schema/v1"
	cschema "github.com/conduitio/conduit-commons/schema"
	conduitv1 "github.com/conduitio/conduit-processor-sdk/proto/conduit/v1"
)

func GetSchemaRequest(subject string, version int) *conduitv1.GetSchemaRequest {
	return &conduitv1.GetSchemaRequest{
		Subject: subject,
		Version: int32(version),
	}
}

func GetSchemaResponse(inst cschema.Schema) *conduitv1.GetSchemaResponse {
	return &conduitv1.GetSchemaResponse{
		Schema: &schemav1.Schema{
			Subject: inst.Subject,
			Version: int32(inst.Version),
			Type:    schemav1.Schema_Type(inst.Type),
			Bytes:   inst.Bytes,
		},
	}
}

func CreateSchemaRequest(subject string, typ cschema.Type, bytes []byte) *conduitv1.CreateSchemaRequest {
	return &conduitv1.CreateSchemaRequest{
		Subject: subject,
		Type:    schemav1.Schema_Type(typ),
		Bytes:   bytes,
	}
}

func CreateSchemaResponse(inst cschema.Schema) *conduitv1.CreateSchemaResponse {
	return &conduitv1.CreateSchemaResponse{
		Schema: &schemav1.Schema{
			Subject: inst.Subject,
			Version: int32(inst.Version),
			Type:    schemav1.Schema_Type(inst.Type),
			Bytes:   inst.Bytes,
		},
	}
}
