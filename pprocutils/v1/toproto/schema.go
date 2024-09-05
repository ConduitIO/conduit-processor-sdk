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
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	procutilsv1 "github.com/conduitio/conduit-processor-sdk/proto/procutils/v1"
)

func GetSchemaRequest(in pprocutils.GetSchemaRequest) *procutilsv1.GetSchemaRequest {
	return &procutilsv1.GetSchemaRequest{
		Subject: in.Subject,
		Version: int32(in.Version), //nolint:gosec // no risk of overflow
	}
}

func GetSchemaResponse(in pprocutils.GetSchemaResponse) *procutilsv1.GetSchemaResponse {
	return &procutilsv1.GetSchemaResponse{
		Schema: &schemav1.Schema{
			Id:      int32(in.Schema.ID), //nolint:gosec // no risk of overflow
			Subject: in.Schema.Subject,
			Version: int32(in.Schema.Version), //nolint:gosec // no risk of overflow
			Type:    schemav1.Schema_Type(in.Schema.Type),
			Bytes:   in.Schema.Bytes,
		},
	}
}

func CreateSchemaRequest(in pprocutils.CreateSchemaRequest) *procutilsv1.CreateSchemaRequest {
	return &procutilsv1.CreateSchemaRequest{
		Subject: in.Subject,
		Type:    schemav1.Schema_Type(in.Type),
		Bytes:   in.Bytes,
	}
}

func CreateSchemaResponse(in pprocutils.CreateSchemaResponse) *procutilsv1.CreateSchemaResponse {
	return &procutilsv1.CreateSchemaResponse{
		Schema: &schemav1.Schema{
			Id:      int32(in.Schema.ID), //nolint:gosec // no risk of overflow
			Subject: in.Schema.Subject,
			Version: int32(in.Schema.Version), //nolint:gosec // no risk of overflow
			Type:    schemav1.Schema_Type(in.Schema.Type),
			Bytes:   in.Schema.Bytes,
		},
	}
}
