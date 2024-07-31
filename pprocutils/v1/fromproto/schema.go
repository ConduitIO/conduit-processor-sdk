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

package fromproto

import (
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	procutilsv1 "github.com/conduitio/conduit-processor-sdk/proto/procutils/v1"
)

func CreateSchemaRequest(req *procutilsv1.CreateSchemaRequest) pprocutils.CreateSchemaRequest {
	return pprocutils.CreateSchemaRequest{
		Subject: req.Subject,
		Type:    schema.Type(req.Type),
		Bytes:   req.Bytes,
	}
}

func GetSchemaRequest(req *procutilsv1.GetSchemaRequest) pprocutils.GetSchemaRequest {
	return pprocutils.GetSchemaRequest{
		Subject: req.Subject,
		Version: int(req.Version),
	}
}

func CreateSchemaResponse(resp *procutilsv1.CreateSchemaResponse) pprocutils.CreateSchemaResponse {
	return pprocutils.CreateSchemaResponse{
		Schema: schema.Schema{
			ID:      int(resp.Schema.Id),
			Subject: resp.Schema.Subject,
			Version: int(resp.Schema.Version),
			Type:    schema.Type(resp.Schema.Type),
			Bytes:   resp.Schema.Bytes,
		},
	}
}

func GetSchemaResponse(resp *procutilsv1.GetSchemaResponse) pprocutils.GetSchemaResponse {
	return pprocutils.GetSchemaResponse{
		Schema: schema.Schema{
			ID:      int(resp.Schema.Id),
			Subject: resp.Schema.Subject,
			Version: int(resp.Schema.Version),
			Type:    schema.Type(resp.Schema.Type),
			Bytes:   resp.Schema.Bytes,
		},
	}
}
