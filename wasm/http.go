// Copyright © 2026 Meroxa, Inc.
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
	"google.golang.org/protobuf/proto"
)

// httpService is the guest-side pprocutils.HTTPService implementation for
// standalone (WASM) processors. It is a verbatim reuse of the schemaService
// pattern: marshal the request into the shared buffer, invoke the host
// import via the existing park/resize protocol (hostCall), and unmarshal the
// buffered response the host wrote back. All I/O and policy enforcement
// (allowlist, resolved-IP dial-time gate, redirect suppression, size cap,
// host-injected credentials) happens host-side; this type only carries bytes
// across the host boundary.
type httpService struct{}

// Do marshals req, calls the conduit.http_request host import, and
// unmarshals the response. A host-side rejection or failure surfaces as a
// *pprocutils.Error carrying one of the ErrorCodeHTTP* codes (see
// pprocutils/errors.go), decoded by hostCall from the numeric error-code band
// exactly as create_schema/get_schema already do.
func (*httpService) Do(_ context.Context, req pprocutils.HTTPRequest) (pprocutils.HTTPResponse, error) {
	protoReq := toproto.HTTPRequest(req)

	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)

	buffer, err := proto.MarshalOptions{}.MarshalAppend(buffer[:0], protoReq)
	if err != nil {
		return pprocutils.HTTPResponse{}, fmt.Errorf("error marshalling request: %w", err)
	}

	buffer, cmdSize, err := hostCall(_httpRequest, buffer)
	if err != nil {
		return pprocutils.HTTPResponse{}, fmt.Errorf("error calling httpRequest: %w", err)
	}

	var resp procutilsv1.HTTPResponse
	err = proto.Unmarshal(buffer[:cmdSize], &resp)
	if err != nil {
		return pprocutils.HTTPResponse{}, fmt.Errorf("failed unmarshalling %v bytes into proto type: %w", cmdSize, err)
	}

	return fromproto.HTTPResponse(&resp), nil
}
