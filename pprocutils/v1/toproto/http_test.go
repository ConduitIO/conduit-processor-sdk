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

package toproto_test

import (
	"testing"

	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	"github.com/conduitio/conduit-processor-sdk/pprocutils/v1/fromproto"
	"github.com/conduitio/conduit-processor-sdk/pprocutils/v1/toproto"
	procutilsv1 "github.com/conduitio/conduit-processor-sdk/proto/procutils/v1"
	"github.com/matryer/is"
	"google.golang.org/protobuf/proto"
)

// TestHTTPRequest_RoundTrip exercises the exact path the guest stub in
// wasm/http.go relies on: a pprocutils.HTTPRequest converted to its proto
// form, marshalled to wire bytes, unmarshalled back, and converted back to a
// pprocutils.HTTPRequest — the same sequence hostCall's buffer protocol
// performs across the host boundary.
func TestHTTPRequest_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  pprocutils.HTTPRequest
	}{
		{
			name: "full request with multi-value headers",
			req: pprocutils.HTTPRequest{
				Method: "POST",
				URL:    "https://api.example.com/v1/embeddings",
				Headers: map[string][]string{
					"Content-Type": {"application/json"},
					"X-Trace-Id":   {"abc", "def"},
				},
				Body:          []byte(`{"input":"hello"}`),
				AuthSecretRef: "openai_api_key",
			},
		},
		{
			name: "no headers, no body, no auth ref",
			req: pprocutils.HTTPRequest{
				Method: "GET",
				URL:    "https://api.example.com/v1/models",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)

			protoReq := toproto.HTTPRequest(tt.req)

			wire, err := proto.Marshal(protoReq)
			is.NoErr(err)

			var decoded procutilsv1.HTTPRequest
			err = proto.Unmarshal(wire, &decoded)
			is.NoErr(err)

			got := fromproto.HTTPRequest(&decoded)
			is.Equal(got, tt.req)
		})
	}
}

// TestHTTPResponse_RoundTrip is the response-side companion to
// TestHTTPRequest_RoundTrip, covering the direction the guest stub decodes:
// host-produced proto bytes back into a pprocutils.HTTPResponse.
func TestHTTPResponse_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		resp pprocutils.HTTPResponse
	}{
		{
			name: "full response with multi-value headers",
			resp: pprocutils.HTTPResponse{
				StatusCode: 200,
				Headers: map[string][]string{
					"Content-Type": {"application/json"},
					"Set-Cookie":   {"a=1", "b=2"},
					"X-Request-Id": {"req-123"},
				},
				Body: []byte(`{"result":"ok"}`),
			},
		},
		{
			name: "error status, no headers, no body",
			resp: pprocutils.HTTPResponse{
				StatusCode: 403,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)

			protoResp := toproto.HTTPResponse(tt.resp)

			wire, err := proto.Marshal(protoResp)
			is.NoErr(err)

			var decoded procutilsv1.HTTPResponse
			err = proto.Unmarshal(wire, &decoded)
			is.NoErr(err)

			got := fromproto.HTTPResponse(&decoded)
			is.Equal(got, tt.resp)
		})
	}
}
