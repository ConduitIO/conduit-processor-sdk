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

package toproto

import (
	"sort"

	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	procutilsv1 "github.com/conduitio/conduit-processor-sdk/proto/procutils/v1"
)

func httpHeaders(in map[string][]string) []*procutilsv1.HTTPHeader {
	if len(in) == 0 {
		return nil
	}
	// Deterministic key order so the wire encoding is stable (round-trip tests,
	// reproducible marshalling).
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]*procutilsv1.HTTPHeader, 0, len(keys))
	for _, k := range keys {
		out = append(out, &procutilsv1.HTTPHeader{Key: k, Values: in[k]})
	}
	return out
}

func HTTPRequest(in pprocutils.HTTPRequest) *procutilsv1.HTTPRequest {
	return &procutilsv1.HTTPRequest{
		Method:        in.Method,
		Url:           in.URL,
		Headers:       httpHeaders(in.Headers),
		Body:          in.Body,
		AuthSecretRef: in.AuthSecretRef,
	}
}

func HTTPResponse(in pprocutils.HTTPResponse) *procutilsv1.HTTPResponse {
	return &procutilsv1.HTTPResponse{
		StatusCode: int32(in.StatusCode), //nolint:gosec // HTTP status codes are small
		Headers:    httpHeaders(in.Headers),
		Body:       in.Body,
	}
}
