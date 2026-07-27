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

package fromproto

import (
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	procutilsv1 "github.com/conduitio/conduit-processor-sdk/proto/procutils/v1"
)

func httpHeaders(in []*procutilsv1.HTTPHeader) map[string][]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]string, len(in))
	for _, h := range in {
		if h == nil {
			continue
		}
		// Preserve multi-value semantics; a repeated key merges its values.
		out[h.Key] = append(out[h.Key], h.Values...)
	}
	return out
}

func HTTPRequest(req *procutilsv1.HTTPRequest) pprocutils.HTTPRequest {
	return pprocutils.HTTPRequest{
		Method:        req.Method,
		URL:           req.Url,
		Headers:       httpHeaders(req.Headers),
		Body:          req.Body,
		AuthSecretRef: req.AuthSecretRef,
	}
}

func HTTPResponse(resp *procutilsv1.HTTPResponse) pprocutils.HTTPResponse {
	return pprocutils.HTTPResponse{
		StatusCode: int(resp.StatusCode),
		Headers:    httpHeaders(resp.Headers),
		Body:       resp.Body,
	}
}
