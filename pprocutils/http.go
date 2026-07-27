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

//go:generate mockgen -typed -destination=mock/http.go -package=mock -mock_names=HTTPService=HTTPService . HTTPService

package pprocutils

import "context"

// HTTPRequest is a single-shot, buffered outbound HTTP request a standalone
// (WASM) processor asks the host to perform on its behalf. The guest never gets
// a socket: the host validates this request against a per-processor egress
// policy (allowlist + resolved-IP gate) and performs the I/O with full net/http.
type HTTPRequest struct {
	Method  string
	URL     string
	Headers map[string][]string
	Body    []byte
	// AuthSecretRef names a secret the HOST resolves and injects as the
	// Authorization header, after all validation, immediately before dispatch.
	// The guest never supplies the credential value, and a guest-supplied
	// Authorization header is rejected — the key never enters guest memory.
	AuthSecretRef string
}

// HTTPResponse is the fully-buffered, host-size-capped response returned to the
// guest. There is no streaming contract: the whole (capped) body is
// materialized host-side before it crosses back to the guest.
type HTTPResponse struct {
	StatusCode int
	Headers    map[string][]string
	Body       []byte
}

// HTTPService is the host-side seam for the egress capability, mirroring
// SchemaService. It is implemented host-side, bound to a single processor
// instance's resolved egress policy; the implementation enforces the security
// boundary (allowlist, DNS-rebinding resolved-IP gate, no-proxy transport,
// redirect suppression, per-call timeout, response-size cap, host-injected
// credentials). DO NOT use this package directly.
type HTTPService interface {
	Do(ctx context.Context, req HTTPRequest) (HTTPResponse, error)
}
