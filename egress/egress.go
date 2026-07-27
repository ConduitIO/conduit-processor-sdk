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

package egress

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-processor-sdk/pprocutils"
)

// HTTPService is the service backing [Do]. In a standalone (WebAssembly)
// processor the engine overwrites this at startup with a client that
// forwards calls through the host's http_request capability. In any other
// hosting mode (built-in processors, tests) it defaults to a stub that
// always returns [ErrEgressDisabled]: a built-in processor already has a
// real socket and should call net/http directly instead of this package.
// Replace it in tests to stub egress behavior.
var HTTPService pprocutils.HTTPService = disabledHTTPService{}

// Sentinel errors mirror the ABI's numeric error-code band
// (pprocutils.ErrorCodeHTTP*, see pprocutils/errors.go) so a caller can
// classify a [Do] failure with [errors.Is] without importing pprocutils
// directly.
var (
	// ErrEgressDisabled is returned when the processor was not opted into
	// egress by its operator. Egress is deny-all by default: a pipeline
	// author must explicitly allowlist a destination before a processor can
	// reach it at all.
	ErrEgressDisabled = pprocutils.ErrHTTPEgressDisabled
	// ErrForbidden is returned when the destination host, scheme, or
	// resolved IP is not permitted by the host-enforced allowlist. This
	// also covers the DNS-rebinding defense: a hostname that passed the
	// allowlist can still be refused here if it resolves to a
	// private/reserved address at dial time.
	ErrForbidden = pprocutils.ErrHTTPForbidden
	// ErrInvalidRequest is returned for a malformed request: an unparsable
	// URL, a disallowed scheme, header injection (CRLF), or a header the
	// host reserves for itself (Host, Authorization, Accept-Encoding).
	ErrInvalidRequest = pprocutils.ErrHTTPInvalidRequest
	// ErrDNS is returned when host-side name resolution fails. Unlike
	// [ErrForbidden], this is a transient condition a processor may choose
	// to retry.
	ErrDNS = pprocutils.ErrHTTPDNS
	// ErrTimeout is returned when the host-enforced per-call deadline is
	// exceeded. The processor cannot extend this deadline.
	ErrTimeout = pprocutils.ErrHTTPTimeout
	// ErrResponseTooLarge is returned when the response body — after
	// decompression, if any — exceeds the host-enforced size cap. No
	// partial body is returned.
	ErrResponseTooLarge = pprocutils.ErrHTTPResponseTooLarge
	// ErrTransport is returned for a connection-level failure: reset, TLS
	// handshake failure, and similar.
	ErrTransport = pprocutils.ErrHTTPTransport
)

// Request is a single outbound HTTP call a standalone processor asks Conduit
// to perform on its behalf. The guest never gets a socket: Conduit validates
// the request against the processor's host-configured egress policy
// (allowlist, resolved-IP dial-time gate) and performs the I/O itself with a
// hardened net/http client. The call is buffered, not streaming: [Do] blocks
// until the full (capped) response is available.
type Request struct {
	// Method is the HTTP method, e.g. "GET" or "POST".
	Method string
	// URL is the target URL, including scheme. Only https is permitted
	// unless the exact (host, port) pair is explicitly allowlisted for http
	// (the local-Ollama case).
	URL string
	// Headers are sent as-is, except for a small host-reserved set the
	// processor cannot set: Host, Authorization, and Accept-Encoding.
	// Setting any of them returns [ErrInvalidRequest].
	Headers map[string][]string
	// Body is the request body, if any.
	Body []byte
	// AuthSecretRef names a secret Conduit resolves and injects as the
	// Authorization header, immediately before the request is dispatched.
	// The credential value never enters the processor's memory — there is
	// no guest-supplied-credential path, by design. Leave empty for an
	// unauthenticated request.
	AuthSecretRef string
}

// Response is the fully-buffered, host-size-capped response to a [Request].
type Response struct {
	StatusCode int
	Headers    map[string][]string
	// Body is the full response body, up to the host-enforced size cap. A
	// response that exceeds the cap is never delivered: [Do] returns
	// [ErrResponseTooLarge] instead, with no partial body.
	Body []byte
}

// Do performs a host-mediated outbound HTTP call. It is the processor-facing
// entry point for the WASM host-egress capability; [HTTPService] does the
// actual work, so a processor should call Do rather than [HTTPService]
// directly.
//
// Do returns a wrapped error for every host-side rejection or failure; test
// against the sentinel errors in this package with [errors.Is] to classify
// the outcome — for example, to distinguish a policy rejection
// ([ErrForbidden], [ErrEgressDisabled]) from a transient one ([ErrTimeout],
// [ErrDNS], [ErrTransport]) for a retry/DLQ decision.
func Do(ctx context.Context, req Request) (Response, error) {
	resp, err := HTTPService.Do(ctx, pprocutils.HTTPRequest{
		Method:        req.Method,
		URL:           req.URL,
		Headers:       req.Headers,
		Body:          req.Body,
		AuthSecretRef: req.AuthSecretRef,
	})
	if err != nil {
		return Response{}, fmt.Errorf("error performing http egress request: %w", err)
	}

	return Response{
		StatusCode: resp.StatusCode,
		Headers:    resp.Headers,
		Body:       resp.Body,
	}, nil
}

// disabledHTTPService is the default, non-WASM [HTTPService]. Outside a
// standalone processor there is no host boundary to broker the call
// through, so every request is refused with [ErrEgressDisabled] rather than
// silently falling back to an unpolicied net/http call — the security
// boundary this capability exists to enforce has no meaning without the host
// mediating it.
type disabledHTTPService struct{}

func (disabledHTTPService) Do(context.Context, pprocutils.HTTPRequest) (pprocutils.HTTPResponse, error) {
	return pprocutils.HTTPResponse{}, ErrEgressDisabled
}
