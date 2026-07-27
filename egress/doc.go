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

// Package egress is the processor-facing API for the WASM host-mediated
// network-egress capability. A standalone (WebAssembly) processor has no
// socket API of its own — Conduit performs the outbound HTTP call on the
// processor's behalf, through a security boundary the processor cannot see
// or influence. See docs/design-documents/20260726-wasm-host-egress-capability.md
// in ConduitIO/conduit for the full design.
//
// [Do] is the only entry point, backed by the package-level [HTTPService]:
//
//   - Standalone (WebAssembly): the engine replaces [HTTPService] at startup
//     with a client that forwards calls through the host's http_request
//     capability.
//   - Built-in processors / tests: the default [HTTPService] always returns
//     [ErrEgressDisabled]. A built-in processor already runs with a real
//     socket and should call net/http directly instead of this package;
//     replace [HTTPService] with a stub or mock in tests.
//
// # Egress is deny-by-default and operator-gated
//
// A processor gets zero egress unless its operator explicitly opts it in
// with a destination allowlist, optionally further clamped by an
// engine-level ceiling. A processor cannot widen its own policy: a [Do] call
// outside the resolved allowlist fails with [ErrForbidden], and a call from a
// processor that was never opted in fails with [ErrEgressDisabled] — neither
// is ever a silent pass-through.
//
// # Credentials are host-injected, never guest-supplied
//
// [Request.AuthSecretRef] names a secret; Conduit resolves it and sets the
// Authorization header itself, immediately before dispatch. There is no
// guest-supplied-credential path — a [Request] with an explicit Authorization
// header is rejected with [ErrInvalidRequest] — so a credential can never be
// read out of a processor's own memory or leaked by a memory-disclosure bug.
//
// # Redirects are not followed
//
// A 3xx response is returned to the caller as an ordinary [Response], never
// followed automatically. A redirect to a non-allowlisted or private
// Location therefore surfaces as a normal response, not a policy bypass.
//
// # Responses are buffered and size-capped
//
// The call is single-shot request/response, not streaming: [Do] blocks until
// the full response is available. A response larger than the host-enforced
// size cap is not delivered — [Do] returns [ErrResponseTooLarge] instead,
// with no partial body.
package egress
