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

// Package sdk is the Go SDK for building Conduit processors. A processor
// receives records flowing through a pipeline, transforms them, and returns the
// result. Authors implement the [Processor] interface (or adapt a function with
// [NewProcessorFunc]) and the SDK handles the rest: configuration parsing,
// schema encode/decode middleware, and the plumbing that connects the processor
// to the Conduit engine.
//
// # Standalone vs. built-in processors
//
// The same [Processor] implementation can run in two ways:
//
//   - Standalone: the processor is compiled to a WebAssembly module
//     (GOOS=wasip1 GOARCH=wasm) and executed by Conduit in a wazero runtime.
//     The module's main function calls [Run], which becomes the entry point.
//     This is the default and recommended mode — it isolates the processor from
//     the engine and lets it be distributed as a single portable binary.
//   - Built-in: the processor is compiled natively into a custom Conduit
//     build. This avoids the WebAssembly boundary (and its per-call
//     serialization cost) at the price of coupling the processor to the engine
//     binary. Built-in processors do not call [Run]; the engine invokes the
//     [Processor] methods directly.
//
// Author code is identical in both modes. Only the entry point and build
// constraints differ, so write to the [Processor] contract and let the build
// target decide how the processor is hosted.
//
// # Implementing a Processor
//
// Embed [UnimplementedProcessor] in your type. It provides no-op implementations
// of the optional methods and satisfies the unexported marker method that keeps
// the interface closed, so adding a method to [Processor] in a later release is
// not a breaking change for existing processors:
//
//	type myProcessor struct {
//		sdk.UnimplementedProcessor
//		cfg myConfig
//	}
//
//	func (p *myProcessor) Specification() (sdk.Specification, error) { ... }
//	func (p *myProcessor) Configure(ctx context.Context, cfg config.Config) error { ... }
//	func (p *myProcessor) Process(ctx context.Context, recs []opencdc.Record) []sdk.ProcessedRecord { ... }
//
// # Lifecycle
//
// The runtime calls a processor's methods in a fixed order, and (for a single
// processor instance) never concurrently — the standalone command loop in [Run]
// processes one command at a time. A processor therefore does not need to guard
// its own fields against concurrent access by the SDK, but it must not assume
// any parallelism either.
//
//  1. Specification — called to discover the processor's name, version, and
//     configuration parameters. Must be side-effect free; it may be called
//     before Configure and without any configuration.
//  2. Configure — called once with the user's configuration. Validate and store
//     it here. Do not open connections or start background work; that is Open's
//     job. See [ParseConfig] for turning the raw config map into a typed struct.
//  3. Open — called once after Configure. Acquire resources and start any
//     background work here.
//  4. Process — called repeatedly, once per incoming batch, until shutdown. See
//     the record-handling contract below.
//  5. Teardown — called once when the pipeline is shutting down. No other method
//     is called after Teardown returns; the processor is then discarded. Release
//     everything Open acquired.
//
// Process may be called more than once with the same records (for example after
// a restart when records were not flushed downstream), so processing must be
// idempotent.
//
// # Record handling and error propagation
//
// Process receives a batch of [opencdc.Record] values and returns a
// [ProcessedRecord] for each. The returned slice is positional: the result at
// index i is the outcome of the input record at index i. Each input record may
// carry raw or structured data in its key and payload; a processor that reads
// structured fields should enable the schema-decode middleware (see below) or
// handle both shapes.
//
// The concrete [ProcessedRecord] type an author returns decides how the record
// continues through the pipeline:
//
//   - [SingleRecord] — the transformed record continues downstream. This is the
//     common case.
//   - [MultiRecord] — the record is split into zero or more records. Returning
//     an empty MultiRecord is equivalent to [FilterRecord]; returning one record
//     is equivalent to [SingleRecord].
//   - [FilterRecord] — the record is acknowledged and dropped from the pipeline.
//     Use this to intentionally discard records; it is not an error.
//   - [ErrorRecord] — processing failed. The record is nacked and handled
//     according to the pipeline's error policy (for example routed to a dead-
//     letter queue or halting the pipeline). Returning an ErrorRecord is the
//     only way to signal a per-record failure — a processor must not drop a
//     record it could not process, or at-least-once delivery is violated.
//
// Because filtering, splitting, and failing are all expressed through the
// return value rather than through the process's exit or a returned error,
// Process itself does not return an error: a batch always produces a result for
// every record it accounts for.
//
// # Middleware
//
// [DefaultProcessorMiddleware] wraps every processor run through [Run] with
// schema decode and encode middleware. Decode middleware fetches the schema
// referenced in a record's metadata and turns raw key/payload bytes into
// [opencdc.StructuredData] before Process sees them; encode middleware reverses
// that afterwards, so a processor can operate on structured data without dealing
// with schema resolution. A processor tunes this via [Processor.MiddlewareOptions]
// (see [ProcessorWithSchemaDecodeConfig] and [ProcessorWithSchemaEncodeConfig]).
//
// # Neighboring packages
//
//   - github.com/conduitio/conduit-commons/opencdc defines the Record type that
//     flows through Process.
//   - github.com/conduitio/conduit-commons/config defines the configuration and
//     parameter types used in Specification and Configure.
//   - The schema subpackage is the author-facing API for creating and fetching
//     schemas from within a processor.
//   - The wasm and pprocutils subpackages are engine plumbing and are not meant
//     to be imported by processor authors.
package sdk
