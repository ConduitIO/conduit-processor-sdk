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

// Package schema is the processor-facing API for creating and fetching schemas.
// Processors use it to register the schema of records they emit and to look up
// the schema of records they receive, keeping schema identity consistent across
// a pipeline.
//
// The package exposes two entry points, [Get] and [Create], both backed by the
// package-level [SchemaService]. The service a processor talks to depends on how
// it is hosted:
//
//   - Standalone (WebAssembly): the engine replaces [SchemaService] at startup
//     with an implementation that forwards calls to Conduit's schema registry
//     over the host boundary, so schemas are shared with the rest of the
//     pipeline.
//   - Built-in / tests: the default [SchemaService] is an in-process,
//     [NewInMemoryService]-backed store wrapped in a cache. It has no
//     persistence and is not shared with a real registry — useful for unit
//     tests, not for cross-processor schema sharing.
//
// Get and Create results are cached, so repeated lookups of the same schema do
// not cross the host boundary again.
package schema
