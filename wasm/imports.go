// Copyright © 2023 Meroxa, Inc.
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

import "unsafe"

// Imports `command_request` from the host, which retrieves
// the next command for a processor.
//
// The arguments are:
// (1) a pointer to the address where the command should be written
// (2) the size of allocated memory.
//
// The return value indicates the size of the allocated request in bytes. If the
// command is larger than the allocated memory, the caller should reallocate the
// memory and call `command_request` again.
//
//go:wasmimport conduit command_request
func _commandRequest(ptr unsafe.Pointer, size uint32) uint32

// Imports `command_response` from the host, which informs
// the host about the response for the previous command.
//
// The arguments are:
// (1) a pointer to the address where the reply was written
// (2) the size of allocated memory.
//
//go:wasmimport conduit command_response
func _commandResponse(ptr unsafe.Pointer, size uint32) uint32

// Imports `create_schema` from the host, which creates a schema
//
// The arguments are:
// (1) a pointer to the address where the response should be written
// (2) the size of allocated memory.
//
// The return value indicates the size of the allocated request in bytes. If the
// command is larger than the allocated memory, the caller should reallocate the
// memory and call `create_schema` again.
//
//go:wasmimport conduit create_schema
func _createSchema(ptr unsafe.Pointer, size uint32) uint32

// Imports `get_schema` from the host, which gets a schema
//
// The arguments are:
// (1) a pointer to the address where the response should be written
// (2) the size of allocated memory.
//
// The return value indicates the size of the allocated request in bytes. If the
// command is larger than the allocated memory, the caller should reallocate the
// memory and call `get_schema` again.
//
//go:wasmimport conduit get_schema
func _getSchema(ptr unsafe.Pointer, size uint32) uint32
