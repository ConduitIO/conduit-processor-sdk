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

package internal

// Imports `nextCommand` from the host, which retrieves
// the next command for a processor.
//
// The arguments are:
// (1) a pointer to the address where the command should be written
// (2) the size of allocated memory.
//
// The return value can be 0 (for a successful reply) or an error code.
//
//go:wasmimport env nextCommand
func _nextCommand(ptr, size uint32) uint32

// Imports `reply` from the host, which informs
// the host about the reply for the previous command.
//
// The arguments are:
// (1) a pointer to the address where the reply should be written
// (2) the size of allocated memory.
//
// The return value can be 0 (for a successful reply) or an error code.
//
//go:wasmimport env reply
func _reply(ptr, size uint32)
