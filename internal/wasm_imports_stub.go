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

// The functions in this file are stubs of the functions defined
// in wasm_imports.go.
// They exist to make it possible to test, lint
// or generally run the code in a non-WASM environment.

//go:build !wasm

package internal

func _nextCommand(_, _ uint32) uint32 {
	panic("stub")
}

func _reply(_, _ uint32) {
	panic("stub")
}
