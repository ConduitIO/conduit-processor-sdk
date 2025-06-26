// Copyright © 2025 Meroxa, Inc.
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

import (
	"unsafe"

	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
)

//go:wasmexport conduit.processor.v1.malloc
func malloc(size uint32) unsafe.Pointer {
	// Allocate a buffer of the specified size.
	exportReqBuffer.Grow(int(size))
	return exportReqBuffer.Pointer()
}

//go:wasmexport conduit.processor.v1.specification
func specification(ptr unsafe.Pointer, size uint32) uint64 {
	return handleExportedCall(ptr, size, Handler.Specification, &processorv1.Specify_Request{})
}

//go:wasmexport conduit.processor.v1.configure
func configure(ptr unsafe.Pointer, size uint32) uint64 {
	return handleExportedCall(ptr, size, Handler.Configure, &processorv1.Configure_Request{})
}

//go:wasmexport conduit.processor.v1.open
func open(ptr unsafe.Pointer, size uint32) uint64 {
	return handleExportedCall(ptr, size, Handler.Open, &processorv1.Open_Request{})
}

//go:wasmexport conduit.processor.v1.process
func process(ptr unsafe.Pointer, size uint32) uint64 {
	// TODO reuse the same request object and reset it instead of creating a new one each time
	return handleExportedCall(ptr, size, Handler.Process, &processorv1.Process_Request{})
}

//go:wasmexport conduit.processor.v1.teardown
func teardown(ptr unsafe.Pointer, size uint32) uint64 {
	return handleExportedCall(ptr, size, Handler.Teardown, &processorv1.Teardown_Request{})
}

// Handler is the bridge between the WebAssembly exports and the processor SDK.
var Handler interface {
	Specification(*processorv1.Specify_Request) (*processorv1.Specify_Response, error)
	Configure(*processorv1.Configure_Request) (*processorv1.Configure_Response, error)
	Open(*processorv1.Open_Request) (*processorv1.Open_Response, error)
	Process(*processorv1.Process_Request) (*processorv1.Process_Response, error)
	Teardown(*processorv1.Teardown_Request) (*processorv1.Teardown_Response, error)
}
