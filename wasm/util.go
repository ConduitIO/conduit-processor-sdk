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

package wasm

import (
	"fmt"
	"unsafe"

	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"google.golang.org/protobuf/proto"
)

// -- buffer -------------------------------------------------------------------

// buffer is a utility struct that holds a byte slice and an unsafe pointer to
// the start of the slice's data. It is used to efficiently pass data between
// the host and the WebAssembly module without unnecessary copying.
type buffer []byte

func newBuffer(size int) *buffer {
	b := make(buffer, size)
	return &b
}

func (b *buffer) Grow(size int) {
	if cap(*b) < size {
		// This append logic preserves existing data when growing.
		// We append to the end of the slice after expanding it to the full capacity.
		*b = append((*b)[:cap(*b)], make([]byte, size-cap(*b))...)
	}
	// Reslice to the new size if we had enough capacity.
	// This does not shrink the buffer if size < len(*b).
	if len(*b) < size {
		*b = (*b)[:size]
	}
}

// Pointer returns a pointer to the buffer's data.
// Includes a safety check for zero-length slices to prevent panics.
func (b *buffer) Pointer() unsafe.Pointer {
	if len(*b) == 0 {
		return nil
	}
	return unsafe.Pointer(&(*b)[0])
}

// PointerAndSize returns the pointer and size in a single uint64.
// The higher 32 bits are the pointer, and the lower 32 bits are the size.
func (b *buffer) PointerAndSize() uint64 {
	return (uint64(uintptr(b.Pointer())) << 32) | uint64(len(*b))
}

// -- Imported Function Utilities ----------------------------------------------

var importCallBuffer = newBuffer(1024) // 1kB buffer for requests and responses

// hostFunc is the function type for the imported functions from the host.
//
// The arguments are:
// (1) a pointer to the address where the command should be written
// (2) the size of allocated memory.
//
// The return value indicates the size of the allocated response in bytes. If the
// response is larger than the allocated memory, the caller should reallocate the
// memory and call the function again.
type hostFunc func(ptr unsafe.Pointer, size uint32) uint32

// handleImportedCall calls the function from the host 2 times max, is the buffer
// size is not enough the first time its called, it will be resized the second call.
// Returns the buffer, command size, and error.
func handleImportedCall[REQ, RESP proto.Message](fn hostFunc, req REQ, resp RESP) error {
	reqData, err := proto.MarshalOptions{}.MarshalAppend((*importCallBuffer)[:0], req)
	if err != nil {
		return fmt.Errorf("error marshalling proto type %T: %w", req, err)
	}
	*importCallBuffer = reqData

	// 2 tries, 1st try is with the current buffer size, if that's not enough,
	// then resize the buffer and try again
	for i := 0; i < 2; i++ {
		// request the host to write the response to the given buffer address
		cmdSize := fn(importCallBuffer.Pointer(), uint32(len(*importCallBuffer))) //nolint:gosec // no risk of overflow
		switch {
		case cmdSize >= pprocutils.ErrorCodeStart:
			// error codes
			return pprocutils.NewErrorFromCode(cmdSize)
		case cmdSize > uint32(len(*importCallBuffer)): //nolint:gosec // no risk of overflow
			// not enough memory, resize the buffer and try again
			importCallBuffer.Grow(int(cmdSize))
			continue // try again
		}

		// we have a valid response, unmarshal it
		err = proto.Unmarshal((*importCallBuffer)[:cmdSize], resp)
		if err != nil {
			return fmt.Errorf("failed unmarshalling %v bytes into proto type %T: %w", cmdSize, resp, err)
		}

		return nil
	}
	panic("if this is reached, then the buffer was not resized correctly and we are in an infinite loop")
}

// -- Exported Function Utilities ----------------------------------------------

var exportCallBuffer = newBuffer(1024) // 1kB buffer for export responses

// handleExportedCall handles the exported call from the host.
func handleExportedCall[REQ, RESP proto.Message](
	ptr unsafe.Pointer, size uint32,
	handleFn func(REQ) (RESP, error),
	cmdReq REQ,
) uint64 {
	in := unsafe.Slice((*byte)(ptr), size)

	err := proto.Unmarshal(in[:size], cmdReq)
	if err != nil {
		return handleExportedCallError(fmt.Errorf("failed to unmarshal %v bytes into proto type %T: %w", size, cmdReq, err))
	}

	resp, err := handleFn(cmdReq)
	if err != nil {
		return handleExportedCallError(fmt.Errorf("failed to handle command: %w", err))
	}

	outData, err := proto.MarshalOptions{}.MarshalAppend((*exportCallBuffer)[:0], resp)
	if err != nil {
		return handleExportedCallError(fmt.Errorf("failed marshalling proto type %T into bytes: %w", resp, err))
	}
	*exportCallBuffer = outData
	return exportCallBuffer.PointerAndSize()
}

func handleExportedCallError(err error) uint64 {
	protoErr := &processorv1.Error{
		Code:    pprocutils.ErrorCodeInternal,
		Message: fmt.Sprintf("error handling command: %v", err),
	}
	outData, err := proto.MarshalOptions{}.MarshalAppend((*exportCallBuffer)[:0], protoErr)
	if err != nil {
		// If we fail to marshal the error, we panic because we can't return an error to the host.
		panic(fmt.Sprintf("failed to marshal error response: %v", err))
	}
	*exportCallBuffer = outData
	return exportCallBuffer.PointerAndSize()
}
