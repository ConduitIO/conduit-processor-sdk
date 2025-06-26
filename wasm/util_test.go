// Copyright © 2024 Meroxa, Inc.
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
	"testing"
	"unsafe"

	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	"github.com/matryer/is"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestBuffer_NewBuffer(t *testing.T) {
	t.Run("should create a buffer with a specific size", func(t *testing.T) {
		is := is.New(t)
		size := 128
		b := newBuffer(size)

		is.Equal(len(*b), size)
		is.True(cap(*b) >= size)
	})

	t.Run("should create an empty buffer for size 0", func(t *testing.T) {
		is := is.New(t)
		b := newBuffer(0)
		is.True(b != nil)    // The buffer itself is not nil
		is.Equal(len(*b), 0) // The underlying slice has zero length
		is.Equal(cap(*b), 0) // and zero capacity
	})
}

func TestBuffer_Grow(t *testing.T) {
	t.Run("should grow by re-allocating and preserve data", func(t *testing.T) {
		is := is.New(t)
		b := newBuffer(10)
		// Put some data in the buffer
		copy(*b, "0123456789")

		originalCap := cap(*b)
		originalPtr := b.Pointer()

		newSize := originalCap + 10
		b.Grow(newSize)

		is.Equal(len(*b), newSize)
		is.True(cap(*b) >= newSize)
		is.True(b.Pointer() != originalPtr)       // Pointer should change after reallocation
		is.Equal(string((*b)[:10]), "0123456789") // Old data must be preserved
	})

	t.Run("should grow by re-slicing when capacity is sufficient", func(t *testing.T) {
		is := is.New(t)
		// Create a buffer with more capacity than length
		b := buffer(make([]byte, 5, 20))
		copy(b, "hello")

		originalCap := cap(b)
		originalPtr := b.Pointer()

		newSize := 15
		b.Grow(newSize)

		is.Equal(len(b), newSize)
		is.Equal(cap(b), originalCap)      // Capacity should not change
		is.Equal(b.Pointer(), originalPtr) // Pointer should not change
		is.Equal(string(b[:5]), "hello")   // Old data must be preserved
	})
}

func TestBuffer_PointerAndSize(t *testing.T) {
	t.Run("should correctly encode pointer and size", func(t *testing.T) {
		is := is.New(t)
		// Start with a non-zero buffer per assumptions
		b := newBuffer(256)

		packed := b.PointerAndSize()
		is.True(packed != 0)

		// Decode the values
		decodedPtrVal := uint32(packed >> 32)
		decodedSize := uint32(packed)

		// This test simulates a 32-bit wasm architecture. On a 64-bit test host,
		// we must compare the decoded 32-bit pointer value with the truncated
		// original 64-bit pointer value.
		is.Equal(decodedPtrVal, uint32(uintptr(b.Pointer())))
		is.Equal(int(decodedSize), len(*b))
		is.Equal(int(decodedSize), 256)
	})

	t.Run("should return 0 for an empty buffer", func(t *testing.T) {
		is := is.New(t)
		// Even though the assumption is non-zero, the methods should be robust.
		b := newBuffer(0)
		packed := b.PointerAndSize()
		is.Equal(packed, uint64(0)) // Pointer is nil (0) and size is 0
	})
}

func TestHostCall(t *testing.T) {
	responseMsg := &anypb.Any{Value: []byte("response")}
	responseBytes, err := proto.Marshal(responseMsg)
	if err != nil {
		panic(err)
	}
	responseLen := len(responseBytes)

	tests := []struct {
		name      string
		hostFunc  hostFunc
		buffer    *buffer
		req       *anypb.Any
		wantResp  *anypb.Any
		wantError error
	}{
		{
			name: "buffer large enough on first call",
			hostFunc: func(ptr unsafe.Pointer, size uint32) uint32 {
				buf := unsafe.Slice((*byte)(ptr), size)
				copy(buf, responseBytes)
				return uint32(responseLen)
			},
			buffer:    newBuffer(responseLen),
			req:       &anypb.Any{Value: []byte("request")},
			wantResp:  responseMsg,
			wantError: nil,
		},
		{
			name: "buffer not large enough on first call, resized on second",
			hostFunc: func(ptr unsafe.Pointer, size uint32) uint32 {
				if size < uint32(responseLen) {
					return uint32(responseLen)
				}
				buf := unsafe.Slice((*byte)(ptr), size)
				copy(buf, responseBytes)
				return uint32(responseLen)
			},
			buffer:    newBuffer(responseLen - 1), // initial buffer is too small
			req:       &anypb.Any{Value: []byte("request")},
			wantResp:  responseMsg,
			wantError: nil,
		},
		{
			name: "host returns error",
			hostFunc: func(_ unsafe.Pointer, _ uint32) uint32 {
				return pprocutils.ErrorCodeInternal
			},
			req:       &anypb.Any{Value: []byte("request")},
			wantResp:  nil,
			wantError: pprocutils.NewErrorFromCode(pprocutils.ErrorCodeInternal),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			if tt.buffer != nil {
				oldBuffer := importBuffer
				importBuffer = tt.buffer
				defer func() {
					importBuffer = oldBuffer // Restore the original buffer after the test
				}()
			}

			var resp anypb.Any
			err := handleImportedCall(tt.hostFunc, tt.req, &resp)
			is.Equal(tt.wantError, err)
			if err == nil {
				is.Equal(tt.wantResp.Value, resp.Value)
			}
		})
	}
}
