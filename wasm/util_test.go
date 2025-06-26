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
	"fmt"
	"testing"
	"unsafe"

	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
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

func TestHandleImportedCall(t *testing.T) {
	is := is.New(t)
	responseMsg := &anypb.Any{Value: []byte("response")}
	responseBytes, err := proto.Marshal(responseMsg)
	is.NoErr(err)
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
				oldBuffer := importCallBuffer
				importCallBuffer = tt.buffer
				defer func() {
					importCallBuffer = oldBuffer // Restore the original buffer after the test
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

func TestHandleExportedCall(t *testing.T) {
	is := is.New(t)

	// Prepare a request and response
	reqMsg := &anypb.Any{Value: []byte("request")}
	respMsg := &anypb.Any{Value: []byte("response")}
	reqBytes, err := proto.Marshal(reqMsg)
	is.NoErr(err)

	// Handler that returns a valid response
	handler := func(r *anypb.Any) (*anypb.Any, error) {
		is.Equal(r.Value, reqMsg.Value)
		return respMsg, nil
	}

	// Handler that returns an error
	errorHandler := func(*anypb.Any) (*anypb.Any, error) {
		return nil, fmt.Errorf("handler error")
	}

	t.Run("success", func(t *testing.T) {
		is := is.New(t)
		// Prepare buffer for request
		buf := newBuffer(len(reqBytes))
		copy(*buf, reqBytes)

		// Call handleExportedCall
		result := handleExportedCall(buf.Pointer(), uint32(len(*buf)), handler, &anypb.Any{})
		// Unpack pointer and size
		respPtr := uint32(result >> 32)
		respSize := uint32(result)
		if respPtr == 0 || respSize == 0 {
			t.Fatalf("expected non-zero pointer and size")
		}
		// Read response from exportCallBuffer
		got := &anypb.Any{}
		err := proto.Unmarshal((*exportCallBuffer)[:respSize], got)
		is.NoErr(err)
		is.Equal(got.Value, respMsg.Value)
	})

	t.Run("handler error", func(t *testing.T) {
		is := is.New(t)
		buf := newBuffer(len(reqBytes))
		copy(*buf, reqBytes)

		result := handleExportedCall(buf.Pointer(), uint32(len(*buf)), errorHandler, &anypb.Any{})
		respSize := uint32(result)
		got := &processorv1.Error{}
		err := proto.Unmarshal((*exportCallBuffer)[:respSize], got)
		is.NoErr(err)
		is.Equal(int(got.Code), pprocutils.ErrorCodeInternal)
		is.True(len(got.Message) > 0)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		is := is.New(t)
		// Pass invalid proto bytes
		buf := newBuffer(len("not a proto"))
		copy(*buf, "not a proto")

		result := handleExportedCall(buf.Pointer(), uint32(len(*buf)), handler, &anypb.Any{})
		respSize := uint32(result)
		got := &processorv1.Error{}
		err := proto.Unmarshal((*exportCallBuffer)[:respSize], got)
		is.NoErr(err)
		is.Equal(int(got.Code), pprocutils.ErrorCodeInternal)
		is.True(len(got.Message) > 0)
	})
}
