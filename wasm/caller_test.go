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
)

func TestHostCall(t *testing.T) {
	tests := []struct {
		name            string
		hostFunc        HostFunc
		buf             []byte
		expectedBuf     []byte
		expectedCmdSize uint32
		expectedError   error
	}{
		{
			name: "buffer large enough on first call",
			hostFunc: func(ptr unsafe.Pointer, size uint32) uint32 {
				buf := unsafe.Slice((*byte)(ptr), size)
				copy(buf, "command")
				return uint32(len("command"))
			},
			buf:             make([]byte, 10),
			expectedBuf:     []byte("command\x00\x00\x00"),
			expectedCmdSize: 7,
			expectedError:   nil,
		},
		{
			name: "buffer not large enough on first call, resized on second",
			hostFunc: func(ptr unsafe.Pointer, size uint32) uint32 {
				if size < 7 {
					return 7
				}
				buf := unsafe.Slice((*byte)(ptr), size)
				copy(buf, "command")
				return uint32(len("command"))
			},
			buf:             make([]byte, 3),
			expectedBuf:     []byte("command"),
			expectedCmdSize: 7,
			expectedError:   nil,
		},
		{
			name: "host returns error",
			hostFunc: func(_ unsafe.Pointer, _ uint32) uint32 {
				return pprocutils.ErrorCodeInternal
			},
			buf:             make([]byte, 10),
			expectedBuf:     nil,
			expectedCmdSize: pprocutils.ErrorCodeInternal,
			expectedError:   pprocutils.NewErrorFromCode(pprocutils.ErrorCodeInternal),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			buf, cmdSize, err := hostCall(tt.hostFunc, tt.buf)
			is.Equal(tt.expectedError, err)
			is.Equal(tt.expectedBuf, buf)
			is.Equal(tt.expectedCmdSize, cmdSize)
		})
	}
}
