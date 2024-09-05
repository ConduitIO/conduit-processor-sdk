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
	"unsafe"

	"github.com/conduitio/conduit-processor-sdk/pprocutils"
)

// HostFunc is the function type for the imported functions from the host.
//
// The arguments are:
// (1) a pointer to the address where the command should be written
// (2) the size of allocated memory.
//
// The return value indicates the size of the allocated response in bytes. If the
// response is larger than the allocated memory, the caller should reallocate the
// memory and call the function again.
type HostFunc func(ptr unsafe.Pointer, size uint32) uint32

// Call calls the function from the host 2 times max, is the buffer size is not
// enough the first time its called, it will be resized the second call.
// returns the buffer, command size, and error.
func hostCall(fn HostFunc, buf []byte) ([]byte, uint32, error) {
	// 2 tries, 1st try is with the current buffer size, if that's not enough,
	// then resize the buffer and try again
	for i := 0; i < 2; i++ {
		// request the host to write the response to the given buffer address
		ptr := unsafe.Pointer(&buf[0])
		cmdSize := fn(ptr, uint32(len(buf))) //nolint:gosec // no risk of overflow
		switch {
		case cmdSize >= pprocutils.ErrorCodeStart:
			// error codes
			return nil, cmdSize, pprocutils.NewErrorFromCode(cmdSize)
		case cmdSize > uint32(len(buf)) && i == 0: //nolint:gosec // no risk of overflow
			// not enough memory, resize the buffer and try again
			oldSize := uint32(len(buf)) //nolint:gosec // no risk of overflow
			buf = append(buf, make([]byte, cmdSize-oldSize)...)
			continue // try again
		}
		return buf, cmdSize, nil
	}
	panic("if this is reached, then the buffer was not resized correctly and we are in an infinite loop")
}
