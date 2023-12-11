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

package internal

import (
	"fmt"
	"unsafe"
)

var allocs = make(map[uintptr][]byte)

func allocate(size uint32) (uint32, func()) {
	fmt.Printf("allocating %v bytes", size)

	return Write(make([]byte, size))
}

func free(ptr unsafe.Pointer) {
	if ptr == nil {
		return
	}

	if _, ok := allocs[uintptr(ptr)]; ok {
		delete(allocs, uintptr(ptr))
	} else {
		panic("free: invalid pointer")
	}
}

func ptrToByteArray(ptr uint32, size uint32) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), size)
}

func Write(bytes []byte) (uint32, func()) {
	fmt.Printf("writing %v bytes to memory\n", len(bytes))
	ptr := unsafe.Pointer(&bytes[0])
	allocs[uintptr(ptr)] = bytes

	return uint32(uintptr(ptr)), func() {
		free(ptr)
	}
}
