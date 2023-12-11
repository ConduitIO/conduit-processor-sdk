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

	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/goccy/go-json"
)

// Imports `nextCommand` from the host, which retrieves
// the next command for a processor.
//
// The arguments are a pointer to the address where
// the command should be written and the size of
// the allocated memory.
//
// The return value should be the number of bytes written,
// or an error code.
//
//go:wasmimport env nextCommand
func _nextCommand(ptr, size uint32) uint32

// Imports `reply` from the host, which informs
// the host about the reply for the previous command.
//
// The arguments are a pointer to the address where
// the reply will be written and the size of
// the allocated memory.
//
// The return values is an error code.
//
//go:wasmimport env reply
func _reply(ptr, size uint32) uint32

func NextCommand() (sdk.Command, error) {
	size := uint32(1024)
	ptr, cleanup := allocate(size)
	defer cleanup()

	fmt.Println("getting next command")
	bytesWritten := _nextCommand(ptr, size)
	if bytesWritten < 5 { // error codes
		fmt.Printf("got error code: %v\n", bytesWritten)
		return sdk.Command{}, fmt.Errorf("failed getting next command from host, error code: %v", bytesWritten)
	}

	var cmd sdk.Command
	err := json.Unmarshal(ptrToByteArray(ptr, size), &cmd)
	if err != nil {
		return sdk.Command{}, fmt.Errorf("failed unmarshalling")
	}

	return cmd, nil
}

func Reply(ptr uint32, size int) {
	fmt.Println("calling reply")
	_reply(ptr, uint32(size))
}
