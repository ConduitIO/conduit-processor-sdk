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

package wasm

import (
	"fmt"
	"unsafe"

	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"google.golang.org/protobuf/proto"
)

const defaultCommandSize = 1024 // 1kB

var (
	buffer = make([]byte, defaultCommandSize)
)

// NextCommand retrieves the next command from Conduit.
func NextCommand(cmdReq *processorv1.CommandRequest) error {
	// 2 tries, 1st try is with the current buffer size, if that's not enough,
	// then resize the buffer and try again
	for i := 0; i < 2; i++ {
		// request Conduit to write the command to the given buffer
		ptr := unsafe.Pointer(&buffer[0])
		cmdSize := _commandRequest(ptr, uint32(cap(buffer)))

		switch {
		case cmdSize >= ErrorCodeStart: // error codes
			return NewErrorFromCode(cmdSize)
		case cmdSize > uint32(cap(buffer)): // not enough memory
			buffer = make([]byte, cmdSize) // resize buffer
			continue                       // try again
		}

		// parse the command
		if err := proto.Unmarshal(buffer[:cmdSize], cmdReq); err != nil {
			return fmt.Errorf("failed unmarshalling %v bytes into proto type: %w", cmdSize, err)
		}
		return nil
	}
	panic("if this is reached, then the buffer was not resized correctly and we are in an infinite loop")
}

func Reply(resp *processorv1.CommandResponse) error {
	var err error
	buffer, err = proto.MarshalOptions{}.MarshalAppend(buffer[:0], resp)
	if err != nil {
		return fmt.Errorf("failed marshalling proto type into bytes: %w", err)
	}

	ptr := unsafe.Pointer(&buffer[0])
	errCode := _commandResponse(ptr, uint32(len(buffer)))
	if errCode != 0 {
		return NewErrorFromCode(errCode)
	}

	return nil
}
