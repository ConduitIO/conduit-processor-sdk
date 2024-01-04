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

package wasm

import (
	"fmt"

	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit-processor-sdk/proto"
)

var defaultCommandSize = uint32(1024)

// NextCommand retrieves the next command from Conduit.
func NextCommand() (sdk.Command, error) {
	// allocate some memory for Conduit to write the command
	// we're allocating some memory in advance, so that
	// we don't need to introduce another call just to
	// get the amount of memory which is needed.
	ptr, cleanup := allocate(defaultCommandSize)
	defer cleanup()

	// request Conduit to write the command to the given allocation
	fmt.Println("getting next command")
	resp := _nextCommand(ptr, defaultCommandSize)
	if resp > sdk.ErrorCodeStart { // error codes
		// todo if more memory is needed, allocate it
		// https://github.com/ConduitIO/conduit-processor-sdk/issues/6
		fmt.Printf("got error code: %v\n", resp)

		if resp == sdk.ErrCodeNoMoreCommands {
			return nil, fmt.Errorf("error code %v: %w", resp, sdk.ErrNoMoreCommands)
		}
		return nil, fmt.Errorf("error code %v: %w", resp, sdk.ErrNextCommand)
	}

	// parse the command
	cmd, err := proto.UnmarshalCommand(ptrToByteArray(ptr, resp))
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling command: %w", err)
	}

	return cmd, nil
}

func Reply(resp sdk.CommandResponse) error {
	bytes, err := proto.MarshalCommandResponse(resp)
	if err != nil {
		return fmt.Errorf("failed marshalling CommandResponse to bytes: %w", err)
	}

	ptr, cleanup := Write(bytes)
	defer cleanup()
	_reply(ptr, uint32(len(bytes)))

	return nil
}
