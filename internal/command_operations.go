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
	"math"

	sdk "github.com/conduitio/conduit-processor-sdk"
)

var (
	defaultCommandSize = uint32(1024)

	// ErrorCodeStart is the smallest error code
	// which the host (i.e. Conduit) can send.
	// The imported function _nextCommand return an uint32 value
	// that is either the number of bytes actually written or an error code.
	// Because of that, we're reserving a range of error code.
	ErrorCodeStart = math.MaxUint32 - uint32(100)
)

// NextCommand retrieves the next command from Conduit
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
	if resp > ErrorCodeStart { // error codes
		// todo if more memory is needed, allocate it
		return nil, fmt.Errorf("failed getting next command from host, error code: %v", resp)
	}

	// parse the command
	cmd, err := sdk.UnmarshalCommand(ptrToByteArray(ptr, resp))
	if err != nil {
		return nil, fmt.Errorf("failed unmarshalling command: %w", err)
	}

	return cmd, nil
}

func Reply(resp sdk.CommandResponse) error {
	bytes, err := resp.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed writing reply: %w", err)
	}

	ptr, cleanup := Write(bytes)
	defer cleanup()
	_reply(ptr, uint32(len(bytes)))

	return nil
}
