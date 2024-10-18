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
	"sync"

	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"google.golang.org/protobuf/proto"
)

const defaultBufferSize = 1024 // 1kB

var bufferPool = sync.Pool{
	New: func() any {
		return make([]byte, defaultBufferSize)
	},
}

func NextCommand(cmdReq *processorv1.CommandRequest) error {
	buffer := bufferPool.Get().([]byte)
	defer func() {
		bufferPool.Put(buffer)
	}()

	buffer, cmdSize, err := hostCall(_commandRequest, buffer[:cap(buffer)])
	if err != nil {
		return err
	}
	// parse the command
	if err := proto.Unmarshal(buffer[:cmdSize], cmdReq); err != nil {
		return fmt.Errorf("failed unmarshalling %v bytes into proto type: %w", cmdSize, err)
	}
	return nil
}

func Reply(resp *processorv1.CommandResponse) error {
	buffer := bufferPool.Get().([]byte)
	defer func() {
		bufferPool.Put(buffer)
	}()

	buffer, err := proto.MarshalOptions{}.MarshalAppend(buffer[:0], resp)
	if err != nil {
		return fmt.Errorf("failed marshalling proto type into bytes: %w", err)
	}
	buffer, _, err = hostCall(_commandResponse, buffer)
	return err
}
