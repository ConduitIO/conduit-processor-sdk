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
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"google.golang.org/protobuf/proto"
)

const defaultCommandSize = 1024 // 1kB

var buffer = make([]byte, defaultCommandSize)

func NextCommand(cmdReq *processorv1.CommandRequest) error {
	commandRequestCaller := HostCaller{Func: _commandRequest}
	buf, cmdSize, err := commandRequestCaller.Call(buffer, cap(buffer))
	if err != nil {
		return err
	}
	// parse the command
	if err := proto.Unmarshal(buf[:cmdSize], cmdReq); err != nil {
		return fmt.Errorf("failed unmarshalling %v bytes into proto type: %w", cmdSize, err)
	}
	return nil
}

func Reply(resp *processorv1.CommandResponse) error {
	var err error
	buffer, err = proto.MarshalOptions{}.MarshalAppend(buffer[:0], resp)
	if err != nil {
		return fmt.Errorf("failed marshalling proto type into bytes: %w", err)
	}
	commandResponseCaller := HostCaller{Func: _commandResponse}
	_, _, err = commandResponseCaller.Call(buffer, len(buffer))
	return err
}
