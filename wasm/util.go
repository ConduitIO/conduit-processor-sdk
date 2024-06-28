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

//go:build wasm

package wasm

import (
	"context"
	"fmt"
	"os"
	"unsafe"

	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

type Util struct {
	logger zerolog.Logger
}

func NewUtil(logLevel string) Util {
	return Util{
		logger: initLogger(logLevel),
	}
}

func initLogger(logLevel string) zerolog.Logger {
	logger := zerolog.New(os.Stdout)

	level, err := zerolog.ParseLevel(logLevel)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to parse log level, falling back to debug")
		// fallback to debug level
		level = zerolog.DebugLevel
	}
	logger = logger.Level(level)
	return logger
}

func (u Util) Logger(ctx context.Context) *zerolog.Logger {
	l := u.logger.With().Ctx(ctx).Logger()
	return &l
}

func CreateSchema(req *processorv1.CreateSchemaRequest) (*processorv1.CreateSchemaResponse, error) {
	buf, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshalling request: %w", err)
	}
	// 2 tries, 1st try is with the current buffer size, if that's not enough,
	// then resize the buffer and try again
	for i := 0; i < 2; i++ {
		// request Conduit to write the command to the given buffer
		ptr := unsafe.Pointer(&buf[0])
		cmdSize := _createSchema(ptr, uint32(cap(buf)))
		switch {
		case cmdSize >= ErrorCodeStart: // error codes
			return nil, NewErrorFromCode(cmdSize)
		case cmdSize > uint32(cap(buf)) || i == 0: // not enough memory
			oldSize := uint32(len(buf))
			buf = append(buf, make([]byte, cmdSize-oldSize)...)
			continue // try again
		}
		var resp processorv1.CreateSchemaResponse
		err = proto.Unmarshal(buf[:cmdSize], &resp)
		if err != nil {
			return nil, fmt.Errorf("failed unmarshalling %v bytes into proto type: %w", cmdSize, err)
		}
		return &resp, nil
	}
	panic("if this is reached, then the buffer was not resized correctly and we are in an infinite loop")
}

func GetSchema(req *processorv1.GetSchemaRequest) (*processorv1.GetSchemaResponse, error) {
	buf, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshalling request: %w", err)
	}
	// 2 tries, 1st try is with the current buffer size, if that's not enough,
	// then resize the buffer and try again
	for i := 0; i < 2; i++ {
		// request Conduit to write the command to the given buffer
		ptr := unsafe.Pointer(&buf[0])
		cmdSize := _getSchema(ptr, uint32(cap(buf)))
		switch {
		case cmdSize >= ErrorCodeStart: // error codes
			return nil, NewErrorFromCode(cmdSize)
		case cmdSize > uint32(cap(buf)) || i == 0: // not enough memory
			oldSize := uint32(len(buf))
			buf = append(buf, make([]byte, cmdSize-oldSize)...)
			continue // try again
		}
		var resp processorv1.GetSchemaResponse
		err = proto.Unmarshal(buf[:cmdSize], &resp)
		if err != nil {
			return nil, fmt.Errorf("failed unmarshalling %v bytes into proto type: %w", cmdSize, err)
		}
		return &resp, nil
	}
	panic("if this is reached, then the buffer was not resized correctly and we are in an infinite loop")
}
