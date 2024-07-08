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
	"errors"
	"math"
)

const (
	// ErrorCodeStart is the smallest error code which the host (i.e. Conduit) can send.
	// The imported function _nextCommand returns an uint32 value
	// that is either the number of bytes actually written or an error code.
	// Because of that, we're reserving a range of error codes.
	ErrorCodeStart = math.MaxUint32 - 100

	ErrorCodeNoMoreCommands = math.MaxUint32 - iota
	ErrorCodeUnknownCommandRequest
	ErrorCodeUnknownCommandResponse
	ErrorCodeMemoryOutOfRange

	ErrorCodeSchemaUnmarshal
	ErrorCodeSchemaMarshal
	ErrorCodeSchemaNotFound
	ErrorCodeInvalidSchemaSubject
	ErrorCodeInvalidSchemaType
	ErrorCodeInvalidSchemaBytes
)

var (
	ErrNoMoreCommands         = NewError(ErrorCodeNoMoreCommands, "no more commands")
	ErrUnknownCommandRequest  = NewError(ErrorCodeUnknownCommandRequest, "unknown command request")
	ErrUnknownCommandResponse = NewError(ErrorCodeUnknownCommandResponse, "unknown command response")
	ErrMemoryOutOfRange       = NewError(ErrorCodeMemoryOutOfRange, "memory out of range")

	ErrSchemaUnmarshal = NewError(ErrorCodeSchemaUnmarshal, "failed to unmarshal the proto request")
	ErrSchemaMarshal   = NewError(ErrorCodeSchemaMarshal, "failed to marshal the proto response into a schema")

	ErrSchemaNotFound       = NewError(ErrorCodeSchemaNotFound, "schema not found")
	ErrInvalidSchemaSubject = NewError(ErrorCodeInvalidSchemaSubject, "invalid schema subject")
	ErrInvalidSchemaType    = NewError(ErrorCodeInvalidSchemaType, "invalid schema type")
	ErrInvalidSchemaBytes   = NewError(ErrorCodeInvalidSchemaBytes, "invalid schema bytes")
)

// Error is an error sent to or received from the host (i.e. Conduit).
type Error struct {
	ErrCode uint32
	Message string
}

func (e *Error) Error() string {
	return e.Message
}

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.ErrCode == t.ErrCode
}

func NewError(code uint32, message string) *Error {
	return &Error{
		ErrCode: code,
		Message: message,
	}
}

func NewErrorFromCode(code uint32) *Error {
	switch code {
	case ErrorCodeNoMoreCommands:
		return ErrNoMoreCommands
	case ErrorCodeUnknownCommandRequest:
		return ErrUnknownCommandRequest
	case ErrorCodeUnknownCommandResponse:
		return ErrUnknownCommandResponse
	case ErrorCodeMemoryOutOfRange:
		return ErrMemoryOutOfRange
	case ErrorCodeSchemaUnmarshal:
		return ErrSchemaUnmarshal
	case ErrorCodeSchemaMarshal:
		return ErrSchemaMarshal
	case ErrorCodeSchemaNotFound:
		return ErrSchemaNotFound
	case ErrorCodeInvalidSchemaSubject:
		return ErrInvalidSchemaSubject
	case ErrorCodeInvalidSchemaType:
		return ErrInvalidSchemaType
	case ErrorCodeInvalidSchemaBytes:
		return ErrInvalidSchemaBytes
	default:
		return NewError(code, "unknown error code")
	}
}

func CodeFromError(err error) uint32 {
	var wasmErr *Error
	if errors.As(err, &wasmErr) {
		return wasmErr.ErrCode
	}
	return 0
}
