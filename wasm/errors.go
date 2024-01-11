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
	"math"
)

const (
	// ErrorCodeStart is the smallest error code which the host (i.e. Conduit) can send.
	// The imported function _nextCommand returns an uint32 value
	// that is either the number of bytes actually written or an error code.
	// Because of that, we're reserving a range of error codes.
	ErrorCodeStart = ErrorCodeNoMoreCommands

	ErrorCodeInsufficientSize = math.MaxUint32 - iota
	ErrorCodeUnknownCommandRequest
	ErrorCodeUnknownCommandResponse
	ErrorCodeMemoryOutOfRange
	ErrorCodeNoMoreCommands
)

var (
	ErrInsufficientSize       = NewError(ErrorCodeInsufficientSize, "allocated memory size is insufficient")
	ErrUnknownCommandRequest  = NewError(ErrorCodeUnknownCommandRequest, "unknown command request")
	ErrUnknownCommandResponse = NewError(ErrorCodeUnknownCommandResponse, "unknown command response")
	ErrMemoryOutOfRange       = NewError(ErrorCodeMemoryOutOfRange, "memory out of range")
	ErrNoMoreCommands         = NewError(ErrorCodeNoMoreCommands, "no more commands")
)

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
	case ErrorCodeInsufficientSize:
		return ErrInsufficientSize
	case ErrorCodeUnknownCommandRequest:
		return ErrUnknownCommandRequest
	case ErrorCodeUnknownCommandResponse:
		return ErrUnknownCommandResponse
	case ErrorCodeMemoryOutOfRange:
		return ErrMemoryOutOfRange
	case ErrorCodeNoMoreCommands:
		return ErrNoMoreCommands
	default:
		return NewError(code, "unknown error code")
	}
}
