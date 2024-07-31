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

package pprocutils

import (
	"math"
)

const (
	// ErrorCodeStart is the smallest error code which the wasm package can send.
	ErrorCodeStart = math.MaxUint32 - 100

	ErrorCodeNoMoreCommands = math.MaxUint32 - iota
	ErrorCodeUnknownCommandRequest
	ErrorCodeUnknownCommandResponse
	ErrorCodeMemoryOutOfRange
	ErrorCodeInternal

	ErrorCodeSubjectNotFound
	ErrorCodeVersionNotFound
	ErrorCodeInvalidSchema
)

var (
	ErrNoMoreCommands         = NewError(ErrorCodeNoMoreCommands, "no more commands")
	ErrUnknownCommandRequest  = NewError(ErrorCodeUnknownCommandRequest, "unknown command request")
	ErrUnknownCommandResponse = NewError(ErrorCodeUnknownCommandResponse, "unknown command response")
	ErrMemoryOutOfRange       = NewError(ErrorCodeMemoryOutOfRange, "memory out of range")

	ErrSubjectNotFound = NewError(ErrorCodeSubjectNotFound, "schema subject not found")
	ErrVersionNotFound = NewError(ErrorCodeVersionNotFound, "schema version not found")
	ErrInvalidSchema   = NewError(ErrorCodeInvalidSchema, "invalid schema")

	ErrInternal = NewError(ErrorCodeInternal, "internal error")
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
	case ErrorCodeNoMoreCommands:
		return ErrNoMoreCommands
	case ErrorCodeUnknownCommandRequest:
		return ErrUnknownCommandRequest
	case ErrorCodeUnknownCommandResponse:
		return ErrUnknownCommandResponse
	case ErrorCodeMemoryOutOfRange:
		return ErrMemoryOutOfRange
	case ErrorCodeSubjectNotFound:
		return ErrSubjectNotFound
	case ErrorCodeVersionNotFound:
		return ErrVersionNotFound
	case ErrorCodeInvalidSchema:
		return ErrInvalidSchema
	case ErrorCodeInternal:
		return ErrInternal
	default:
		return NewError(code, "unknown error code")
	}
}
