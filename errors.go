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

package sdk

import (
	"errors"
	"math"
)

var (
	// ErrorCodeStart is the smallest error code which the host (i.e. Conduit) can send.
	// The imported function _nextCommand returns an uint32 value
	// that is either the number of bytes actually written or an error code.
	// Because of that, we're reserving a range of error codes.
	ErrorCodeStart = ErrMemoryOutOfRange

	ErrCodeInsufficientSize     = math.MaxUint32 - uint32(1)
	ErrCodeFailedGettingCommand = math.MaxUint32 - uint32(2)
	ErrMemoryOutOfRange         = math.MaxUint32 - uint32(3)
)

var ErrNextCommand = errors.New("failed getting next command")
