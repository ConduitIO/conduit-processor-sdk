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

// Package global provides the functionality for Conduit to set up utilities
// for processors. DO NOT use this package directly.
package global

import (
	"os"

	"github.com/conduitio/conduit-processor-sdk/conduit"
	"github.com/rs/zerolog"
)

var (
	// Logger is the logger for the processor. DO NOT use this logger directly,
	// instead use the Logger() function in the root of the processor SDK.
	Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).
		With().
		Timestamp().
		Logger()

	// TODO by default set to an in-memory schema service
	SchemaService conduit.SchemaService
)
