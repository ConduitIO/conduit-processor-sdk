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
	"os"

	"github.com/rs/zerolog"
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
