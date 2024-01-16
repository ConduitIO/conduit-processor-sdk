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

package internal

import (
	"context"

	"github.com/rs/zerolog"
)

type Util interface {
	Logger(ctx context.Context) *zerolog.Logger
}

// utilCtxKey is used as the key when saving the Util in a context.
type utilCtxKey struct{}

// ContextWithUtil wraps ctx and returns a context that contains Util.
func ContextWithUtil(ctx context.Context, util Util) context.Context {
	return context.WithValue(ctx, utilCtxKey{}, util)
}

// UtilFromContext fetches the record Util from the context. If the
// context does not contain a Util it returns nil.
func UtilFromContext(ctx context.Context) Util {
	util := ctx.Value(utilCtxKey{})
	if util != nil {
		return util.(Util)
	}
	return nil
}
