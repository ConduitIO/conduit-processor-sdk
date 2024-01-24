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

// Package conduit provides the functionality for Conduit to set up and run
// built-in processors. DO NOT use this package directly.
package conduit

import (
	"context"

	"github.com/conduitio/conduit-processor-sdk/internal"
)

// ContextWithUtil allows Conduit to set the Util interface for built-in
// processors. DO NOT use this function in your processor.
func ContextWithUtil(ctx context.Context, util internal.Util) context.Context {
	return internal.ContextWithUtil(ctx, util)
}
