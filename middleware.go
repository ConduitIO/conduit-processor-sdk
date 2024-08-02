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

package sdk

// ProcessorMiddlewareOption is a function that can be used to configure a
// ProcessorMiddleware.
type ProcessorMiddlewareOption interface {
	Apply(ProcessorMiddleware)
}

// ProcessorMiddleware wraps a Processor and adds functionality to it.
type ProcessorMiddleware interface {
	Wrap(Processor) Processor
}

// DefaultProcessorMiddleware returns a slice of middleware that is added to all
// processors by default.
func DefaultProcessorMiddleware(opts ...ProcessorMiddlewareOption) []ProcessorMiddleware {
	middleware := []ProcessorMiddleware{
		// TODO add default middleware
	}

	// apply options to all middleware
	for _, m := range middleware {
		for _, opt := range opts {
			opt.Apply(m)
		}
	}
	return middleware
}

// ProcessorWithMiddleware wraps the processor into the supplied middleware.
func ProcessorWithMiddleware(p Processor, middleware ...ProcessorMiddleware) Processor {
	// apply middleware in reverse order to preserve the order as specified
	for i := len(middleware) - 1; i >= 0; i-- {
		p = middleware[i].Wrap(p)
	}
	return p
}
