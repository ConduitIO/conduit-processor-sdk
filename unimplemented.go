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

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
)

// UnimplementedProcessor should be embedded to have forward compatible implementations.
type UnimplementedProcessor struct{}

// Specification needs to be overridden in the actual implementation.
func (UnimplementedProcessor) Specification() (Specification, error) {
	return Specification{}, fmt.Errorf("action \"Specification\": %w", ErrUnimplemented)
}

// Configure is optional and can be overridden in the actual implementation.
func (UnimplementedProcessor) Configure(context.Context, config.Config) error {
	return nil
}

// Open is optional and can be overridden in the actual implementation.
func (UnimplementedProcessor) Open(context.Context) error {
	return nil
}

// Process needs to be overridden in the actual implementation.
func (UnimplementedProcessor) Process(context.Context, []opencdc.Record) []ProcessedRecord {
	return []ProcessedRecord{ErrorRecord{Error: ErrUnimplemented}}
}

// Teardown is optional and can be overridden in the actual implementation.
func (UnimplementedProcessor) Teardown(context.Context) error {
	return nil
}

// MiddlewareOptions is optional and can be overridden in the actual implementation.
func (UnimplementedProcessor) MiddlewareOptions() []ProcessorMiddlewareOption {
	return nil
}

func (UnimplementedProcessor) mustEmbedUnimplementedProcessor() {}
