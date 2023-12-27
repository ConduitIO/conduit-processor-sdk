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
	"context"

	"github.com/conduitio/conduit-commons/opencdc"
)

type Command interface {
	Execute(context.Context, Processor) CommandResponse
}

type CommandResponse interface {
	Error() error
	isCommandResponse()
}

type SpecifyCmd struct{}

func (c *SpecifyCmd) Execute(_ context.Context, plugin Processor) CommandResponse {
	spec, err := plugin.Specification()
	return &SpecifyResponse{Specification: spec, Err: err}
}

type SpecifyResponse struct {
	Specification Specification
	Err           error
}

func (r *SpecifyResponse) Error() error {
	return r.Err
}

func (r *SpecifyResponse) isCommandResponse() {}

type ConfigureCmd struct {
	ConfigMap map[string]string
}

func (c *ConfigureCmd) Execute(ctx context.Context, p Processor) CommandResponse {
	return &ConfigureResponse{
		Err: p.Configure(ctx, c.ConfigMap),
	}
}

type ConfigureResponse struct {
	Err error
}

func (r *ConfigureResponse) Error() error {
	return r.Err
}

func (r *ConfigureResponse) isCommandResponse() {}

type OpenCmd struct{}

func (c *OpenCmd) Execute(ctx context.Context, p Processor) CommandResponse {
	return &OpenResponse{Err: p.Open(ctx)}
}

type OpenResponse struct {
	Err error
}

func (r *OpenResponse) Error() error {
	return r.Err
}

func (r *OpenResponse) isCommandResponse() {}

type ProcessCmd struct {
	Records []opencdc.Record
}

func (c *ProcessCmd) Execute(ctx context.Context, plugin Processor) CommandResponse {
	return &ProcessResponse{Records: plugin.Process(ctx, c.Records)}
}

type ProcessResponse struct {
	Records []ProcessedRecord
}

func (r *ProcessResponse) Error() error {
	return nil
}

func (r *ProcessResponse) isCommandResponse() {}

type TeardownCmd struct{}

func (c *TeardownCmd) Execute(ctx context.Context, p Processor) CommandResponse {
	return &TeardownResponse{
		Err: p.Teardown(ctx),
	}
}

type TeardownResponse struct {
	Err error
}

func (r *TeardownResponse) Error() error {
	return r.Err
}

func (r *TeardownResponse) isCommandResponse() {}
