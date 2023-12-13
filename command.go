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
	"fmt"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/goccy/go-json"
)

var cmdConstructors = map[string]func() Command{
	"specify": func() Command {
		return &SpecifyCmd{}
	},
	"configure": func() Command {
		return &ConfigureCmd{}
	},
	"open": func() Command {
		return &OpenCmd{}
	},
	"process": func() Command {
		return &ProcessCmd{}
	},
	"teardown": func() Command {
		return &TeardownCmd{}
	},
}

type cmdWrapper struct {
	Name    string `json:"name"`
	Command []byte `json:"command"`
}

type Command interface {
	Name() string
	Execute(context.Context, ProcessorPlugin) CommandResponse
}

type CommandResponse struct {
	Bytes []byte `json:"bytes"`
	Error string `json:"error"`
}

func NewCommandResponse(bytes []byte, err error) CommandResponse {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}

	return CommandResponse{Bytes: bytes, Error: errStr}
}

func UnmarshalCommand(bytes []byte) (Command, error) {
	var cmdw cmdWrapper
	err := json.Unmarshal(bytes, &cmdw)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshall command: %w", err)
	}

	if cmdw.Name == "" {
		return nil, fmt.Errorf("missing command name")
	}
	ctor, ok := cmdConstructors[cmdw.Name]
	if !ok {
		return nil, fmt.Errorf("unknown command name: %v", cmdw.Name)
	}

	cmd := ctor()
	err = json.Unmarshal(cmdw.Command, cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshall command: %w", err)
	}

	return cmd, nil
}

func MarshalCommand(command Command) ([]byte, error) {
	bytes, err := json.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("failed marshaling command: %w", err)
	}

	return json.Marshal(cmdWrapper{
		Name:    command.Name(),
		Command: bytes,
	})
}

type SpecifyCmd struct {
}

func (c *SpecifyCmd) Name() string {
	return "specify"
}

func (c *SpecifyCmd) Execute(ctx context.Context, plugin ProcessorPlugin) CommandResponse {
	return NewCommandResponse(json.Marshal(plugin.Specification()))
}

type ConfigureCmd struct {
	ConfigMap map[string]string `json:"configMap"`
}

func (c *ConfigureCmd) Name() string {
	return "configure"
}

func (c *ConfigureCmd) Execute(ctx context.Context, plugin ProcessorPlugin) CommandResponse {
	return NewCommandResponse(nil, plugin.Configure(ctx, c.ConfigMap))
}

type OpenCmd struct {
}

func (c *OpenCmd) Name() string {
	return "open"
}

func (c *OpenCmd) Execute(ctx context.Context, plugin ProcessorPlugin) CommandResponse {
	return NewCommandResponse(nil, plugin.Open(ctx))
}

type ProcessCmd struct {
	Records []opencdc.Record
}

func (c *ProcessCmd) Name() string {
	return "process"
}

func (c *ProcessCmd) Execute(ctx context.Context, plugin ProcessorPlugin) CommandResponse {
	return NewCommandResponse(json.Marshal(plugin.Process(ctx, c.Records)))
}

type TeardownCmd struct {
}

func (c *TeardownCmd) Name() string {
	return "teardown"
}

func (c *TeardownCmd) Execute(ctx context.Context, plugin ProcessorPlugin) CommandResponse {
	return NewCommandResponse(nil, plugin.Teardown(ctx))
}
