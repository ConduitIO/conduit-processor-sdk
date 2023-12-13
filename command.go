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
	"encoding/base64"
	"errors"
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

func (c CommandResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(c)
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

type SpecifyCmd struct{}

func (c *SpecifyCmd) Name() string {
	return "specify"
}

func (c *SpecifyCmd) Execute(_ context.Context, plugin ProcessorPlugin) CommandResponse {
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

type OpenCmd struct{}

func (c *OpenCmd) Name() string {
	return "open"
}

func (c *OpenCmd) Execute(ctx context.Context, plugin ProcessorPlugin) CommandResponse {
	return NewCommandResponse(nil, plugin.Open(ctx))
}

type ProcessCmd struct {
	Records []opencdc.Record `json:"records"`
}

func (c *ProcessCmd) UnmarshalJSON(bytes []byte) error {
	m := make(map[string]interface{})
	err := json.Unmarshal(bytes, &m)
	if err != nil {
		return err
	}

	if r, ok := m["records"]; !ok || r == nil {
		// no records to parse
		return nil
	}

	var records []opencdc.Record
	recordsJSON, ok := m["records"].([]interface{})
	if !ok {
		return errors.New("records is not an array")
	}

	// manually parse the records, to make sure
	// that raw data fields get correctly deserialized
	// from Base64-encoded strings.
	for _, recJSON := range recordsJSON {
		rec, err := c.deserializeRecord(recJSON)
		if err != nil {
			return fmt.Errorf("failed deserializing record: %w", err)
		}
		records = append(records, rec)
	}

	c.Records = records
	return nil
}

// getData converts an interface{} into opencdc.Data.
func (c *ProcessCmd) getData(val interface{}) (opencdc.Data, error) {
	switch v := val.(type) {
	case nil:
		return nil, nil
	case opencdc.RawData:
		return v, nil
	case opencdc.StructuredData:
		return v, nil
	case string:
		str, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("failed parsing key: %w", err)
		}
		return opencdc.RawData(str), nil
	case map[string]interface{}:
		return opencdc.StructuredData(v), nil
	default:
		return nil, fmt.Errorf("unknown type: %T", v)
	}
}

func (c *ProcessCmd) Name() string {
	return "process"
}

func (c *ProcessCmd) Execute(ctx context.Context, plugin ProcessorPlugin) CommandResponse {
	return NewCommandResponse(
		json.Marshal(plugin.Process(ctx, c.Records)),
	)
}

func (c *ProcessCmd) deserializeRecord(recJSON interface{}) (opencdc.Record, error) {
	rm, ok := recJSON.(map[string]interface{})
	if !ok {
		return opencdc.Record{}, errors.New("record not a map")
	}

	key, err := c.getData(rm["key"])
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed deserializing key: %w", err)
	}
	delete(rm, "key")

	payloadMap, ok := rm["payload"].(map[string]interface{})
	if !ok {
		return opencdc.Record{}, fmt.Errorf("payload not a map")
	}

	before, err := c.getData(payloadMap["before"])
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("payload before: %w", err)
	}
	after, err := c.getData(payloadMap["after"])
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("payload before: %w", err)
	}
	delete(rm, "payload")

	bytes, err := json.Marshal(rm)
	if err != nil {
		return opencdc.Record{}, err
	}

	var rec opencdc.Record
	err = json.Unmarshal(bytes, &rec)
	if err != nil {
		return opencdc.Record{}, err
	}
	rec.Key = key
	rec.Payload = opencdc.Change{Before: before, After: after}

	return rec, nil
}

type TeardownCmd struct{}

func (c *TeardownCmd) Name() string {
	return "teardown"
}

func (c *TeardownCmd) Execute(ctx context.Context, plugin ProcessorPlugin) CommandResponse {
	return NewCommandResponse(nil, plugin.Teardown(ctx))
}
