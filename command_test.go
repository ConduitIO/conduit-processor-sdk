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
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/goccy/go-json"
	"github.com/matryer/is"
)

func TestCommand_Marshal_Unmarshal(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name  string
		input Command
	}{
		{
			name:  "specify",
			input: &SpecifyCmd{},
		},
		{
			name: "configure",
			input: &ConfigureCmd{
				ConfigMap: map[string]string{
					"url": "https://conduit.io",
				},
			},
		},
		{
			name:  "open",
			input: &OpenCmd{},
		},
		{
			name:  "process with no records",
			input: &ProcessCmd{},
		},
		{
			name: "process with records",
			input: &ProcessCmd{
				Records: []opencdc.Record{
					{
						Position:  opencdc.Position("test-post"),
						Operation: opencdc.OperationCreate,
						Metadata: opencdc.Metadata{
							"meta1": "v1",
						},
						Key: opencdc.RawData("test-key"),
						Payload: opencdc.Change{
							After: opencdc.StructuredData{
								"key1": "value1",
							},
						},
					},
				},
			},
		},
		{
			name:  "teardown",
			input: &TeardownCmd{},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			bytes, err := MarshalCommand(tt.input)
			is.NoErr(err)

			cmd, err := UnmarshalCommand(bytes)
			is.NoErr(err)
			is.Equal(tt.input, cmd)
		})
	}
}

func TestCommand_UnmarshalUnknownName(t *testing.T) {
	is := is.New(t)

	cmdBytes, err := json.Marshal(OpenCmd{})
	is.NoErr(err)

	bytes, err := json.Marshal(cmdWrapper{
		Name:    "foobar",
		Command: cmdBytes,
	})
	is.NoErr(err)

	cmd, err := UnmarshalCommand(bytes)
	is.True(err != nil)
	is.Equal("unknown command name: foobar", err.Error())
	is.True(cmd == nil)
}

func TestProcessCmd_Unmarshal(t *testing.T) {
	is := is.New(t)

	input := &ProcessCmd{
		Records: []opencdc.Record{
			{
				Position:  opencdc.Position("test-post"),
				Operation: opencdc.OperationCreate,
				Metadata: opencdc.Metadata{
					"meta1": "v1",
				},
				Key: opencdc.RawData("test-key"),
				Payload: opencdc.Change{
					After: opencdc.StructuredData{
						"key1": "value1",
					},
				},
			},
		},
	}
	bytes, err := MarshalCommand(input)
	is.NoErr(err)

	command, err := UnmarshalCommand(bytes)
	is.New(t).NoErr(err)
	is.Equal(input, command)
}
