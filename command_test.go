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
	"fmt"
	"github.com/goccy/go-json"
	"testing"

	"github.com/matryer/is"
)

func TestCommand_Marshal_Unmarshal(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name    string
		input   Command
		wantErr error
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
			name:  "process",
			input: &ProcessCmd{},
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

func TestCommand_Unmarshal(t *testing.T) {
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

func TestName(t *testing.T) {
	bytes := []byte(" {\"name\":\"specify\",\"command\":\"e30=\"}")
	command, err := UnmarshalCommand(bytes)
	is.New(t).NoErr(err)
	fmt.Println(command)
}
