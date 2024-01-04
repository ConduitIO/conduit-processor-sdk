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

package proto

import (
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/matryer/is"
)

func TestCommand_Marshal_Unmarshal(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name  string
		input sdk.Command
	}{
		{
			name:  "specify",
			input: &sdk.SpecifyCmd{},
		},
		{
			name: "configure",
			input: &sdk.ConfigureCmd{
				ConfigMap: map[string]string{
					"url": "https://conduit.io",
				},
			},
		},
		{
			name:  "open",
			input: &sdk.OpenCmd{},
		},
		{
			name:  "process with no records",
			input: &sdk.ProcessCmd{},
		},
		{
			name: "process with records",
			input: &sdk.ProcessCmd{
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
			input: &sdk.TeardownCmd{},
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

func TestCommandResponse_Marshal_Unmarshal(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name  string
		input sdk.CommandResponse
	}{
		{
			name: "specify",
			input: &sdk.SpecifyResponse{
				Specification: sdk.Specification{
					Name:        "test-connector",
					Summary:     "A test connector",
					Description: "A test connector",
					Version:     "v2.3.4",
					Author:      "Conduit",
					Parameters: map[string]sdk.Parameter{
						"required-param": {
							Default:     "",
							Type:        sdk.ParameterTypeString,
							Description: "Service URL",
							Validations: []sdk.Validation{
								{
									Type: sdk.ValidationTypeRequired,
								},
							},
						},
						"duration-param": {
							Default:     "1m",
							Type:        sdk.ParameterTypeDuration,
							Description: "Timeout",
							Validations: nil,
						},
						"inclusion-param": {
							Default:     "snapshot",
							Type:        sdk.ParameterTypeDuration,
							Description: "Timeout",
							Validations: []sdk.Validation{
								{
									Type:  sdk.ValidationTypeInclusion,
									Value: "snapshot,cdc",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "specify with error",
			input: &sdk.SpecifyResponse{
				Err: errors.New("specify error"),
			},
		},
		{
			name:  "configure",
			input: &sdk.ConfigureResponse{},
		},
		{
			name: "configure with error",
			input: &sdk.ConfigureResponse{
				Err: errors.New("configure error"),
			},
		},
		{
			name: "open",
			input: &sdk.OpenResponse{
				Err: errors.New("open error"),
			},
		},
		{
			name:  "process response with no records",
			input: &sdk.ProcessResponse{},
		},
		{
			name: "process with records",
			input: &sdk.ProcessResponse{
				Records: []sdk.ProcessedRecord{
					sdk.SingleRecord{
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
			input: &sdk.TeardownResponse{},
		},
		{
			name: "teardown with error",
			input: &sdk.TeardownResponse{
				Err: errors.New("teardown error"),
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			bytes, err := MarshalCommandResponse(tt.input)
			is.NoErr(err)

			got, err := UnmarshalCommandResponse(bytes)
			is.NoErr(err)
			is.Equal(tt.input, got)
		})
	}
}
