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

//go:build wasm

package sdk

import (
	"context"
	"errors"
	"fmt"
	"os"

	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"github.com/conduitio/conduit-processor-sdk/wasm"
)

// Run is the entry-point for a standalone processor. It handles all communication
// with Conduit. It will block forever, or until an error occurs. If an error
// occurs, it will be printed to stderr and the process will exit with a non-zero
// exit code. Otherwise, it will exit with a zero exit code.
//
// A processor plugin needs to call this function in its main function.
func Run(p Processor) {
	ctx := context.Background()
	var cmd processorv1.CommandRequest

	for {
		cmd.Reset()
		err := wasm.NextCommand(&cmd)
		if err != nil {
			if errors.Is(err, wasm.ErrNoMoreCommands) {
				os.Exit(0)
			}
			_, _ = fmt.Fprintf(os.Stderr, "failed retrieving next command: %v", err)
			os.Exit(1)
		}

		resp := executeCommand(ctx, p, &cmd)
		err = wasm.Reply(resp)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed writing reply: %v\n", err)
			os.Exit(1)
		}
	}
}

func executeCommand(ctx context.Context, p Processor, cmdReq *processorv1.CommandRequest) *processorv1.CommandResponse {
	var resp *processorv1.CommandResponse
	var err error

	switch req := cmdReq.GetRequest().(type) {
	case *processorv1.CommandRequest_Specify:
		resp, err = executeCommandSpecify(ctx, p, req.Specify)
	case *processorv1.CommandRequest_Configure:
		resp, err = executeCommandConfigure(ctx, p, req.Configure)
	case *processorv1.CommandRequest_Open:
		resp, err = executeCommandOpen(ctx, p, req.Open)
	case *processorv1.CommandRequest_Process:
		resp, err = executeCommandProcess(ctx, p, req.Process)
	case *processorv1.CommandRequest_Teardown:
		resp, err = executeCommandTeardown(ctx, p, req.Teardown)
	default:
		err = wasm.ErrUnknownCommandRequest
	}

	if err != nil {
		var wasmErr *wasm.Error
		var code uint32
		if errors.As(err, &wasmErr) {
			code = wasmErr.ErrCode
		}
		resp = &processorv1.CommandResponse{
			Response: &processorv1.CommandResponse_Error{
				Error: &processorv1.ErrorResponse{
					Code:    code,
					Message: err.Error(),
				},
			},
		}
	}

	return resp
}

func executeCommandSpecify(_ context.Context, p Processor, _ *processorv1.Specify_Request) (*processorv1.CommandResponse, error) {
	var spec Specification
	spec, err := p.Specification()
	if err != nil {
		return nil, err
	}
	resp := &processorv1.CommandResponse_Specify{
		Specify: &processorv1.Specify_Response{
			Name:        spec.Name,
			Summary:     spec.Summary,
			Description: spec.Description,
			Version:     spec.Version,
			Author:      spec.Author,
			Parameters:  specificationParametersToProto(spec.Parameters),
		},
	}
	return &processorv1.CommandResponse{Response: resp}, nil
}

func specificationParametersToProto(in map[string]Parameter) map[string]*processorv1.Specify_Parameter {
	out := make(map[string]*processorv1.Specify_Parameter, len(in))
	for name, param := range in {
		out[name] = &processorv1.Specify_Parameter{
			Default:     param.Default,
			Description: param.Description,
			Type:        processorv1.Specify_Parameter_Type(param.Type),
			Validations: specificationValidationsToProto(param.Validations),
		}
	}
	return out
}

func specificationValidationsToProto(in []Validation) []*processorv1.Specify_Parameter_Validation {
	if in == nil {
		return nil
	}
	out := make([]*processorv1.Specify_Parameter_Validation, len(in))
	for i, v := range in {
		out[i] = &processorv1.Specify_Parameter_Validation{
			Type:  processorv1.Specify_Parameter_Validation_Type(v.Type),
			Value: v.Value,
		}
	}
	return out
}

func executeCommandConfigure(ctx context.Context, p Processor, req *processorv1.Configure_Request) (*processorv1.CommandResponse, error) {
	panic("unimplemented")
}

func executeCommandOpen(ctx context.Context, p Processor, req *processorv1.Open_Request) (*processorv1.CommandResponse, error) {
	panic("unimplemented")
}

func executeCommandProcess(ctx context.Context, p Processor, req *processorv1.Process_Request) (*processorv1.CommandResponse, error) {
	panic("unimplemented")
}

func executeCommandTeardown(ctx context.Context, p Processor, req *processorv1.Teardown_Request) (*processorv1.CommandResponse, error) {
	panic("unimplemented")
}
