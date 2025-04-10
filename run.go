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

	"github.com/conduitio/conduit-commons/opencdc"
	configv1 "github.com/conduitio/conduit-commons/proto/config/v1"
	opencdcv1 "github.com/conduitio/conduit-commons/proto/opencdc/v1"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"github.com/conduitio/conduit-processor-sdk/wasm"
	"github.com/rs/zerolog"
)

// Run is the entry-point for a standalone processor. It handles all communication
// with Conduit. It will block forever, or until an error occurs. If an error
// occurs, it will be printed to stderr and the process will exit with a non-zero
// exit code. Otherwise, it will exit with a zero exit code.
//
// A processor plugin needs to call this function in its main function. The
// entrypoint file should look like this:
//
//	//go:build wasm
//
//	package main
//
//	import (
//		sdk "github.com/conduitio/conduit-processor-sdk"
//	)
//
//	func main() {
//		processor := NewMyProcessor()
//		sdk.Run(processor)
//	}
func Run(p Processor) {
	checkMagicCookie()

	var (
		env = struct {
			processorID string
			logLevel    string
		}{
			processorID: os.Getenv("CONDUIT_PROCESSOR_ID"),
			logLevel:    os.Getenv("CONDUIT_LOG_LEVEL"),
		}

		ctx = context.Background()
		cmd processorv1.CommandRequest
	)

	wasm.InitUtils(env.logLevel)
	logger := Logger(ctx)

	defer func() {
		if r := recover(); r != nil {
			logger.Error().Msgf("Processor run panicked: %v", r)
		} else {
			logger.Info().Msg("Processor run completed.")
		}
	}()

	logger.Debug().Msg("wrapping processor with middleware")
	p = ProcessorWithMiddleware(p, DefaultProcessorMiddleware(p.MiddlewareOptions()...)...)

	executor := commandExecutor{
		protoconv: protoConverter{},
		logger:    logger,
	}

	for {
		logger.Trace().Msg("retrieving next command")
		cmd.Reset()
		err := wasm.NextCommand(&cmd)
		if err != nil {
			if errors.Is(err, pprocutils.ErrNoMoreCommands) {
				os.Exit(0)
			}
			_, _ = fmt.Fprintf(os.Stderr, "failed retrieving next command: %v", err)
			os.Exit(1)
		}

		resp := executor.Execute(ctx, p, &cmd)
		err = wasm.Reply(resp)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed writing reply: %v\n", err)
			os.Exit(1)
		}
	}
}

func checkMagicCookie() {
	const (
		// magicCookieKey and value are used as a very basic verification
		// that a plugin is intended to be launched. This is not a security
		// measure, just a UX feature. If the magic cookie doesn't match,
		// we show human-friendly output.
		magicCookieKey   = "CONDUIT_MAGIC_COOKIE"
		magicCookieValue = "3stnegqd0x02axggy0vrc4izjeq2zik6g7somyb3ye4vy5iivvjm5s1edppl5oja"
	)
	if os.Getenv(magicCookieKey) != magicCookieValue {
		_, _ = fmt.Fprintf(os.Stderr,
			"This binary is a plugin. These are not meant to be executed directly.\n"+
				"Please execute the program that consumes these plugins, which will\n"+
				"load any plugins automatically\n")
		os.Exit(1)
	}
}

// commandExecutor executes commands received from Conduit.
type commandExecutor struct {
	protoconv protoConverter
	logger    *zerolog.Logger
}

// Execute executes the given command request. It returns a command response
// that will be sent back to Conduit.
func (e commandExecutor) Execute(ctx context.Context, p Processor, cmdReq *processorv1.CommandRequest) *processorv1.CommandResponse {
	e.logger.Trace().Type("command", cmdReq.GetRequest()).Msg("executing command")

	var resp *processorv1.CommandResponse
	var err error

	switch req := cmdReq.GetRequest().(type) {
	case *processorv1.CommandRequest_Specify:
		resp, err = e.executeSpecify(ctx, p, req.Specify)
	case *processorv1.CommandRequest_Configure:
		resp, err = e.executeConfigure(ctx, p, req.Configure)
	case *processorv1.CommandRequest_Open:
		resp, err = e.executeOpen(ctx, p, req.Open)
	case *processorv1.CommandRequest_Process:
		resp, err = e.executeProcess(ctx, p, req.Process)
	case *processorv1.CommandRequest_Teardown:
		resp, err = e.executeTeardown(ctx, p, req.Teardown)
	default:
		err = pprocutils.ErrUnknownCommandRequest
	}

	if err != nil {
		e.logger.Trace().Err(err).Msg("command returned an error")
		resp = &processorv1.CommandResponse{
			Response: &processorv1.CommandResponse_Error{
				Error: e.protoconv.error(err),
			},
		}
	}

	return resp
}

func (e commandExecutor) executeSpecify(_ context.Context, p Processor, _ *processorv1.Specify_Request) (*processorv1.CommandResponse, error) {
	spec, err := p.Specification()
	if err != nil {
		return nil, err
	}
	return &processorv1.CommandResponse{
		Response: &processorv1.CommandResponse_Specify{
			Specify: e.protoconv.specifyResponse(spec),
		},
	}, nil
}

func (e commandExecutor) executeConfigure(ctx context.Context, p Processor, req *processorv1.Configure_Request) (*processorv1.CommandResponse, error) {
	err := p.Configure(ctx, req.Parameters)
	if err != nil {
		return nil, err
	}
	return &processorv1.CommandResponse{
		Response: &processorv1.CommandResponse_Configure{
			Configure: &processorv1.Configure_Response{},
		},
	}, nil
}

func (e commandExecutor) executeOpen(ctx context.Context, p Processor, _ *processorv1.Open_Request) (*processorv1.CommandResponse, error) {
	err := p.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &processorv1.CommandResponse{
		Response: &processorv1.CommandResponse_Open{
			Open: &processorv1.Open_Response{},
		},
	}, nil
}

func (e commandExecutor) executeProcess(ctx context.Context, p Processor, req *processorv1.Process_Request) (*processorv1.CommandResponse, error) {
	records, err := e.protoconv.records(req.Records)
	if err != nil {
		return nil, fmt.Errorf("failed to convert proto opencdc records: %w", err)
	}
	processedRecords := p.Process(ctx, records)
	protoRecords, err := e.protoconv.processedRecords(processedRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to convert processed records: %w", err)
	}

	return &processorv1.CommandResponse{
		Response: &processorv1.CommandResponse_Process{
			Process: &processorv1.Process_Response{
				Records: protoRecords,
			},
		},
	}, nil
}

func (e commandExecutor) executeTeardown(ctx context.Context, p Processor, _ *processorv1.Teardown_Request) (*processorv1.CommandResponse, error) {
	err := p.Teardown(ctx)
	if err != nil {
		return nil, err
	}
	return &processorv1.CommandResponse{
		Response: &processorv1.CommandResponse_Teardown{
			Teardown: &processorv1.Teardown_Response{},
		},
	}, nil
}

// protoConverter converts between the SDK and protobuf types.
type protoConverter struct{}

func (c protoConverter) specifyResponse(in Specification) *processorv1.Specify_Response {
	params := make(map[string]*configv1.Parameter, len(in.Parameters))
	in.Parameters.ToProto(params)
	return &processorv1.Specify_Response{
		Name:        in.Name,
		Summary:     in.Summary,
		Description: in.Description,
		Version:     in.Version,
		Author:      in.Author,
		Parameters:  params,
	}
}

func (c protoConverter) records(in []*opencdcv1.Record) ([]opencdc.Record, error) {
	if in == nil {
		return nil, nil
	}
	out := make([]opencdc.Record, len(in))
	for i, v := range in {
		err := out[i].FromProto(v)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c protoConverter) processedRecords(in []ProcessedRecord) ([]*processorv1.Process_ProcessedRecord, error) {
	if in == nil {
		return nil, nil
	}

	var err error
	out := make([]*processorv1.Process_ProcessedRecord, len(in))
	for i, v := range in {
		out[i], err = c.processedRecord(v)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c protoConverter) processedRecord(in ProcessedRecord) (*processorv1.Process_ProcessedRecord, error) {
	switch v := in.(type) {
	case SingleRecord:
		return c.singleRecord(v)
	case FilterRecord:
		return c.filterRecord(v)
	case ErrorRecord:
		return c.errorRecord(v)
	default:
		return nil, fmt.Errorf("unknown processed record type: %T", in)
	}
}

func (c protoConverter) singleRecord(in SingleRecord) (*processorv1.Process_ProcessedRecord, error) {
	protoRecord := &opencdcv1.Record{}
	err := opencdc.Record(in).ToProto(protoRecord)
	if err != nil {
		return nil, err
	}
	return &processorv1.Process_ProcessedRecord{
		Record: &processorv1.Process_ProcessedRecord_SingleRecord{
			SingleRecord: protoRecord,
		},
	}, nil
}

func (c protoConverter) filterRecord(_ FilterRecord) (*processorv1.Process_ProcessedRecord, error) {
	return &processorv1.Process_ProcessedRecord{
		Record: &processorv1.Process_ProcessedRecord_FilterRecord{
			FilterRecord: &processorv1.Process_FilterRecord{},
		},
	}, nil
}

func (c protoConverter) errorRecord(in ErrorRecord) (*processorv1.Process_ProcessedRecord, error) {
	return &processorv1.Process_ProcessedRecord{
		Record: &processorv1.Process_ProcessedRecord_ErrorRecord{
			ErrorRecord: &processorv1.Process_ErrorRecord{
				Error: c.error(in.Error),
			},
		},
	}, nil
}

func (c protoConverter) error(err error) *processorv1.Error {
	var wasmErr *pprocutils.Error
	var code uint32
	if errors.As(err, &wasmErr) {
		code = wasmErr.ErrCode
	}
	return &processorv1.Error{
		Code:    code,
		Message: err.Error(),
	}
}
