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

// Run is the entry-point for a standalone processor. It will set up the handler
// for the exported functions and wrap the provided Processor with middleware.
//
// A processor plugin needs to call this function in its init function. The
// entrypoint file should look like this:
//
//		//go:build wasm
//
//		package main
//
//		import (
//			sdk "github.com/conduitio/conduit-processor-sdk"
//		)
//
//		func init() {
//			processor := NewMyProcessor()
//			sdk.Run(processor)
//		}
//
//	 // Main is required by the Go compiler, but it is not executed.
//	 func main() {}
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
		p:         p,
	}

	wasm.Handler = executor
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
	p         Processor
}

func (e commandExecutor) Specification(_ *processorv1.Specify_Request) (*processorv1.Specify_Response, error) {
	spec, err := e.p.Specification()
	if err != nil {
		return nil, err
	}
	return e.protoconv.specifyResponse(spec), nil
}

func (e commandExecutor) Configure(_ *processorv1.Configure_Request) (*processorv1.Configure_Response, error) {
	err := e.p.Configure(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	return &processorv1.Configure_Response{}, nil
}

func (e commandExecutor) Open(_ *processorv1.Open_Request) (*processorv1.Open_Response, error) {
	err := e.p.Open(context.Background())
	if err != nil {
		return nil, err
	}
	return &processorv1.Open_Response{}, nil
}

func (e commandExecutor) Process(req *processorv1.Process_Request) (*processorv1.Process_Response, error) {
	records, err := e.protoconv.records(req.Records)
	if err != nil {
		return nil, fmt.Errorf("failed to convert proto opencdc records: %w", err)
	}
	processedRecords := e.p.Process(context.Background(), records)
	protoRecords, err := e.protoconv.processedRecords(processedRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to convert processed records: %w", err)
	}

	return &processorv1.Process_Response{
		Records: protoRecords,
	}, nil
}

func (e commandExecutor) Teardown(_ *processorv1.Teardown_Request) (*processorv1.Teardown_Response, error) {
	err := e.p.Teardown(context.Background())
	if err != nil {
		return nil, err
	}
	return &processorv1.Teardown_Response{}, nil
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
	case MultiRecord:
		return c.multiRecord(v)
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

func (c protoConverter) multiRecord(in MultiRecord) (*processorv1.Process_ProcessedRecord, error) {
	protoRecords := make([]*opencdcv1.Record, len(in))
	for i, v := range in {
		protoRecord := &opencdcv1.Record{}
		err := v.ToProto(protoRecord)
		if err != nil {
			return nil, err
		}
		protoRecords[i] = protoRecord
	}
	return &processorv1.Process_ProcessedRecord{
		Record: &processorv1.Process_ProcessedRecord_MultiRecord{
			MultiRecord: &processorv1.Process_MultiRecord{
				Records: protoRecords,
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
