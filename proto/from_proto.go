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
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
	opencdcv1 "github.com/conduitio/conduit-commons/proto/opencdc/v1"
	sdk "github.com/conduitio/conduit-processor-sdk"
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"google.golang.org/protobuf/proto"
)

func UnmarshalCommand(bytes []byte) (sdk.Command, error) {
	protoCmd := &processorv1.Command{}
	if err := proto.Unmarshal(bytes, protoCmd); err != nil {
		return nil, fmt.Errorf("failed unmarshalling %v proto bytes into a command: %w", len(bytes), err)
	}

	return sdkCommand(protoCmd)
}

func sdkCommand(cmd *processorv1.Command) (sdk.Command, error) {
	c := cmd.GetCommand()
	if c == nil {
		return nil, ErrNilCommand
	}

	switch v := c.(type) {
	case *processorv1.Command_SpecifyCmd:
		return &sdk.SpecifyCmd{}, nil
	case *processorv1.Command_ConfigureCmd:
		return &sdk.ConfigureCmd{ConfigMap: v.ConfigureCmd.Parameters}, nil
	case *processorv1.Command_OpenCmd:
		return &sdk.OpenCmd{}, nil
	case *processorv1.Command_ProcessCmd:
		records, err := opencdcRecords(v.ProcessCmd.Records)
		if err != nil {
			return nil, err
		}
		return &sdk.ProcessCmd{
			Records: records,
		}, nil
	case *processorv1.Command_TeardownCmd:
		return &sdk.TeardownCmd{}, nil
	default:
		return nil, fmt.Errorf("%T: %w", v, ErrUnknownType)
	}
}

func UnmarshalCommandResponse(bytes []byte) (sdk.CommandResponse, error) {
	protoCmd := &processorv1.CommandResponse{}
	if err := proto.Unmarshal(bytes, protoCmd); err != nil {
		return nil, fmt.Errorf("failed unmarshalling proto bytes: %w", err)
	}

	return sdkCommandResponse(protoCmd)
}

func sdkCommandResponse(cr *processorv1.CommandResponse) (sdk.CommandResponse, error) {
	resp := cr.GetResponse()
	if resp == nil {
		return nil, ErrNilCommand
	}

	switch v := resp.(type) {
	case *processorv1.CommandResponse_SpecifyResp:
		return &sdk.SpecifyResponse{
			Specification: sdkSpec(v.SpecifyResp),
			Err:           errorFromString(v.SpecifyResp.Err),
		}, nil
	case *processorv1.CommandResponse_ConfigureResp:
		return &sdk.ConfigureResponse{
			Err: errorFromString(v.ConfigureResp.Err),
		}, nil
	case *processorv1.CommandResponse_OpenResp:
		return &sdk.OpenResponse{
			Err: errorFromString(v.OpenResp.Err),
		}, nil
	case *processorv1.CommandResponse_ProcessResp:
		records, err := sdkProcessedRecord(v.ProcessResp.Records)
		if err != nil {
			return nil, err
		}
		return &sdk.ProcessResponse{
			Records: records,
		}, nil
	case *processorv1.CommandResponse_TeardownResp:
		return &sdk.TeardownResponse{
			Err: errorFromString(v.TeardownResp.Err),
		}, nil
	default:
		return nil, fmt.Errorf("%T: %w", v, ErrUnknownType)
	}
}

func errorFromString(s string) error {
	if s == "" {
		return nil
	}

	return errors.New(s) //nolint:goerr113 // the only way to deal with errors with protobuf
}

func sdkProcessedRecord(in []*processorv1.Process_ProcessedRecord) ([]sdk.ProcessedRecord, error) {
	if in == nil {
		return nil, nil
	}

	out := make([]sdk.ProcessedRecord, len(in))
	for i, r := range in {
		switch v := r.GetRecord().(type) {
		case *processorv1.Process_ProcessedRecord_SingleRecord:
			rec := opencdc.Record{}
			err := rec.FromProto(v.SingleRecord)
			if err != nil {
				return nil, fmt.Errorf("failed converting proto record to OpenCDC record: %w", err)
			}
			out[i] = sdk.SingleRecord(rec)
		case *processorv1.Process_ProcessedRecord_FilterRecord:
			out[i] = sdk.FilterRecord{}
		case *processorv1.Process_ProcessedRecord_ErrorRecord:
			out[i] = sdk.ErrorRecord{
				//nolint:goerr113 // the only way to deal with errors with protobuf
				Err: errors.New(v.ErrorRecord.Err),
			}
		default:
			return nil, fmt.Errorf("%T: %w", v, ErrUnknownType)
		}
	}

	return out, nil
}

func sdkSpec(resp *processorv1.Specify_Response) sdk.Specification {
	return sdk.Specification{
		Name:        resp.Name,
		Summary:     resp.Summary,
		Description: resp.Description,
		Version:     resp.Version,
		Author:      resp.Author,
		Parameters:  sdkSpecParams(resp.Parameters),
	}
}

func sdkSpecParams(in map[string]*processorv1.Specify_Parameter) map[string]sdk.Parameter {
	if in == nil {
		return nil
	}

	out := make(map[string]sdk.Parameter, len(in))
	for name, param := range in {
		out[name] = sdk.Parameter{
			Default:     param.Default,
			Type:        sdk.ParameterType(param.Type),
			Description: param.Description,
			Validations: sdkParamValidations(param.Validations),
		}
	}

	return out
}

func sdkParamValidations(in []*processorv1.Specify_Parameter_Validation) []sdk.Validation {
	if in == nil {
		return nil
	}

	out := make([]sdk.Validation, len(in))
	for i, v := range in {
		out[i] = sdk.Validation{
			Type:  sdk.ValidationType(v.Type),
			Value: v.Value,
		}
	}

	return out
}

func opencdcRecords(in []*opencdcv1.Record) ([]opencdc.Record, error) {
	if in == nil {
		return nil, nil
	}

	outRecs := make([]opencdc.Record, len(in))
	for i, protoRec := range in {
		rec := opencdc.Record{}
		err := rec.FromProto(protoRec)
		if err != nil {
			return nil, fmt.Errorf("failed converting protobuf record at index %v to OpenCDC record: %w", i, err)
		}

		outRecs[i] = rec
	}

	return outRecs, nil
}
