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
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
	opencdcv1 "github.com/conduitio/conduit-commons/proto/opencdc/v1"
	sdk "github.com/conduitio/conduit-processor-sdk"
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
	"google.golang.org/protobuf/proto"
)

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var validationTypes [1]struct{}
	_ = validationTypes[int(sdk.ValidationTypeRequired)-int(processorv1.Specify_Parameter_Validation_TYPE_REQUIRED)]
	_ = validationTypes[int(sdk.ValidationTypeRegex)-int(processorv1.Specify_Parameter_Validation_TYPE_REGEX)]
	_ = validationTypes[int(sdk.ValidationTypeInclusion)-int(processorv1.Specify_Parameter_Validation_TYPE_INCLUSION)]
	_ = validationTypes[int(sdk.ValidationTypeExclusion)-int(processorv1.Specify_Parameter_Validation_TYPE_EXCLUSION)]
	_ = validationTypes[int(sdk.ValidationTypeLessThan)-int(processorv1.Specify_Parameter_Validation_TYPE_LESS_THAN)]
	_ = validationTypes[int(sdk.ValidationTypeGreaterThan)-int(processorv1.Specify_Parameter_Validation_TYPE_GREATER_THAN)]

	var paramTypes [1]struct{}
	_ = paramTypes[int(sdk.ParameterTypeInt)-int(processorv1.Specify_Parameter_TYPE_INT)]
	_ = paramTypes[int(sdk.ParameterTypeFloat)-int(processorv1.Specify_Parameter_TYPE_FLOAT)]
	_ = paramTypes[int(sdk.ParameterTypeBool)-int(processorv1.Specify_Parameter_TYPE_BOOL)]
	_ = paramTypes[int(sdk.ParameterTypeString)-int(processorv1.Specify_Parameter_TYPE_STRING)]
	_ = paramTypes[int(sdk.ParameterTypeDuration)-int(processorv1.Specify_Parameter_TYPE_DURATION)]
	_ = paramTypes[int(sdk.ParameterTypeFile)-int(processorv1.Specify_Parameter_TYPE_FILE)]
}

func MarshalCommand(cmd sdk.Command) ([]byte, error) {
	protoCmd, err := protoCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed converting sdk.Command to protobuf command: %w", err)
	}

	bytes, err := proto.Marshal(protoCmd)
	if err != nil {
		return nil, fmt.Errorf("failed marshalling protobuf command: %w", err)
	}

	return bytes, nil
}

func protoCommand(cmd sdk.Command) (*processorv1.Command, error) {
	if cmd == nil {
		return nil, ErrNilCommand
	}

	switch v := cmd.(type) {
	case *sdk.SpecifyCmd:
		return &processorv1.Command{
			Command: &processorv1.Command_SpecifyCmd{
				SpecifyCmd: &processorv1.Specify_Command{},
			},
		}, nil
	case *sdk.ConfigureCmd:
		return &processorv1.Command{
			Command: &processorv1.Command_ConfigureCmd{
				ConfigureCmd: &processorv1.Configure_Command{
					Parameters: v.ConfigMap,
				},
			},
		}, nil
	case *sdk.OpenCmd:
		return &processorv1.Command{
			Command: &processorv1.Command_OpenCmd{
				OpenCmd: &processorv1.Open_Command{},
			},
		}, nil
	case *sdk.ProcessCmd:
		recs, err := protoRecords(v.Records)
		if err != nil {
			return nil, err
		}
		return &processorv1.Command{
			Command: &processorv1.Command_ProcessCmd{
				ProcessCmd: &processorv1.Process_Command{
					Records: recs,
				},
			},
		}, nil
	case *sdk.TeardownCmd:
		return &processorv1.Command{
			Command: &processorv1.Command_TeardownCmd{
				TeardownCmd: &processorv1.Teardown_Command{},
			},
		}, nil
	default:
		return nil, fmt.Errorf("%T: %w", v, ErrUnknownType)
	}
}

func MarshalCommandResponse(resp sdk.CommandResponse) ([]byte, error) {
	if resp == nil {
		return nil, ErrNilCommand
	}

	protoResp := &processorv1.CommandResponse{}
	switch v := resp.(type) {
	case *sdk.SpecifyResponse:
		protoResp.Response = &processorv1.CommandResponse_SpecifyResp{
			SpecifyResp: &processorv1.Specify_Response{
				Name:        v.Specification.Name,
				Summary:     v.Specification.Summary,
				Description: v.Specification.Description,
				Version:     v.Specification.Version,
				Author:      v.Specification.Author,
				Parameters:  protoSpecificationParams(v.Specification.Parameters),
				Err:         errorToString(v.Err),
			},
		}
	case *sdk.ConfigureResponse:
		protoResp.Response = &processorv1.CommandResponse_ConfigureResp{
			ConfigureResp: &processorv1.Configure_Response{
				Err: errorToString(v.Err),
			},
		}
	case *sdk.OpenResponse:
		protoResp.Response = &processorv1.CommandResponse_OpenResp{
			OpenResp: &processorv1.Open_Response{
				Err: errorToString(v.Err),
			},
		}
	case *sdk.ProcessResponse:
		recs, err := protoProcessedRecords(v.Records)
		if err != nil {
			return nil, err
		}
		protoResp.Response = &processorv1.CommandResponse_ProcessResp{
			ProcessResp: &processorv1.Process_Response{
				Records: recs,
			},
		}
	case *sdk.TeardownResponse:
		protoResp.Response = &processorv1.CommandResponse_TeardownResp{
			TeardownResp: &processorv1.Teardown_Response{
				Err: errorToString(v.Err),
			},
		}
	default:
		return nil, fmt.Errorf("%T: %w", v, ErrUnknownType)
	}

	bytes, err := proto.Marshal(protoResp)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshaling bytes into protobuf message: %w", err)
	}

	return bytes, nil
}

func errorToString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func protoSpecificationParams(in map[string]sdk.Parameter) map[string]*processorv1.Specify_Parameter {
	out := make(map[string]*processorv1.Specify_Parameter, len(in))
	for name, param := range in {
		out[name] = &processorv1.Specify_Parameter{
			Default:     param.Default,
			Description: param.Description,
			Type:        processorv1.Specify_Parameter_Type(param.Type),
			Validations: protoSpecValidations(param.Validations),
		}
	}

	return out
}

func protoSpecValidations(in []sdk.Validation) []*processorv1.Specify_Parameter_Validation {
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

func protoProcessedRecords(in []sdk.ProcessedRecord) ([]*processorv1.Process_ProcessedRecord, error) {
	if in == nil {
		return nil, nil
	}

	out := make([]*processorv1.Process_ProcessedRecord, len(in))
	for i, rec := range in {
		outRec := &processorv1.Process_ProcessedRecord{}
		// todo handle nil
		switch v := rec.(type) {
		case sdk.SingleRecord:
			protoRec := &opencdcv1.Record{}
			err := opencdc.Record(v).ToProto(protoRec)
			if err != nil {
				return nil, fmt.Errorf("failed converting record %v to proto: %w", i, err)
			}
			outRec.Record = &processorv1.Process_ProcessedRecord_SingleRecord{
				SingleRecord: protoRec,
			}
		case sdk.FilterRecord:
			outRec.Record = &processorv1.Process_ProcessedRecord_FilterRecord{}
		case sdk.ErrorRecord:
			outRec.Record = &processorv1.Process_ProcessedRecord_ErrorRecord{
				ErrorRecord: &processorv1.Process_ErrorRecord{
					// todo check if v.Err is nil by mistake
					Err: v.Err.Error(),
				},
			}
		}

		out[i] = outRec
	}

	return out, nil
}

func protoRecords(records []opencdc.Record) ([]*opencdcv1.Record, error) {
	out := make([]*opencdcv1.Record, len(records))
	for i, record := range records {
		outRec := &opencdcv1.Record{}
		err := record.ToProto(outRec)
		if err != nil {
			return nil, fmt.Errorf("failed converting record %v to proto: %w", i, err)
		}
		out[i] = outRec
	}

	return out, nil
}
