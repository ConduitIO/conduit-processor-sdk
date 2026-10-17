// Copyright © 2026 Meroxa, Inc.
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
	"errors"
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
	configv1 "github.com/conduitio/conduit-commons/proto/config/v1"
	opencdcv1 "github.com/conduitio/conduit-commons/proto/opencdc/v1"
	opencdcv2 "github.com/conduitio/conduit-commons/proto/opencdc/v2"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
)

// protoConverter converts between the SDK and protobuf types. The conversions
// live in this file, without the wasm build constraint, so they are tested
// natively; the rest of the guest loop is in run.go.
//
// A record can cross the boundary in either of two shapes: opencdc.v1 (the
// lossy google.protobuf.Struct shape, which refuses integers outside
// ±(2^53−1)) or opencdc.v2 (the lossless shape that carries int64, uint64,
// bytes and decimal). The v2 methods take a bool so a command is converted in
// one shape end to end; the shape never changes mid-command.
type protoConverter struct{}

// specifyResponse converts a Specification into its protobuf representation.
// It lives here (without the wasm build constraint) so the stateless and
// structured_values_v2 declarations round-trip under native tests.
//
// StructuredValuesV2 is always set: it is a capability of the SDK, not a
// processor-author choice. Every guest built against this SDK can process
// opencdc.v2 records, so it declares the capability and the host sends it v2
// records. A processor author cannot opt out — the flag describes what the
// SDK's conversion layer supports.
func (c protoConverter) specifyResponse(in Specification) *processorv1.Specify_Response {
	params := make(map[string]*configv1.Parameter, len(in.Parameters))
	in.Parameters.ToProto(params)
	return &processorv1.Specify_Response{
		Name:               in.Name,
		Summary:            in.Summary,
		Description:        in.Description,
		Version:            in.Version,
		Author:             in.Author,
		Parameters:         params,
		Stateless:          in.Stateless,
		StructuredValuesV2: true,
	}
}

// processedRecords converts the processor's results. A result that cannot be
// converted (for example a v1 record with an integer a protobuf number cannot
// hold, which conduit-commons refuses instead of rounding) becomes an
// ErrorRecord at its own index. It never fails the whole command: the host
// maps results to input records by index, so one whole-command error would be
// attributed to the first record of the batch, not to the one that failed
// (ConduitIO/conduit#3127, #3100).
func (c protoConverter) processedRecords(in []ProcessedRecord, v2 bool) []*processorv1.Process_ProcessedRecord {
	if in == nil {
		return nil
	}

	out := make([]*processorv1.Process_ProcessedRecord, len(in))
	for i, v := range in {
		var err error
		out[i], err = c.processedRecord(v, v2)
		if err != nil {
			out[i] = c.errorRecord(ErrorRecord{
				Error: fmt.Errorf("failed to convert processed record %d: %w", i, err),
			})
		}
	}
	return out
}

func (c protoConverter) processedRecord(in ProcessedRecord, v2 bool) (*processorv1.Process_ProcessedRecord, error) {
	switch v := in.(type) {
	case SingleRecord:
		return c.singleRecord(v, v2)
	case FilterRecord:
		return c.filterRecord(v)
	case ErrorRecord:
		return c.errorRecord(v), nil
	case MultiRecord:
		return c.multiRecord(v, v2)
	default:
		return nil, fmt.Errorf("unknown processed record type: %T", in)
	}
}

func (c protoConverter) singleRecord(in SingleRecord, v2 bool) (*processorv1.Process_ProcessedRecord, error) {
	if v2 {
		protoRecord := &opencdcv2.Record{}
		err := opencdc.Record(in).ToProtoV2(protoRecord)
		if err != nil {
			return nil, fmt.Errorf("single record: %w", err)
		}
		return &processorv1.Process_ProcessedRecord{
			Record: &processorv1.Process_ProcessedRecord_SingleRecordV2{
				SingleRecordV2: protoRecord,
			},
		}, nil
	}

	protoRecord := &opencdcv1.Record{}
	err := opencdc.Record(in).ToProto(protoRecord)
	if err != nil {
		return nil, fmt.Errorf("single record: %w", err)
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

func (c protoConverter) errorRecord(in ErrorRecord) *processorv1.Process_ProcessedRecord {
	return &processorv1.Process_ProcessedRecord{
		Record: &processorv1.Process_ProcessedRecord_ErrorRecord{
			ErrorRecord: &processorv1.Process_ErrorRecord{
				Error: c.error(in.Error),
			},
		},
	}
}

func (c protoConverter) multiRecord(in MultiRecord, v2 bool) (*processorv1.Process_ProcessedRecord, error) {
	if v2 {
		protoRecords := make([]*opencdcv2.Record, len(in))
		for i, v := range in {
			protoRecord := &opencdcv2.Record{}
			err := v.ToProtoV2(protoRecord)
			if err != nil {
				return nil, fmt.Errorf("multi-record entry %d: %w", i, err)
			}
			protoRecords[i] = protoRecord
		}
		return &processorv1.Process_ProcessedRecord{
			Record: &processorv1.Process_ProcessedRecord_MultiRecordV2{
				MultiRecordV2: &processorv1.Process_MultiRecordV2{
					Records: protoRecords,
				},
			},
		}, nil
	}

	protoRecords := make([]*opencdcv1.Record, len(in))
	for i, v := range in {
		protoRecord := &opencdcv1.Record{}
		err := v.ToProto(protoRecord)
		if err != nil {
			return nil, fmt.Errorf("multi-record entry %d: %w", i, err)
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

// records converts the host's opencdc.v1 Process request records back into
// opencdc.Record values.
//
//nolint:unused // called from run.go's v1 path (req.GetRecordsV2() == nil); staticcheck misses the value-receiver call
func (c protoConverter) records(in []*opencdcv1.Record) ([]opencdc.Record, error) {
	if in == nil {
		return nil, nil
	}
	out := make([]opencdc.Record, len(in))
	for i, v := range in {
		err := out[i].FromProto(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert record %d from opencdc.v1: %w", i, err)
		}
	}
	return out, nil
}

// recordsV2 converts the host's opencdc.v2 Process request records back into
// opencdc.Record values. The v2 shape is lossless, so int64, uint64, bytes and
// decimal values arrive exactly as the host sent them.
func (c protoConverter) recordsV2(in []*opencdcv2.Record) ([]opencdc.Record, error) {
	if in == nil {
		return nil, nil
	}
	out := make([]opencdc.Record, len(in))
	for i, v := range in {
		err := out[i].FromProtoV2(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert record %d from opencdc.v2: %w", i, err)
		}
	}
	return out, nil
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
