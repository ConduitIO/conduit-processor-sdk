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
	opencdcv1 "github.com/conduitio/conduit-commons/proto/opencdc/v1"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	processorv1 "github.com/conduitio/conduit-processor-sdk/proto/processor/v1"
)

// protoConverter converts between the SDK and protobuf types. The conversions
// of processor results live in this file, without the wasm build constraint,
// so they are tested natively; the rest is in run.go.
type protoConverter struct{}

// processedRecords converts the processor's results. A result that cannot be
// converted (for example a record with an integer a protobuf number cannot
// hold, which conduit-commons refuses instead of rounding) becomes an
// ErrorRecord at its own index. It never fails the whole command: the host
// maps results to input records by index, so one whole-command error would be
// attributed to the first record of the batch, not to the one that failed
// (ConduitIO/conduit#3127, #3100).
func (c protoConverter) processedRecords(in []ProcessedRecord) []*processorv1.Process_ProcessedRecord {
	if in == nil {
		return nil
	}

	out := make([]*processorv1.Process_ProcessedRecord, len(in))
	for i, v := range in {
		var err error
		out[i], err = c.processedRecord(v)
		if err != nil {
			out[i] = c.errorRecord(ErrorRecord{
				Error: fmt.Errorf("failed to convert processed record %d: %w", i, err),
			})
		}
	}
	return out
}

func (c protoConverter) processedRecord(in ProcessedRecord) (*processorv1.Process_ProcessedRecord, error) {
	switch v := in.(type) {
	case SingleRecord:
		return c.singleRecord(v)
	case FilterRecord:
		return c.filterRecord(v)
	case ErrorRecord:
		return c.errorRecord(v), nil
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

func (c protoConverter) multiRecord(in MultiRecord) (*processorv1.Process_ProcessedRecord, error) {
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
