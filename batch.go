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
	"context"
	"errors"
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
)

// ProcessBatchOrProcess invokes the processor's batch surface when present,
// otherwise it falls back to calling [Processor.Process] per record, so every
// existing processor runs unchanged.
//
// When p implements [BatchProcessor], the returned slice is enforced to be the
// same length as the input (the 1:1 index contract of the batch surface): a
// whole-batch error or a wrong-length result is returned as a non-nil error, so
// the caller can reject the batch without committing it. When p does not
// implement the batch surface — including when it is hidden behind a middleware
// wrapper that returns [ErrNotBatchProcessor] — the result of [Processor.Process]
// is returned as-is with a nil error, preserving the per-record semantics.
func ProcessBatchOrProcess(ctx context.Context, p Processor, records []opencdc.Record) ([]ProcessedRecord, error) {
	bp, ok := p.(BatchProcessor)
	if !ok {
		return p.Process(ctx, records), nil
	}

	out, err := bp.ProcessBatch(ctx, records)
	switch {
	case errors.Is(err, ErrNotBatchProcessor):
		return p.Process(ctx, records), nil
	case err != nil:
		return nil, fmt.Errorf("processor ProcessBatch: %w", err)
	case len(out) != len(records):
		return nil, fmt.Errorf("processor ProcessBatch returned %d results for %d records", len(out), len(records))
	default:
		return out, nil
	}
}
