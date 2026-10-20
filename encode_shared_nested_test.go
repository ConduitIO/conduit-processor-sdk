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
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	sdkschema "github.com/conduitio/conduit-processor-sdk/schema"
)

// Regression test for conduit-processor-sdk#178 (conduit-commons#342).
//
// The schema encode middleware passes the record's StructuredData to
// Serde.Marshal. In conduit-commons <= v0.11.0 that hoisted union values in
// place, so a nested slice shared by Before and After was encoded from the
// hoisted values the second time.
func TestAudit_EncodeMiddleware_SharedNestedSlice(t *testing.T) {
	ctx := context.Background()
	sch, err := sdkschema.Create(ctx, schema.TypeAvro, "audit-shared", []byte(
		`{"type":"record","name":"r","fields":[{"name":"a","type":{"type":"array","items":["null","string",{"type":"map","values":"string"}]}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	proc := NewProcessorFunc(Specification{Name: "p"}, func(_ context.Context, rec opencdc.Record) (opencdc.Record, error) {
		shared := []any{"bar"}
		rec.Payload.Before = opencdc.StructuredData{"a": shared}
		rec.Payload.After = opencdc.StructuredData{"a": shared}
		schema.AttachPayloadSchemaToRecord(rec, sch) // processor picks a schema
		return rec, nil
	})
	p := (&ProcessorWithSchemaEncode{}).Wrap(proc)
	if err := p.Configure(ctx, config.Config{}); err != nil {
		t.Fatal(err)
	}
	out := p.Process(ctx, []opencdc.Record{{Metadata: opencdc.Metadata{}}})
	rec, ok := out[0].(SingleRecord)
	if !ok {
		t.Fatalf("got %#v", out[0])
	}
	var before, after opencdc.StructuredData
	if err := sch.Unmarshal(rec.Payload.Before.Bytes(), &before); err != nil {
		t.Fatal(err)
	}
	if err := sch.Unmarshal(rec.Payload.After.Bytes(), &after); err != nil {
		t.Fatal(err)
	}
	t.Logf("before=%v after=%v", before, after)
	if after["a"].([]any)[0] != "bar" {
		t.Errorf("BUG: after.a[0] written as %#v instead of \"bar\"", after["a"].([]any)[0])
	}
}
