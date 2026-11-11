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
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	sdkschema "github.com/conduitio/conduit-processor-sdk/schema"
)

// inferPayload encodes a payload that no longer fits its carried schema with a
// schema inferred from the values (see [SchemaEncodeStrategyInfer]), registers
// that schema as a new version of subject and attaches it.
//
// Before and after share one schema. It is the schema extracted from after (or
// from before if after is absent), as it always was, if both parts survive an
// encode and decode with it unchanged; that keeps the schema text, and so the
// registered version, of every shape that already encoded. Otherwise it is
// inferred from both parts (see inferPayloadSchema), which is what lets a
// nullable column change between a value and NULL. Nothing is registered
// unless both parts encode, and a field one part would lose fails the record.
func (p *processorWithSchemaEncode) inferPayload(ctx context.Context, rec opencdc.Record, schemaType schema.Type, subject string) (opencdc.Record, error) {
	// A nil StructuredData part holds no data, like a nil part: it is not
	// encoded and left as it is.
	before, _ := rec.Payload.Before.(opencdc.StructuredData)
	after, _ := rec.Payload.After.(opencdc.StructuredData)

	// Invariant 6: a field the processor removed from after but left in
	// before would be dropped from before by any schema inferred from after.
	// This does not depend on the schema, so it is checked first.
	if before == nil && after == nil {
		return rec, nil // nothing to encode: both parts are nil StructuredData
	}
	if err := checkBeforeFieldsInAfter(rec.Payload); err != nil {
		return opencdc.Record{}, err // already wrapped
	}

	b, encBefore, encAfter, err := p.inferAndEncodePayload(schemaType, subject, rec.Payload)
	if err != nil {
		return opencdc.Record{}, err // already wrapped
	}

	sch, err := sdkschema.Create(ctx, schemaType, subject, b)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to extract schema for payload: failed to create schema: %w", err)
	}
	if before != nil {
		rec.Payload.Before = opencdc.RawData(encBefore)
	}
	if after != nil {
		rec.Payload.After = opencdc.RawData(encAfter)
	}
	schema.AttachPayloadSchemaToRecord(rec, sch)
	return rec, nil
}

// inferAndEncodePayload returns the text of the schema inferPayload uses and
// the structured parts of payload encoded with it (nil for a part that is not
// structured). See inferPayload for which schema that is.
func (p *processorWithSchemaEncode) inferAndEncodePayload(schemaType schema.Type, subject string, payload opencdc.Change) ([]byte, []byte, []byte, error) {
	before, _ := payload.Before.(opencdc.StructuredData)
	after, _ := payload.After.(opencdc.StructuredData)
	primary := after
	if primary == nil {
		primary = before
	}

	// The schema extracted from after alone, as before nullable fields were
	// inferred from both parts. Used only if both parts round-trip with it
	// exactly: a successful encode is not enough, since the codec writes an
	// empty map into a ["null","string"] field as null.
	b, err := extractSchema(schemaType, primary)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to extract schema for payload: %w", err)
	}
	if encBefore, encAfter, ok := encodePayloadPartsLossless(schema.Schema{Subject: subject, Type: schemaType, Bytes: b}, before, after); ok {
		return b, encBefore, encAfter, nil
	}

	inferred, err := inferPayloadSchema(schemaType, before, after)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to extract schema for payload: %w", err)
	}
	sch := schema.Schema{Subject: subject, Type: schemaType, Bytes: inferred}
	if err := checkPayloadNoDroppedFields(sch, payload); err != nil {
		return nil, nil, nil, err // already wrapped
	}
	// Not a lossless encode: the infer strategy accepts what extraction loses
	// (int widened to long, times truncated to microseconds), as documented.
	var encBefore, encAfter []byte
	if before != nil {
		if encBefore, err = p.encodeWithSchema(sch, before); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to encode before payload: %w", err)
		}
	}
	if after != nil {
		if encAfter, err = p.encodeWithSchema(sch, after); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to encode after payload: %w", err)
		}
	}
	return inferred, encBefore, encAfter, nil
}

// encodePayloadPartsLossless encodes the non-nil parts with sch, and reports
// whether every one of them comes back unchanged (see encodeLossless).
func encodePayloadPartsLossless(sch schema.Schema, before, after opencdc.StructuredData) ([]byte, []byte, bool) {
	var encBefore, encAfter []byte
	var err error
	if before != nil {
		if encBefore, err = encodeLossless(sch, before); err != nil {
			return nil, nil, false
		}
	}
	if after != nil {
		if encAfter, err = encodeLossless(sch, after); err != nil {
			return nil, nil, false
		}
	}
	return encBefore, encAfter, true
}

// extractSchema returns the text of the schema the extractor infers for data.
func extractSchema(schemaType schema.Type, data opencdc.StructuredData) ([]byte, error) {
	factory, ok := schema.KnownSerdeFactories[schemaType]
	if !ok {
		return nil, fmt.Errorf("no schema extractor for schema type %v", schemaType)
	}
	srd, err := factory.SerdeForType(data)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema for value: %w", err)
	}
	return []byte(srd.String()), nil
}

// checkBeforeFieldsInAfter returns an error wrapping ErrFieldNotInSchema if
// payload has a structured before and after and before has a top-level field
// after lacks.
//
// Invariant 6: the Avro encoder ignores a map key the record schema does not
// define. The inferred schema has after's top-level fields, so a field the
// processor removed from after but left in before (field.exclude on
// .Payload.After.x) would be dropped from before without an error. The walk in
// checkNoDroppedFields reports it too; this check names both parts, and runs
// before a schema is extracted. Nested fields are left to the walk.
func checkBeforeFieldsInAfter(payload opencdc.Change) error {
	before, beforeOK := payload.Before.(opencdc.StructuredData)
	after, afterOK := payload.After.(opencdc.StructuredData)
	if !beforeOK || !afterOK || before == nil || after == nil {
		return nil
	}
	for _, k := range slices.Sorted(maps.Keys(before)) { // sorted: the same record always reports the same field
		if _, ok := after[k]; !ok {
			return fmt.Errorf("failed to encode before payload: field %q is in payload.before but not in payload.after, "+
				"and one inferred schema describes both, so it would be dropped from payload.before; "+
				"remove it from both parts or from neither: %w", k, ErrFieldNotInSchema)
		}
	}
	return nil
}

// inferPayloadSchema returns the text of a schema inferred from the structured
// payload parts (a nil part is absent). At least one part must be non-nil.
//
// It is the schema the extractor infers from after (or from before if after
// is absent), with two changes for top-level fields, so that one schema fits
// both parts of an update where a nullable column changed between a value and
// NULL:
//
//   - A field that is nil in after takes its type from before's value. The
//     extractor cannot type a nil and guesses ["null","string"], which before's
//     value (an int, say) does not fit.
//   - A field that is nil in one part and set in the other is made nullable.
//     The extractor types a set value as non-null, which the nil does not fit.
//
// If neither change applies, the text is exactly what the extractor returns
// for after, as before this function existed. A field that is set in both
// parts takes after's type; if before's value does not fit it, encoding before
// fails, and the record with it. Nested fields are inferred from one part only.
func inferPayloadSchema(schemaType schema.Type, before, after opencdc.StructuredData) ([]byte, error) {
	primary, other := after, before
	if primary == nil {
		primary, other = before, nil
	}

	// witness holds a value for every field of primary: primary's value, or
	// other's if primary's is nil, so the extractor sees a typed value
	// wherever either part has one.
	witness := primary
	nullable := map[string]bool{}
	if other != nil {
		witness = make(opencdc.StructuredData, len(primary))
		for k, v := range primary {
			ov, inOther := other[k]
			switch {
			case isNil(v) && inOther && !isNil(ov):
				witness[k] = ov
				nullable[k] = true
			case !isNil(v) && inOther && isNil(ov):
				witness[k] = v
				nullable[k] = true
			default:
				witness[k] = v
			}
		}
	}

	b, err := extractSchema(schemaType, witness)
	if err != nil {
		return nil, err
	}
	if len(nullable) == 0 || schemaType != schema.TypeAvro {
		return b, nil
	}
	return makeFieldsNullable(b, nullable)
}

// makeFieldsNullable rewrites the type of each named top-level field of an
// Avro record schema as a union whose first branch is null. Other fields and
// properties are kept as they are.
func makeFieldsNullable(avroSchema []byte, names map[string]bool) ([]byte, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(avroSchema, &top); err != nil {
		return nil, fmt.Errorf("failed to read the inferred schema: %w", err)
	}
	var fields []map[string]json.RawMessage
	if err := json.Unmarshal(top["fields"], &fields); err != nil {
		return nil, fmt.Errorf("failed to read the inferred schema fields: %w", err)
	}
	for _, f := range fields {
		var name string
		if err := json.Unmarshal(f["name"], &name); err != nil {
			return nil, fmt.Errorf("failed to read an inferred field name: %w", err)
		}
		if !names[name] {
			continue
		}
		typ, err := nullableType(f["type"])
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}
		f["type"] = typ
	}
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("failed to build the inferred schema fields: %w", err)
	}
	top["fields"] = fieldsJSON
	out, err := json.Marshal(top)
	if err != nil {
		return nil, fmt.Errorf("failed to build the inferred schema: %w", err)
	}
	return out, nil
}
