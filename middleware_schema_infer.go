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
// Before and after share one schema, so it is inferred from both: see
// inferPayloadSchema. Nothing is registered unless every structured part
// encodes with the inferred schema without losing a field.
func (p *processorWithSchemaEncode) inferPayload(ctx context.Context, rec opencdc.Record, schemaType schema.Type, subject string) (opencdc.Record, error) {
	before, beforeOK := rec.Payload.Before.(opencdc.StructuredData)
	after, afterOK := rec.Payload.After.(opencdc.StructuredData)

	b, err := inferPayloadSchema(schemaType, before, after)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to extract schema for payload: %w", err)
	}
	inferred := schema.Schema{Subject: subject, Type: schemaType, Bytes: b}

	if err := checkInferredPayloadFields(inferred, rec.Payload); err != nil {
		return opencdc.Record{}, err // already wrapped
	}

	var encBefore, encAfter []byte
	if beforeOK {
		if encBefore, err = p.encodeWithSchema(inferred, before); err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to encode before payload: %w", err)
		}
		// Invariant 6: a field only after has is written into before as null
		// when its inferred type is nullable, a field before never had. (With
		// a non-null type the encoder above already failed.)
		if afterOK {
			for _, k := range slices.Sorted(maps.Keys(after)) {
				if _, ok := before[k]; !ok {
					return opencdc.Record{}, fmt.Errorf("failed to encode before payload: field %q is in payload.after but not in payload.before, "+
						"and one inferred schema describes both, so it would be added to payload.before: %w", k, errSchemaMismatch)
				}
			}
		}
	}
	if afterOK {
		if encAfter, err = p.encodeWithSchema(inferred, after); err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to encode after payload: %w", err)
		}
	}

	sch, err := sdkschema.Create(ctx, schemaType, subject, b)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to extract schema for payload: failed to create schema: %w", err)
	}
	if beforeOK {
		rec.Payload.Before = opencdc.RawData(encBefore)
	}
	if afterOK {
		rec.Payload.After = opencdc.RawData(encAfter)
	}
	schema.AttachPayloadSchemaToRecord(rec, sch)
	return rec, nil
}

// checkInferredPayloadFields returns an error if encoding the structured
// parts of payload with inferred, a schema inferred by inferPayloadSchema,
// would drop a field from a part. The schema's top-level fields are those of
// after (or of before if after is absent), so only before can lose one.
func checkInferredPayloadFields(inferred schema.Schema, payload opencdc.Change) error {
	before, beforeOK := payload.Before.(opencdc.StructuredData)
	after, afterOK := payload.After.(opencdc.StructuredData)
	if beforeOK && afterOK {
		// Invariant 6: the Avro encoder ignores a map key the record schema
		// does not define. A field the processor removed from after but left
		// in before (field.exclude on .Payload.After.x) is not in the schema,
		// so before would lose it without an error. Fail the record instead.
		for _, k := range slices.Sorted(maps.Keys(before)) { // sorted: the same record always reports the same field
			if _, ok := after[k]; !ok {
				return fmt.Errorf("failed to encode before payload: field %q is in payload.before but not in payload.after, "+
					"and one inferred schema describes both, so it would be dropped from payload.before; "+
					"remove it from both parts or from neither: %w", k, ErrFieldNotInSchema)
			}
		}
	}
	// Nested fields: the walk reports a map key below the top level that the
	// schema does not define.
	return checkPayloadNoDroppedFields(inferred, payload)
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

	factory, ok := schema.KnownSerdeFactories[schemaType]
	if !ok {
		return nil, fmt.Errorf("no schema extractor for schema type %v", schemaType)
	}
	srd, err := factory.SerdeForType(witness)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema for value: %w", err)
	}
	b := []byte(srd.String())
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
