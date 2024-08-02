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

package sdk

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	procschema "github.com/conduitio/conduit-processor-sdk/schema"
)

// ProcessorMiddleware wraps a Processor and adds functionality to it.
type ProcessorMiddleware interface {
	Wrap(Processor) Processor
}

// ProcessorMiddlewareOption is a function that can be used to configure a
// ProcessorMiddleware.
type ProcessorMiddlewareOption interface {
	Apply(ProcessorMiddleware)
}

// Available middleware options.
var (
	_ ProcessorMiddlewareOption = ProcessorWithSchemaEncodeConfig{}
	_ ProcessorMiddlewareOption = ProcessorWithSchemaDecodeConfig{}
)

// DefaultProcessorMiddleware returns a slice of middleware that is added to all
// processors by default.
func DefaultProcessorMiddleware(opts ...ProcessorMiddlewareOption) []ProcessorMiddleware {
	middleware := []ProcessorMiddleware{
		&ProcessorWithSchemaDecode{},
		&ProcessorWithSchemaEncode{},
	}

	// apply options to all middleware
	for _, m := range middleware {
		for _, opt := range opts {
			opt.Apply(m)
		}
	}
	return middleware
}

// ProcessorWithMiddleware wraps the processor into the supplied middleware.
func ProcessorWithMiddleware(p Processor, middleware ...ProcessorMiddleware) Processor {
	// apply middleware in reverse order to preserve the order as specified
	for i := len(middleware) - 1; i >= 0; i-- {
		p = middleware[i].Wrap(p)
	}
	return p
}

// -- ProcessorWithSchemaDecode --------------------------------------------

const (
	configProcessorWithSchemaDecodePayloadEnabled = "sdk.schema.decode.payload.enabled"
	configProcessorWithSchemaDecodeKeyEnabled     = "sdk.schema.decode.key.enabled"
)

// ProcessorWithSchemaDecodeConfig is the configuration for the
// ProcessorWithSchemaDecode middleware. Fields set to their zero value are
// ignored and will be set to the default value.
//
// ProcessorWithSchemaDecodeConfig can be used as a ProcessorMiddlewareOption.
type ProcessorWithSchemaDecodeConfig struct {
	// Whether to decode the record payload with a schema.
	// If unset, defaults to true.
	PayloadEnabled *bool
	// Whether to decode and decode the record key with a schema.
	// If unset, defaults to true.
	KeyEnabled *bool
}

// Apply sets the default configuration for the ProcessorWithSchemaDecode middleware.
func (c ProcessorWithSchemaDecodeConfig) Apply(m ProcessorMiddleware) {
	if p, ok := m.(*ProcessorWithSchemaDecode); ok {
		p.Config = c
	}
}

func (c ProcessorWithSchemaDecodeConfig) SchemaPayloadEnabledParameterName() string {
	return configProcessorWithSchemaDecodePayloadEnabled
}

func (c ProcessorWithSchemaDecodeConfig) SchemaKeyEnabledParameterName() string {
	return configProcessorWithSchemaDecodeKeyEnabled
}

func (c ProcessorWithSchemaDecodeConfig) parameters() config.Parameters {
	return config.Parameters{
		configProcessorWithSchemaDecodeKeyEnabled: {
			Default:     strconv.FormatBool(*c.KeyEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to decode the record key with a schema.",
		},
		configProcessorWithSchemaDecodePayloadEnabled: {
			Default:     strconv.FormatBool(*c.PayloadEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to decode the record payload with a schema.",
		},
	}
}

// ProcessorWithSchemaDecode is a middleware that decodes the key and/or payload
// of a record using a schema before passing it to the processor. It takes the
// schema subject and version from the record metadata, fetches the schema from
// the schema service, and decodes the key and/or payload using the schema.
// If the schema subject and version is not found in the record metadata, it will
// log a warning and skip decoding the key and/or payload. This middleware is
// useful when the source connector sends the data with the schema attached.
//
// It adds two parameters to the processor config:
//   - `sdk.schema.decode.key.enabled` - Whether to decode and decode the
//     record key with a schema.
//   - `sdk.schema.decode.payload.enabled` - Whether to decode and decode the
//     record payload with a schema.
type ProcessorWithSchemaDecode struct {
	Config ProcessorWithSchemaDecodeConfig
}

// Wrap a Processor into the schema middleware. It will apply default configuration
// values if they are not explicitly set.
func (s *ProcessorWithSchemaDecode) Wrap(impl Processor) Processor {
	if s.Config.KeyEnabled == nil {
		s.Config.KeyEnabled = lang.Ptr(true)
	}
	if s.Config.PayloadEnabled == nil {
		s.Config.PayloadEnabled = lang.Ptr(true)
	}
	return &processorWithSchemaDecode{
		Processor: impl,
		defaults:  s.Config,
	}
}

// processorWithSchemaDecode is the actual middleware implementation.
type processorWithSchemaDecode struct {
	Processor
	defaults ProcessorWithSchemaDecodeConfig

	payloadEnabled bool
	keyEnabled     bool

	payloadWarnOnce sync.Once
	keyWarnOnce     sync.Once
}

func (p *processorWithSchemaDecode) Specification() (Specification, error) {
	spec, err := p.Processor.Specification()
	if err != nil {
		return spec, err //nolint:wrapcheck // middleware shouldn't wrap errors
	}

	// merge parameters from the processor and the schema decode middleware
	spec.Parameters = mergeParameters(spec.Parameters, p.defaults.parameters())
	return spec, nil
}

func (p *processorWithSchemaDecode) Configure(ctx context.Context, config config.Config) error {
	err := p.Processor.Configure(ctx, config)
	if err != nil {
		return err //nolint:wrapcheck // middleware shouldn't wrap errors
	}

	p.keyEnabled = *p.defaults.KeyEnabled
	if val, ok := config[configProcessorWithSchemaDecodeKeyEnabled]; ok {
		p.keyEnabled, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorWithSchemaDecodeKeyEnabled, err)
		}
	}

	p.payloadEnabled = *p.defaults.PayloadEnabled
	if val, ok := config[configProcessorWithSchemaDecodePayloadEnabled]; ok {
		p.payloadEnabled, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorWithSchemaDecodePayloadEnabled, err)
		}
	}

	return nil
}

func (p *processorWithSchemaDecode) Process(ctx context.Context, records []opencdc.Record) []ProcessedRecord {
	if p.keyEnabled {
		for i := range records {
			if err := p.decodeKey(ctx, &records[i]); err != nil {
				if len(records) > 0 {
					err = fmt.Errorf("record %d: %w", i, err)
				}
				return []ProcessedRecord{ErrorRecord{Error: err}}
			}
		}
	}
	if p.payloadEnabled {
		for i := range records {
			if err := p.decodePayload(ctx, &records[i]); err != nil {
				if len(records) > 0 {
					err = fmt.Errorf("record %d: %w", i, err)
				}
				return []ProcessedRecord{ErrorRecord{Error: err}}
			}
		}
	}

	return p.Processor.Process(ctx, records)
}

func (p *processorWithSchemaDecode) decodeKey(ctx context.Context, rec *opencdc.Record) error {
	subject, errSubject := rec.Metadata.GetKeySchemaSubject()
	version, errVersion := rec.Metadata.GetKeySchemaVersion()
	switch {
	case errSubject != nil && !errors.Is(errSubject, opencdc.ErrMetadataFieldNotFound):
		return fmt.Errorf("failed to get key schema subject from metadata: %w", errSubject)
	case errVersion != nil && !errors.Is(errVersion, opencdc.ErrMetadataFieldNotFound):
		return fmt.Errorf("failed to get key schema version from metadata: %w", errVersion)
	case errors.Is(errSubject, opencdc.ErrMetadataFieldNotFound) ||
		errors.Is(errVersion, opencdc.ErrMetadataFieldNotFound):
		// log warning once, to avoid spamming the logs
		p.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record does not have an attached schema for the key, consider disabling the processor schema key decoding using "%s: false"`, configProcessorWithSchemaDecodeKeyEnabled)
		})
		return nil
	}

	if rec.Key != nil {
		decodedKey, err := p.decode(ctx, rec.Key, subject, version)
		if err != nil {
			return fmt.Errorf("failed to decode key: %w", err)
		}
		rec.Key = decodedKey
	}

	return nil
}

func (p *processorWithSchemaDecode) decodePayload(ctx context.Context, rec *opencdc.Record) error {
	subject, errSubject := rec.Metadata.GetPayloadSchemaSubject()
	version, errVersion := rec.Metadata.GetPayloadSchemaVersion()
	switch {
	case errSubject != nil && !errors.Is(errSubject, opencdc.ErrMetadataFieldNotFound):
		return fmt.Errorf("failed to get payload schema subject from metadata: %w", errSubject)
	case errVersion != nil && !errors.Is(errVersion, opencdc.ErrMetadataFieldNotFound):
		return fmt.Errorf("failed to get payload schema version from metadata: %w", errVersion)
	case errors.Is(errSubject, opencdc.ErrMetadataFieldNotFound) ||
		errors.Is(errVersion, opencdc.ErrMetadataFieldNotFound):
		// log warning once, to avoid spamming the logs
		p.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record does not have an attached schema for the payload, consider disabling the processor schema payload decoding using "%s: false"`, configProcessorWithSchemaDecodePayloadEnabled)
		})
		return nil
	}

	if rec.Payload.Before != nil {
		decodedPayloadBefore, err := p.decode(ctx, rec.Payload.Before, subject, version)
		if err != nil {
			return fmt.Errorf("failed to decode payload.before: %w", err)
		}
		rec.Payload.Before = decodedPayloadBefore
	}
	if rec.Payload.After != nil {
		decodedPayloadAfter, err := p.decode(ctx, rec.Payload.After, subject, version)
		if err != nil {
			return fmt.Errorf("failed to decode payload.after: %w", err)
		}
		rec.Payload.After = decodedPayloadAfter
	}

	return nil
}

func (p *processorWithSchemaDecode) decode(ctx context.Context, data opencdc.Data, subject string, version int) (opencdc.StructuredData, error) {
	switch data := data.(type) {
	case opencdc.StructuredData:
		return data, nil // already decoded
	case opencdc.RawData: // let's decode it
	default:
		return nil, fmt.Errorf("unexpected data type %T", data)
	}

	sch, err := procschema.Get(ctx, subject, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for %s:%d: %w", subject, version, err)
	}

	var structuredData opencdc.StructuredData
	err = sch.Unmarshal(data.Bytes(), &structuredData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal bytes with schema: %w", err)
	}

	return structuredData, nil
}

// -- ProcessorWithSchemaEncode ----------------------------------------------

const (
	configProcessorSchemaEncodeType           = "sdk.schema.encode.type"
	configProcessorSchemaEncodePayloadEnabled = "sdk.schema.encode.payload.enabled"
	configProcessorSchemaEncodePayloadSubject = "sdk.schema.encode.payload.subject"
	configProcessorSchemaEncodeKeyEnabled     = "sdk.schema.encode.key.enabled"
	configProcessorSchemaEncodeKeySubject     = "sdk.schema.encode.key.subject"
)

// ProcessorWithSchemaEncodeConfig is the configuration for the
// ProcessorWithSchemaEncode middleware. Fields set to their zero value are
// ignored and will be set to the default value.
//
// ProcessorWithSchemaEncodeConfig can be used as a ProcessorMiddlewareOption.
type ProcessorWithSchemaEncodeConfig struct {
	// The type of the payload schema. Defaults to Avro.
	SchemaType schema.Type
	// Whether to encode the record payload with a schema.
	// If unset, defaults to true.
	PayloadEnabled *bool
	// The subject of the payload schema. If unset, defaults to "payload".
	PayloadSubject *string
	// Whether to encode the record key with a schema.
	// If unset, defaults to true.
	KeyEnabled *bool
	// The subject of the key schema. If unset, defaults to "key".
	KeySubject *string
}

// Apply sets the default configuration for the ProcessorWithSchemaEncode middleware.
func (c ProcessorWithSchemaEncodeConfig) Apply(m ProcessorMiddleware) {
	if s, ok := m.(*ProcessorWithSchemaEncode); ok {
		s.Config = c
	}
}

func (c ProcessorWithSchemaEncodeConfig) SchemaTypeParameterName() string {
	return configProcessorSchemaEncodeType
}

func (c ProcessorWithSchemaEncodeConfig) SchemaPayloadEnabledParameterName() string {
	return configProcessorSchemaEncodePayloadEnabled
}

func (c ProcessorWithSchemaEncodeConfig) SchemaPayloadSubjectParameterName() string {
	return configProcessorSchemaEncodePayloadSubject
}

func (c ProcessorWithSchemaEncodeConfig) SchemaKeyEnabledParameterName() string {
	return configProcessorSchemaEncodeKeyEnabled
}

func (c ProcessorWithSchemaEncodeConfig) SchemaKeySubjectParameterName() string {
	return configProcessorSchemaEncodeKeySubject
}

func (c ProcessorWithSchemaEncodeConfig) parameters() config.Parameters {
	return config.Parameters{
		configProcessorSchemaEncodeType: {
			Default:     c.SchemaType.String(),
			Type:        config.ParameterTypeString,
			Description: "The type of the payload schema.",
			Validations: []config.Validation{
				config.ValidationInclusion{List: c.types()},
			},
		},
		configProcessorSchemaEncodePayloadEnabled: {
			Default:     strconv.FormatBool(*c.PayloadEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to encode the record payload with a schema.",
		},
		configProcessorSchemaEncodePayloadSubject: {
			Default:     *c.PayloadSubject,
			Type:        config.ParameterTypeString,
			Description: `The subject of the payload schema. If the record metadata contains the field "opencdc.collection" it is prepended to the subject name and separated with a dot.`,
		},
		configProcessorSchemaEncodeKeyEnabled: {
			Default:     strconv.FormatBool(*c.KeyEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to encode the record key with a schema.",
		},
		configProcessorSchemaEncodeKeySubject: {
			Default:     *c.KeySubject,
			Type:        config.ParameterTypeString,
			Description: `The subject of the key schema. If the record metadata contains the field "opencdc.collection" it is prepended to the subject name and separated with a dot.`,
		},
	}
}

func (c ProcessorWithSchemaEncodeConfig) types() []string {
	out := make([]string, 0, len(schema.KnownSerdeFactories))
	for t := range schema.KnownSerdeFactories {
		out = append(out, t.String())
	}
	return out
}

// ProcessorWithSchemaEncode is a middleware that encodes the record
// payload and key with a schema. The schema is encodeed from the record data
// for each record produced by the source. The schema is registered with the
// schema service and the schema subject is attached to the record metadata.
type ProcessorWithSchemaEncode struct {
	Config ProcessorWithSchemaEncodeConfig
}

// Wrap a Processor into the schema middleware. It will apply default configuration
// values if they are not explicitly set.
func (p *ProcessorWithSchemaEncode) Wrap(impl Processor) Processor {
	if p.Config.SchemaType == 0 {
		p.Config.SchemaType = schema.TypeAvro
	}

	if p.Config.KeyEnabled == nil {
		p.Config.KeyEnabled = lang.Ptr(true)
	}
	if p.Config.KeySubject == nil {
		p.Config.KeySubject = lang.Ptr("key")
	}

	if p.Config.PayloadEnabled == nil {
		p.Config.PayloadEnabled = lang.Ptr(true)
	}
	if p.Config.PayloadSubject == nil {
		p.Config.PayloadSubject = lang.Ptr("payload")
	}

	return &processorWithSchemaEncode{
		Processor: impl,
		defaults:  p.Config,
	}
}

// processorWithSchemaEncode is the actual middleware implementation.
type processorWithSchemaEncode struct {
	Processor
	defaults ProcessorWithSchemaEncodeConfig

	schemaType     schema.Type
	payloadSubject string
	keySubject     string

	payloadWarnOnce sync.Once
	keyWarnOnce     sync.Once
}

func (p *processorWithSchemaEncode) Specification() (Specification, error) {
	spec, err := p.Processor.Specification()
	if err != nil {
		return spec, err //nolint:wrapcheck // middleware shouldn't wrap errors
	}

	// merge parameters from the processor and the schema decode middleware
	spec.Parameters = mergeParameters(spec.Parameters, p.defaults.parameters())
	return spec, nil
}

func (p *processorWithSchemaEncode) Configure(ctx context.Context, config config.Config) error {
	err := p.Processor.Configure(ctx, config)
	if err != nil {
		return err //nolint:wrapcheck // middleware shouldn't wrap errors
	}

	p.schemaType = p.defaults.SchemaType
	if val, ok := config[configProcessorSchemaEncodeType]; ok {
		if err := p.schemaType.UnmarshalText([]byte(val)); err != nil {
			return fmt.Errorf("invalid %s: failed to parse schema type: %w", configProcessorSchemaEncodeType, err)
		}
	}

	encodeKey := *p.defaults.KeyEnabled
	if val, ok := config[configProcessorSchemaEncodeKeyEnabled]; ok {
		encodeKey, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorSchemaEncodeKeyEnabled, err)
		}
	}
	if encodeKey {
		p.keySubject = *p.defaults.KeySubject
		if val, ok := config[configProcessorSchemaEncodeKeySubject]; ok {
			p.keySubject = val
		}
	}

	encodePayload := *p.defaults.PayloadEnabled
	if val, ok := config[configProcessorSchemaEncodePayloadEnabled]; ok {
		encodePayload, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorSchemaEncodePayloadEnabled, err)
		}
	}
	if encodePayload {
		p.payloadSubject = *p.defaults.PayloadSubject
		if val, ok := config[configProcessorSchemaEncodePayloadSubject]; ok {
			p.payloadSubject = val
		}
	}

	return nil
}

func (p *processorWithSchemaEncode) Process(ctx context.Context, records []opencdc.Record) []ProcessedRecord {
	recsOut := p.Processor.Process(ctx, records)
	for i, rec := range recsOut {
		singleRec, ok := rec.(SingleRecord)
		if !ok {
			continue
		}
		if err := p.encodeKey(ctx, (*opencdc.Record)(&singleRec)); err != nil {
			recsOut[i] = ErrorRecord{Error: err}
		}
		if err := p.encodePayload(ctx, (*opencdc.Record)(&singleRec)); err != nil {
			recsOut[i] = ErrorRecord{Error: err}
		}
		recsOut[i] = singleRec
	}
	return recsOut
}

func (p *processorWithSchemaEncode) encodeKey(ctx context.Context, rec *opencdc.Record) error {
	if p.keySubject == "" {
		return nil // key schema encoding is disabled
	}
	if _, ok := rec.Key.(opencdc.StructuredData); !ok {
		// log warning once, to avoid spamming the logs
		p.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record key is not structured, consider disabling the processor schema key encoding using "%s: false"`, configProcessorSchemaEncodeKeyEnabled)
		})
		return nil
	}

	if rec.Metadata == nil {
		// ensure we have a metadata value, to make it safe for retrieving and setting values
		rec.Metadata = opencdc.Metadata{}
	}
	sch, err := p.schemaForKey(ctx, *rec)
	if err != nil {
		return err // already wrapped
	}

	encoded, err := p.encodeWithSchema(sch, rec.Key)
	if err != nil {
		return fmt.Errorf("failed to encode key: %w", err)
	}

	rec.Key = opencdc.RawData(encoded)
	schema.AttachKeySchemaToRecord(*rec, sch)
	return nil
}

func (p *processorWithSchemaEncode) schemaForKey(ctx context.Context, rec opencdc.Record) (schema.Schema, error) {
	subject, err := rec.Metadata.GetKeySchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get key schema subject: %w", err)
	}

	version, err := rec.Metadata.GetKeySchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get key schema version: %w", err)
	}

	switch {
	case subject != "" && version > 0:
		// The connector has attached the schema subject and version, we can use
		// it to retrieve the schema from the schema service.
		sch, err := procschema.Get(ctx, subject, version)
		if err != nil {
			return schema.Schema{}, fmt.Errorf("failed to get schema for key: %w", err)
		}
		return sch, nil
	case subject != "" || version > 0:
		// The connector has attached either the schema subject or version, but
		// not both, this isn't valid.
		return schema.Schema{}, fmt.Errorf("found metadata fields %v=%v and %v=%v, expected key schema subject and version to be both set to valid values, this is a bug in the connector", opencdc.MetadataKeySchemaSubject, subject, opencdc.MetadataKeySchemaVersion, version)
	}

	// No schema subject or version is attached, we need to encode the schema.
	subject = p.keySubject
	if collection, err := rec.Metadata.GetCollection(); err == nil {
		subject = collection + "." + subject
	}

	sch, err := p.schemaForType(ctx, rec.Key, subject)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to encode schema for key: %w", err)
	}

	return sch, nil
}

func (p *processorWithSchemaEncode) encodePayload(ctx context.Context, rec *opencdc.Record) error {
	if p.payloadSubject == "" {
		return nil // payload schema encoding is disabled
	}
	_, beforeIsStructured := rec.Payload.Before.(opencdc.StructuredData)
	_, afterIsStructured := rec.Payload.After.(opencdc.StructuredData)
	if !beforeIsStructured && !afterIsStructured {
		// log warning once, to avoid spamming the logs
		p.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record payload is not structured, consider disabling the processor schema payload encoding using "%s: false"`, configProcessorSchemaEncodePayloadEnabled)
		})
		return nil
	}

	if rec.Metadata == nil {
		// ensure we have a metadata value, to make it safe for retrieving and setting values
		rec.Metadata = opencdc.Metadata{}
	}
	sch, err := p.schemaForPayload(ctx, *rec)
	if err != nil {
		return fmt.Errorf("failed to encode schema for payload: %w", err)
	}

	// encode both before and after with the encodeed schema
	if beforeIsStructured {
		encoded, err := p.encodeWithSchema(sch, rec.Payload.Before)
		if err != nil {
			return fmt.Errorf("failed to encode before payload: %w", err)
		}
		rec.Payload.Before = opencdc.RawData(encoded)
	}
	if afterIsStructured {
		encoded, err := p.encodeWithSchema(sch, rec.Payload.After)
		if err != nil {
			return fmt.Errorf("failed to encode after payload: %w", err)
		}
		rec.Payload.After = opencdc.RawData(encoded)
	}
	schema.AttachPayloadSchemaToRecord(*rec, sch)
	return nil
}

func (p *processorWithSchemaEncode) schemaForPayload(ctx context.Context, rec opencdc.Record) (schema.Schema, error) {
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get payload schema subject: %w", err)
	}

	version, err := rec.Metadata.GetPayloadSchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return schema.Schema{}, fmt.Errorf("failed to get payload schema version: %w", err)
	}

	switch {
	case subject != "" && version > 0:
		// The connector has attached the schema subject and version, we can use
		// it to retrieve the schema from the schema service.
		sch, err := procschema.Get(ctx, subject, version)
		if err != nil {
			return schema.Schema{}, fmt.Errorf("failed to get schema for payload: %w", err)
		}
		return sch, nil
	case subject != "" || version > 0:
		// The connector has attached either the schema subject or version, but
		// not both, this isn't valid.
		return schema.Schema{}, fmt.Errorf("found metadata fields %v=%v and %v=%v, expected payload schema subject and version to be both set to valid values, this is a bug in the connector", opencdc.MetadataPayloadSchemaSubject, subject, opencdc.MetadataPayloadSchemaVersion, version)
	}

	// No schema subject or version is attached, we need to encode the schema.
	subject = p.payloadSubject
	if collection, err := rec.Metadata.GetCollection(); err == nil {
		subject = collection + "." + subject
	}

	val := rec.Payload.After
	if _, ok := val.(opencdc.StructuredData); !ok {
		// use before as a fallback
		val = rec.Payload.Before
	}

	sch, err := p.schemaForType(ctx, val, subject)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to encode schema for payload: %w", err)
	}

	return sch, nil
}

func (p *processorWithSchemaEncode) schemaForType(ctx context.Context, data any, subject string) (schema.Schema, error) {
	srd, err := schema.KnownSerdeFactories[p.schemaType].SerdeForType(data)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to create schema for value: %w", err)
	}

	sch, err := procschema.Create(ctx, p.schemaType, subject, []byte(srd.String()))
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to create schema: %w", err)
	}

	return sch, nil
}

func (p *processorWithSchemaEncode) encodeWithSchema(sch schema.Schema, data any) ([]byte, error) {
	srd, err := sch.Serde()
	if err != nil {
		return nil, fmt.Errorf("failed to get serde for schema: %w", err)
	}

	encoded, err := srd.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data with schema: %w", err)
	}

	return encoded, nil
}
