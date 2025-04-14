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
	sdkschema "github.com/conduitio/conduit-processor-sdk/schema"
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
	// Whether to decode the record payload using its corresponding schema from the schema registry.
	// If unset, defaults to true.
	PayloadEnabled *bool
	// Whether to decode the record key using its corresponding schema from the schema registry.
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
			Description: "Whether to decode the record key using its corresponding schema from the schema registry.",
		},
		configProcessorWithSchemaDecodePayloadEnabled: {
			Default:     strconv.FormatBool(*c.PayloadEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to decode the record payload using its corresponding schema from the schema registry.",
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
//   - `sdk.schema.decode.key.enabled` - Whether to decode the record key using its
//     corresponding schema from the schema registry.
//   - `sdk.schema.decode.payload.enabled` - Whether to decode the record payload
//     using its corresponding schema from the schema registry.
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
	var err error
	p.keyEnabled = *p.defaults.KeyEnabled
	if val, ok := config[configProcessorWithSchemaDecodeKeyEnabled]; ok {
		p.keyEnabled, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorWithSchemaDecodeKeyEnabled, err)
		}
		delete(config, configProcessorWithSchemaDecodeKeyEnabled)
	}

	p.payloadEnabled = *p.defaults.PayloadEnabled
	if val, ok := config[configProcessorWithSchemaDecodePayloadEnabled]; ok {
		p.payloadEnabled, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorWithSchemaDecodePayloadEnabled, err)
		}
		delete(config, configProcessorWithSchemaDecodePayloadEnabled)
	}

	//nolint:wrapcheck // middleware shouldn't wrap errors
	return p.Processor.Configure(ctx, config)
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

	sch, err := sdkschema.Get(ctx, subject, version)
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
	configProcessorSchemaEncodePayloadEnabled = "sdk.schema.encode.payload.enabled"
	configProcessorSchemaEncodeKeyEnabled     = "sdk.schema.encode.key.enabled"
)

// ProcessorWithSchemaEncodeConfig is the configuration for the
// ProcessorWithSchemaEncode middleware. Fields set to their zero value are
// ignored and will be set to the default value.
//
// ProcessorWithSchemaEncodeConfig can be used as a ProcessorMiddlewareOption.
type ProcessorWithSchemaEncodeConfig struct {
	// Whether to encode the record payload using its corresponding schema from the schema registry.
	// If unset, defaults to true.
	PayloadEnabled *bool
	// Whether to encode the record key using its corresponding schema from the schema registry.
	// If unset, defaults to true.
	KeyEnabled *bool
}

// Apply sets the default configuration for the ProcessorWithSchemaEncode middleware.
func (c ProcessorWithSchemaEncodeConfig) Apply(m ProcessorMiddleware) {
	if s, ok := m.(*ProcessorWithSchemaEncode); ok {
		s.Config = c
	}
}

func (c ProcessorWithSchemaEncodeConfig) SchemaPayloadEnabledParameterName() string {
	return configProcessorSchemaEncodePayloadEnabled
}

func (c ProcessorWithSchemaEncodeConfig) SchemaKeyEnabledParameterName() string {
	return configProcessorSchemaEncodeKeyEnabled
}

func (c ProcessorWithSchemaEncodeConfig) parameters() config.Parameters {
	return config.Parameters{
		configProcessorSchemaEncodePayloadEnabled: {
			Default:     strconv.FormatBool(*c.PayloadEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to encode the record payload using its corresponding schema from the schema registry.",
		},
		configProcessorSchemaEncodeKeyEnabled: {
			Default:     strconv.FormatBool(*c.KeyEnabled),
			Type:        config.ParameterTypeBool,
			Description: "Whether to encode the record key using its corresponding schema from the schema registry.",
		},
	}
}

// ProcessorWithSchemaEncode is a middleware that encodes the record
// payload and/or key with a schema. It only encodes the record key/payload if the
// schema subject and version in the record metadata match the schema subject and
// version of the incoming record. If the schema subject and version is not found
// in the record metadata or it was changed by the processor, it will log a
// warning and skip the encoding.
type ProcessorWithSchemaEncode struct {
	Config ProcessorWithSchemaEncodeConfig
}

// Wrap a Processor into the schema middleware. It will apply default configuration
// values if they are not explicitly set.
func (p *ProcessorWithSchemaEncode) Wrap(impl Processor) Processor {
	if p.Config.KeyEnabled == nil {
		p.Config.KeyEnabled = lang.Ptr(true)
	}
	if p.Config.PayloadEnabled == nil {
		p.Config.PayloadEnabled = lang.Ptr(true)
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

	keyEnabled     bool
	payloadEnabled bool

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
	var err error
	encodeKey := *p.defaults.KeyEnabled
	if val, ok := config[configProcessorSchemaEncodeKeyEnabled]; ok {
		encodeKey, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorSchemaEncodeKeyEnabled, err)
		}
		delete(config, configProcessorSchemaEncodeKeyEnabled)
	}

	encodePayload := *p.defaults.PayloadEnabled
	if val, ok := config[configProcessorSchemaEncodePayloadEnabled]; ok {
		encodePayload, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorSchemaEncodePayloadEnabled, err)
		}
		delete(config, configProcessorSchemaEncodePayloadEnabled)
	}

	p.keyEnabled = encodeKey
	p.payloadEnabled = encodePayload

	//nolint:wrapcheck // middleware shouldn't wrap errors
	return p.Processor.Configure(ctx, config)
}

type subjectVersion struct {
	subject string
	version int
}

func (p *processorWithSchemaEncode) Process(ctx context.Context, records []opencdc.Record) []ProcessedRecord {
	if !p.keyEnabled && !p.payloadEnabled {
		return p.Processor.Process(ctx, records) // middleware is fully disabled
	}

	// first store incoming subject and version values
	keySubjectVersions := make([]subjectVersion, len(records))
	payloadSubjectVersions := make([]subjectVersion, len(records))
	for i, rec := range records {
		sv, err := p.keySubjectVersion(rec)
		if err != nil {
			return []ProcessedRecord{ErrorRecord{Error: fmt.Errorf("record %d: %w", i, err)}}
		}
		keySubjectVersions[i] = sv

		sv, err = p.payloadSubjectVersion(rec)
		if err != nil {
			return []ProcessedRecord{ErrorRecord{Error: fmt.Errorf("record %d: %w", i, err)}}
		}
		payloadSubjectVersions[i] = sv
	}

	// process records
	recsOut := p.Processor.Process(ctx, records)

	// encode key and payload with schema if subject and version didn't change
	for i, rec := range recsOut {
		if i >= len(keySubjectVersions) {
			break // should not happen, if the processor respects the input length
		}

		singleRec, ok := rec.(SingleRecord)
		if !ok {
			continue
		}
		if err := p.encodeKey(ctx, (*opencdc.Record)(&singleRec), keySubjectVersions[i]); err != nil {
			recsOut[i] = ErrorRecord{Error: err}
		}
		if err := p.encodePayload(ctx, (*opencdc.Record)(&singleRec), payloadSubjectVersions[i]); err != nil {
			recsOut[i] = ErrorRecord{Error: err}
		}
		recsOut[i] = singleRec
	}
	return recsOut
}

func (p *processorWithSchemaEncode) keySubjectVersion(rec opencdc.Record) (subjectVersion, error) {
	subject, err := rec.Metadata.GetKeySchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return subjectVersion{}, fmt.Errorf("failed to get key schema subject: %w", err)
	}
	version, err := rec.Metadata.GetKeySchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return subjectVersion{}, fmt.Errorf("failed to get key schema version: %w", err)
	}
	return subjectVersion{subject: subject, version: version}, nil
}

func (p *processorWithSchemaEncode) payloadSubjectVersion(rec opencdc.Record) (subjectVersion, error) {
	subject, err := rec.Metadata.GetPayloadSchemaSubject()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return subjectVersion{}, fmt.Errorf("failed to get payload schema subject: %w", err)
	}
	version, err := rec.Metadata.GetPayloadSchemaVersion()
	if err != nil && !errors.Is(err, opencdc.ErrMetadataFieldNotFound) {
		return subjectVersion{}, fmt.Errorf("failed to get payload schema version: %w", err)
	}
	return subjectVersion{subject: subject, version: version}, nil
}

func (p *processorWithSchemaEncode) encodeKey(ctx context.Context, rec *opencdc.Record, incomingSubjectVersion subjectVersion) error {
	if !p.keyEnabled {
		return nil // key schema encoding is disabled
	}
	if _, ok := rec.Key.(opencdc.StructuredData); !ok {
		// log warning once, to avoid spamming the logs
		p.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record key is not structured, consider disabling the processor schema key encoding using "%s: false"`, configProcessorSchemaEncodeKeyEnabled)
		})
		return nil
	}

	outgoingSubjectVersion, err := p.keySubjectVersion(*rec)
	if err != nil {
		return err // already wrapped
	}

	var newSchema schema.Schema
	switch {
	case outgoingSubjectVersion == (subjectVersion{}):
		// key schema subject and version not found, skip encoding
		// log warning once, to avoid spamming the logs
		p.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`outgoing record does not have an attached schema for the key, consider disabling the processor schema key encoding using "%s: false"`, configProcessorSchemaEncodeKeyEnabled)
		})
		return nil
	case incomingSubjectVersion == outgoingSubjectVersion:
		// key schema subject and version didn't change, extract schema and encode the data

		// first fetch old schema to determine the schema type
		oldSchema, err := sdkschema.Get(ctx, incomingSubjectVersion.subject, incomingSubjectVersion.version)
		if err != nil {
			return fmt.Errorf("failed to get schema for key: %w", err)
		}

		// extract the schema, since the data might have changed
		newSchema, err = p.schemaForType(ctx, oldSchema.Type, outgoingSubjectVersion.subject, rec.Key)
		if err != nil {
			return fmt.Errorf("failed to extract schema for key: %w", err)
		}
	default:
		// key schema subject or version changed, fetch the schema and encode the data
		newSchema, err = sdkschema.Get(ctx, outgoingSubjectVersion.subject, outgoingSubjectVersion.version)
		if err != nil {
			return fmt.Errorf("failed to get schema for key: %w", err)
		}
	}

	encoded, err := p.encodeWithSchema(newSchema, rec.Key)
	if err != nil {
		return fmt.Errorf("failed to encode key: %w", err)
	}

	rec.Key = opencdc.RawData(encoded)
	schema.AttachKeySchemaToRecord(*rec, newSchema)
	return nil
}

//nolint:funlen // maybe refactor in the future
func (p *processorWithSchemaEncode) encodePayload(ctx context.Context, rec *opencdc.Record, incomingSubjectVersion subjectVersion) error {
	if !p.payloadEnabled {
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

	outgoingSubjectVersion, err := p.payloadSubjectVersion(*rec)
	if err != nil {
		return err // already wrapped
	}

	var newSchema schema.Schema
	switch {
	case outgoingSubjectVersion == (subjectVersion{}):
		// payload schema subject and version not found, skip encoding
		// log warning once, to avoid spamming the logs
		p.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`outgoing record does not have an attached schema for the payload, consider disabling the processor schema payload encoding using "%s: false"`, configProcessorSchemaEncodePayloadEnabled)
		})
		return nil
	case incomingSubjectVersion == outgoingSubjectVersion:
		// payload schema subject and version didn't change, extract schema and encode the data

		// first fetch old schema to determine the schema type
		oldSchema, err := sdkschema.Get(ctx, incomingSubjectVersion.subject, incomingSubjectVersion.version)
		if err != nil {
			return fmt.Errorf("failed to get schema for payload: %w", err)
		}

		// extract the schema, since the data might have changed
		val := rec.Payload.After
		if _, ok := val.(opencdc.StructuredData); !ok {
			// use before as a fallback
			val = rec.Payload.Before
		}

		newSchema, err = p.schemaForType(ctx, oldSchema.Type, outgoingSubjectVersion.subject, val)
		if err != nil {
			return fmt.Errorf("failed to extract schema for payload: %w", err)
		}
	default:
		// payload schema subject or version changed, fetch the schema and encode the data
		newSchema, err = sdkschema.Get(ctx, outgoingSubjectVersion.subject, outgoingSubjectVersion.version)
		if err != nil {
			return fmt.Errorf("failed to get schema for payload: %w", err)
		}
	}

	// encode both before and after with the extracted schema
	if beforeIsStructured {
		encoded, err := p.encodeWithSchema(newSchema, rec.Payload.Before)
		if err != nil {
			return fmt.Errorf("failed to encode before payload: %w", err)
		}
		rec.Payload.Before = opencdc.RawData(encoded)
	}
	if afterIsStructured {
		encoded, err := p.encodeWithSchema(newSchema, rec.Payload.After)
		if err != nil {
			return fmt.Errorf("failed to encode after payload: %w", err)
		}
		rec.Payload.After = opencdc.RawData(encoded)
	}
	schema.AttachPayloadSchemaToRecord(*rec, newSchema)
	return nil
}

func (p *processorWithSchemaEncode) schemaForType(ctx context.Context, schemaType schema.Type, subject string, data any) (schema.Schema, error) {
	srd, err := schema.KnownSerdeFactories[schemaType].SerdeForType(data)
	if err != nil {
		return schema.Schema{}, fmt.Errorf("failed to create schema for value: %w", err)
	}

	sch, err := sdkschema.Create(ctx, schemaType, subject, []byte(srd.String()))
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
