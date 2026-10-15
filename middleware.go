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

// ProcessorWithSchemaEncode is a middleware that encodes the structured record
// payload and/or key returned by the processor with a schema, and attaches that
// schema to the record. Which schema it uses depends on the schema subject and
// version in the metadata of the processed record:
//
//   - Not set: the data is left structured and a warning is logged once.
//   - Changed by the processor: the schema registered under the new subject and
//     version is used. If the data does not fit it, the record fails.
//   - Unchanged: the schema the record came in with is used, as long as the
//     data still fits it exactly, meaning decoding the encoded bytes gives back
//     the same values (no dropped fields, no truncated times or numbers). Data a
//     processor returns unchanged always fits, so it keeps its exact bytes and
//     schema. If the data no longer fits, for example because the processor
//     added a field, a schema is extracted from the values, registered as a new
//     version of the same subject, and a warning is logged once. An extracted
//     schema does not keep Avro logical types, enums or named types, and widens
//     int to long, so processors that change the shape of their data and care
//     about exact types should attach the intended schema themselves.
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

	payloadMismatchWarnOnce sync.Once
	keyMismatchWarnOnce     sync.Once
}

// schemaMismatchWarning is logged once per middleware instance and data kind
// (key or payload) when a processor kept the schema subject and version but
// returned data that no longer fits that schema.
const schemaMismatchWarning = "processed record %s no longer fits its schema %s:%d, so a schema is extracted from the values " +
	"and registered as a new version; extracted schemas do not keep Avro logical types, enums, named types or int vs long. " +
	"To keep exact types, attach the intended schema to the record in the processor"

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
	for i, processedRec := range recsOut {
		if i >= len(keySubjectVersions) {
			break // should not happen if the processor respects the input length
		}

		switch processedRec := processedRec.(type) {
		case SingleRecord:
			encodedRecord, err := p.encodeRecord(ctx, opencdc.Record(processedRec), keySubjectVersions[i], payloadSubjectVersions[i])
			if err != nil {
				recsOut[i] = ErrorRecord{Error: err}
				continue
			}
			recsOut[i] = SingleRecord(encodedRecord)
		case MultiRecord:
			for j, rec := range processedRec {
				encodedRecord, err := p.encodeRecord(ctx, rec, keySubjectVersions[i], payloadSubjectVersions[i])
				if err != nil {
					recsOut[i] = ErrorRecord{Error: err}
					break
				}
				processedRec[j] = encodedRecord
			}
		default:
			continue // skip if not a single or multi record
		}
	}
	return recsOut
}

func (p *processorWithSchemaEncode) encodeRecord(
	ctx context.Context,
	rec opencdc.Record,
	keySubjectVersion, payloadSubjectVersion subjectVersion,
) (opencdc.Record, error) {
	rec, err := p.encodeKey(ctx, rec, keySubjectVersion)
	if err != nil {
		return opencdc.Record{}, err
	}
	return p.encodePayload(ctx, rec, payloadSubjectVersion)
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

func (p *processorWithSchemaEncode) encodeKey(ctx context.Context, rec opencdc.Record, incomingSubjectVersion subjectVersion) (opencdc.Record, error) {
	if !p.keyEnabled {
		return rec, nil // key schema encoding is disabled
	}
	if _, ok := rec.Key.(opencdc.StructuredData); !ok {
		// log warning once, to avoid spamming the logs
		p.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record key is not structured, consider disabling the processor schema key encoding using "%s: false"`, configProcessorSchemaEncodeKeyEnabled)
		})
		return rec, nil
	}

	outgoingSubjectVersion, err := p.keySubjectVersion(rec)
	if err != nil {
		return opencdc.Record{}, err // already wrapped
	}

	var newSchema schema.Schema
	switch {
	case outgoingSubjectVersion == (subjectVersion{}):
		// key schema subject and version not found, skip encoding
		// log warning once, to avoid spamming the logs
		p.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`outgoing record does not have an attached schema for the key, consider disabling the processor schema key encoding using "%s: false"`, configProcessorSchemaEncodeKeyEnabled)
		})
		return rec, nil
	case incomingSubjectVersion == outgoingSubjectVersion:
		// key schema subject and version didn't change
		oldSchema, err := sdkschema.Get(ctx, incomingSubjectVersion.subject, incomingSubjectVersion.version)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to get schema for key: %w", err)
		}

		// Invariant 6: encode with the schema the record came in with whenever
		// the key still fits it losslessly. A schema extracted from the decoded
		// values loses types (date becomes timestamp-micros, enum becomes
		// string, int becomes long, ...).
		encoded, err := encodeLossless(oldSchema, rec.Key.(opencdc.StructuredData)) //nolint:forcetypeassert // checked above
		switch {
		case err == nil:
			rec.Key = opencdc.RawData(encoded)
			schema.AttachKeySchemaToRecord(rec, oldSchema)
			return rec, nil
		case !errors.Is(err, errSchemaMismatch):
			return opencdc.Record{}, fmt.Errorf("failed to encode key: %w", err)
		}

		// The processor changed the key so that it no longer fits the schema
		// (for example it added a field). Extract a new schema from the values
		// and register it as a new version of the subject.
		p.keyMismatchWarnOnce.Do(func() {
			Logger(ctx).Warn().Err(err).Msgf(schemaMismatchWarning, "key", oldSchema.Subject, oldSchema.Version)
		})
		newSchema, err = p.schemaForType(ctx, oldSchema.Type, outgoingSubjectVersion.subject, rec.Key)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to extract schema for key: %w", err)
		}
	default:
		// key schema subject or version changed, fetch the schema and encode the data
		newSchema, err = sdkschema.Get(ctx, outgoingSubjectVersion.subject, outgoingSubjectVersion.version)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to get schema for key: %w", err)
		}
	}

	encoded, err := p.encodeWithSchema(newSchema, rec.Key)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to encode key: %w", err)
	}

	rec.Key = opencdc.RawData(encoded)
	schema.AttachKeySchemaToRecord(rec, newSchema)
	return rec, nil
}

//nolint:funlen // maybe refactor in the future
func (p *processorWithSchemaEncode) encodePayload(ctx context.Context, rec opencdc.Record, incomingSubjectVersion subjectVersion) (opencdc.Record, error) {
	if !p.payloadEnabled {
		return rec, nil // payload schema encoding is disabled
	}
	_, beforeIsStructured := rec.Payload.Before.(opencdc.StructuredData)
	_, afterIsStructured := rec.Payload.After.(opencdc.StructuredData)
	if !beforeIsStructured && !afterIsStructured {
		// log warning once, to avoid spamming the logs
		p.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record payload is not structured, consider disabling the processor schema payload encoding using "%s: false"`, configProcessorSchemaEncodePayloadEnabled)
		})
		return rec, nil
	}

	outgoingSubjectVersion, err := p.payloadSubjectVersion(rec)
	if err != nil {
		return opencdc.Record{}, err // already wrapped
	}

	var newSchema schema.Schema
	switch {
	case outgoingSubjectVersion == (subjectVersion{}):
		// payload schema subject and version not found, skip encoding
		// log warning once, to avoid spamming the logs
		p.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`outgoing record does not have an attached schema for the payload, consider disabling the processor schema payload encoding using "%s: false"`, configProcessorSchemaEncodePayloadEnabled)
		})
		return rec, nil
	case incomingSubjectVersion == outgoingSubjectVersion:
		// payload schema subject and version didn't change
		oldSchema, err := sdkschema.Get(ctx, incomingSubjectVersion.subject, incomingSubjectVersion.version)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to get schema for payload: %w", err)
		}

		// Invariant 6: encode with the schema the record came in with whenever
		// both before and after still fit it losslessly. A schema extracted
		// from the decoded values loses types (date becomes timestamp-micros,
		// enum becomes string, int becomes long, ...).
		fitted, err := encodePayloadLossless(oldSchema, rec.Payload)
		switch {
		case err == nil:
			rec.Payload = fitted
			schema.AttachPayloadSchemaToRecord(rec, oldSchema)
			return rec, nil
		case !errors.Is(err, errSchemaMismatch):
			return opencdc.Record{}, err // already wrapped
		}

		// The processor changed the payload so that it no longer fits the
		// schema (for example it added a field). Extract a new schema from the
		// values and register it as a new version of the subject.
		p.payloadMismatchWarnOnce.Do(func() {
			Logger(ctx).Warn().Err(err).Msgf(schemaMismatchWarning, "payload", oldSchema.Subject, oldSchema.Version)
		})
		val := rec.Payload.After
		if _, ok := val.(opencdc.StructuredData); !ok {
			// use before as a fallback
			val = rec.Payload.Before
		}

		newSchema, err = p.schemaForType(ctx, oldSchema.Type, outgoingSubjectVersion.subject, val)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to extract schema for payload: %w", err)
		}
	default:
		// payload schema subject or version changed, fetch the schema and encode the data
		newSchema, err = sdkschema.Get(ctx, outgoingSubjectVersion.subject, outgoingSubjectVersion.version)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to get schema for payload: %w", err)
		}
	}

	// encode both before and after with the extracted schema
	if beforeIsStructured {
		encoded, err := p.encodeWithSchema(newSchema, rec.Payload.Before)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to encode before payload: %w", err)
		}
		rec.Payload.Before = opencdc.RawData(encoded)
	}
	if afterIsStructured {
		encoded, err := p.encodeWithSchema(newSchema, rec.Payload.After)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to encode after payload: %w", err)
		}
		rec.Payload.After = opencdc.RawData(encoded)
	}
	schema.AttachPayloadSchemaToRecord(rec, newSchema)
	return rec, nil
}

// encodePayloadLossless encodes the structured parts of payload with sch, if
// all of them fit it exactly (see encodeLossless). It returns the payload with
// the encoded bytes, or an error wrapping errSchemaMismatch if any part does
// not fit. The input payload is not modified.
func encodePayloadLossless(sch schema.Schema, payload opencdc.Change) (opencdc.Change, error) {
	if before, ok := payload.Before.(opencdc.StructuredData); ok {
		encoded, err := encodeLossless(sch, before)
		if err != nil {
			return opencdc.Change{}, fmt.Errorf("failed to encode before payload: %w", err)
		}
		payload.Before = opencdc.RawData(encoded)
	}
	if after, ok := payload.After.(opencdc.StructuredData); ok {
		encoded, err := encodeLossless(sch, after)
		if err != nil {
			return opencdc.Change{}, fmt.Errorf("failed to encode after payload: %w", err)
		}
		payload.After = opencdc.RawData(encoded)
	}
	return payload, nil
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
