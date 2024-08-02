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
	procschema "github.com/conduitio/conduit-processor-sdk/schema"
)

// ProcessorMiddlewareOption is a function that can be used to configure a
// ProcessorMiddleware.
type ProcessorMiddlewareOption interface {
	Apply(ProcessorMiddleware)
}

// ProcessorMiddleware wraps a Processor and adds functionality to it.
type ProcessorMiddleware interface {
	Wrap(Processor) Processor
}

// DefaultProcessorMiddleware returns a slice of middleware that is added to all
// processors by default.
func DefaultProcessorMiddleware(opts ...ProcessorMiddlewareOption) []ProcessorMiddleware {
	middleware := []ProcessorMiddleware{
		// TODO add default middleware
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

func (d *processorWithSchemaDecode) Specification() (Specification, error) {
	spec, err := d.Processor.Specification()
	if err != nil {
		return spec, err
	}

	// merge parameters from the processor and the schema decode middleware
	spec.Parameters = mergeParameters(spec.Parameters, d.defaults.parameters())
	return spec, nil
}

func (d *processorWithSchemaDecode) Configure(ctx context.Context, config config.Config) error {
	err := d.Processor.Configure(ctx, config)
	if err != nil {
		return err
	}

	d.keyEnabled = *d.defaults.KeyEnabled
	if val, ok := config[configProcessorWithSchemaDecodeKeyEnabled]; ok {
		d.keyEnabled, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorWithSchemaDecodeKeyEnabled, err)
		}
	}

	d.payloadEnabled = *d.defaults.PayloadEnabled
	if val, ok := config[configProcessorWithSchemaDecodePayloadEnabled]; ok {
		d.payloadEnabled, err = strconv.ParseBool(val)
		if err != nil {
			return fmt.Errorf("invalid %s: failed to parse boolean: %w", configProcessorWithSchemaDecodePayloadEnabled, err)
		}
	}

	return nil
}

func (d *processorWithSchemaDecode) Process(ctx context.Context, records []opencdc.Record) []ProcessedRecord {
	if d.keyEnabled {
		for i := range records {
			if err := d.decodeKey(ctx, &records[i]); err != nil {
				if len(records) > 0 {
					err = fmt.Errorf("record %d: %w", i, err)
				}
				return []ProcessedRecord{ErrorRecord{Error: err}}
			}
		}
	}
	if d.payloadEnabled {
		for i := range records {
			if err := d.decodePayload(ctx, &records[i]); err != nil {
				if len(records) > 0 {
					err = fmt.Errorf("record %d: %w", i, err)
				}
				return []ProcessedRecord{ErrorRecord{Error: err}}
			}
		}
	}

	return d.Processor.Process(ctx, records)
}

func (d *processorWithSchemaDecode) decodeKey(ctx context.Context, rec *opencdc.Record) error {
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
		d.keyWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record does not have an attached schema for the key, consider disabling the processor schema key decoding using "%s: false"`, configProcessorWithSchemaDecodeKeyEnabled)
		})
		return nil
	}

	if rec.Key != nil {
		decodedKey, err := d.decode(ctx, rec.Key, subject, version)
		if err != nil {
			return fmt.Errorf("failed to decode key: %w", err)
		}
		rec.Key = decodedKey
	}

	return nil
}

func (d *processorWithSchemaDecode) decodePayload(ctx context.Context, rec *opencdc.Record) error {
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
		d.payloadWarnOnce.Do(func() {
			Logger(ctx).Warn().Msgf(`record does not have an attached schema for the payload, consider disabling the processor schema payload decoding using "%s: false"`, configProcessorWithSchemaDecodePayloadEnabled)
		})
		return nil
	}

	if rec.Payload.Before != nil {
		decodedPayloadBefore, err := d.decode(ctx, rec.Payload.Before, subject, version)
		if err != nil {
			return fmt.Errorf("failed to decode payload.before: %w", err)
		}
		rec.Payload.Before = decodedPayloadBefore
	}
	if rec.Payload.After != nil {
		decodedPayloadAfter, err := d.decode(ctx, rec.Payload.After, subject, version)
		if err != nil {
			return fmt.Errorf("failed to decode payload.after: %w", err)
		}
		rec.Payload.After = decodedPayloadAfter
	}

	return nil
}

func (d *processorWithSchemaDecode) decode(ctx context.Context, data opencdc.Data, subject string, version int) (opencdc.StructuredData, error) {
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
