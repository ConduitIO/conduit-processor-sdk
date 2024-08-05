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
	"fmt"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-processor-sdk/internal/reference"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	"github.com/rs/zerolog"
)

// Logger returns the logger for the processor. Please provide the context that
// is passed to any of the processor's methods (Configure, Open, Process,
// Teardown) to ensure that the log messages include contextual information.
func Logger(ctx context.Context) *zerolog.Logger {
	l := pprocutils.Logger.With().Ctx(ctx).Logger()
	return &l
}

// Reference is an interface that represents a reference to a field in a record.
// It can be used to get and set the value of the field dynamically using input
// provided by the user.
type Reference reference.Reference

// ReferenceResolver is a type that knows how to resolve a reference to a field
// in a record. It is used to specify the target of a processor's output.
type ReferenceResolver reference.Resolver

// Resolve resolves the reference to a field in the record. If the reference
// cannot be resolved an error is returned. If the reference is valid but the
// field does not exist in the record, the field will be created.
// The returned reference can be used to set the value of the field.
func (r ReferenceResolver) Resolve(rec *opencdc.Record) (Reference, error) {
	ref, err := reference.Resolver(r).Resolve(rec)
	return Reference(ref), err
}

// NewReferenceResolver creates a new reference resolver from the input string.
// The input string is a reference to a field in a record. It can be a simple
// field name or a path to a nested field. The returned resolver can be used to
// resolve a reference to the specified field in a record and manipulate that
// field (get or set the value).
//
// Examples of valid references include:
//   - .Position
//   - .Operation
//   - .Key
//   - .Metadata.foo (to access a simple metadata value)
//   - .Metadata["f.o.123"] (to access a metadata value via a key containing non-alpha-numeric characters)
//   - .Payload.Before.foo (to access a nested field in payload before)
//   - .Payload.After["1"]["2"] (to access nested fields in payload after containing non-alpha-numeric characters)
func NewReferenceResolver(input string) (ReferenceResolver, error) {
	resolver, err := reference.NewResolver(input)
	return ReferenceResolver(resolver), err
}

// ParseConfig sanitizes the configuration, applies defaults, validates it and
// copies the values into the target object. It combines the functionality
// provided by github.com/conduitio/conduit-commons/config.Config into a single
// convenient function. It is intended to be used in the Configure method of a
// processor to parse the configuration map.
//
// The function does the following:
//   - Removes leading and trailing spaces from all keys and values in the
//     configuration.
//   - Applies the default values defined in the parameter specifications to the
//     configuration.
//   - Validates the configuration by checking for unrecognized parameters, type
//     validations, and value validations.
//   - Copies configuration values into the target object. The target object must
//     be a pointer to a struct.
func ParseConfig(
	ctx context.Context,
	cfg config.Config,
	target any,
	params config.Parameters,
) error {
	logger := Logger(ctx)

	logger.Debug().Msg("sanitizing configuration and applying defaults")
	c := cfg.Sanitize().ApplyDefaults(params)

	logger.Debug().Msg("validating configuration according to the specifications")
	err := c.Validate(params)
	if err != nil {
		return fmt.Errorf("config invalid: %w", err)
	}

	logger.Debug().Type("target", target).Msg("decoding configuration into the target object")
	//nolint:wrapcheck // error is already wrapped by DecodeInto
	return c.DecodeInto(target)
}

func mergeParameters(p1 config.Parameters, p2 config.Parameters) config.Parameters {
	params := make(config.Parameters, len(p1)+len(p2))
	for k, v := range p1 {
		params[k] = v
	}
	for k, v := range p2 {
		_, ok := params[k]
		if ok {
			panic(fmt.Errorf("parameter %q declared twice", k))
		}
		params[k] = v
	}
	return params
}
