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

package schema

import (
	"context"
	"errors"
	"testing"

	cschema "github.com/conduitio/conduit-commons/schema"
	"github.com/matryer/is"
)

func TestSchema_CreateSuccess(t *testing.T) {
	is := is.New(t)
	want := cschema.Schema{
		Subject: "test1",
		Version: 1,
		Type:    TypeAvro,
		Bytes:   []byte("schema"),
	}
	got, err := Create(context.Background(), want.Type, want.Subject, want.Bytes)
	is.NoErr(err)
	is.Equal(want, got)
}

func TestSchema_CreateFail(t *testing.T) {
	is := is.New(t)
	// invalid schema type: 5 is not a schema type
	got, err := Create(context.Background(), 5, "invalid", []byte("schema"))
	is.True(errors.Is(err, ErrInvalidSchema))
	is.Equal(got, cschema.Schema{})
}

func TestSchema_GetSuccess(t *testing.T) {
	is := is.New(t)
	want := cschema.Schema{
		Subject: "test2",
		Version: 1,
		Type:    TypeAvro,
		Bytes:   []byte("schema"),
	}
	got, err := Create(context.Background(), want.Type, want.Subject, want.Bytes)
	is.NoErr(err)
	is.Equal(want, got)
	resp, err := Get(context.Background(), want.Subject, want.Version)
	is.NoErr(err)
	is.Equal(resp, want)
}

func TestSchema_GetInvalidSubject(t *testing.T) {
	is := is.New(t)
	want := cschema.Schema{
		Subject: "test3",
		Version: 1,
		Type:    TypeAvro,
		Bytes:   []byte("schema"),
	}
	got, err := Create(context.Background(), want.Type, want.Subject, want.Bytes)
	is.NoErr(err)
	is.Equal(want, got)
	resp, err := Get(context.Background(), "invalid", want.Version)
	is.True(errors.Is(err, ErrSubjectNotFound))
	is.Equal(resp, cschema.Schema{})
}

func TestSchema_GetInvalidVersion(t *testing.T) {
	is := is.New(t)
	want := cschema.Schema{
		Subject: "test4",
		Version: 1,
		Type:    TypeAvro,
		Bytes:   []byte("schema"),
	}
	got, err := Create(context.Background(), want.Type, want.Subject, want.Bytes)
	is.NoErr(err)
	is.Equal(want, got)
	// version 5 doesn't exist
	resp, err := Get(context.Background(), want.Subject, 5)
	is.True(errors.Is(err, ErrVersionNotFound))
	is.Equal(resp, cschema.Schema{})
}
