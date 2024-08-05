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
	"fmt"
	"sync"

	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-processor-sdk/pprocutils"
)

type InMemoryService struct {
	// schemas is a map of schema subjects to all the versions of that schema
	// versioning starts at 1, newer versions are appended to the end of the versions slice.
	schemas map[string][]schema.Schema
	// m guards access to schemas
	m sync.Mutex
	// idSequence is used to generate unique schema IDs
	idSequence int
}

func NewInMemoryService() *InMemoryService {
	return &InMemoryService{
		schemas: make(map[string][]schema.Schema),
	}
}

func (s *InMemoryService) CreateSchema(_ context.Context, request pprocutils.CreateSchemaRequest) (pprocutils.CreateSchemaResponse, error) {
	if request.Type != schema.TypeAvro {
		return pprocutils.CreateSchemaResponse{}, pprocutils.ErrInvalidSchema
	}

	s.m.Lock()
	defer s.m.Unlock()

	inst := schema.Schema{
		ID:      s.nextID(),
		Subject: request.Subject,
		Version: len(s.schemas[request.Subject]) + 1,
		Type:    request.Type,
		Bytes:   request.Bytes,
	}
	s.schemas[request.Subject] = append(s.schemas[request.Subject], inst)

	return pprocutils.CreateSchemaResponse{Schema: inst}, nil
}

func (s *InMemoryService) GetSchema(_ context.Context, request pprocutils.GetSchemaRequest) (pprocutils.GetSchemaResponse, error) {
	s.m.Lock()
	defer s.m.Unlock()

	versions, ok := s.schemas[request.Subject]
	if !ok {
		return pprocutils.GetSchemaResponse{}, fmt.Errorf("subject %v: %w", request.Subject, pprocutils.ErrSubjectNotFound)
	}

	if len(versions) < request.Version {
		return pprocutils.GetSchemaResponse{}, fmt.Errorf("version %v: %w", request.Version, pprocutils.ErrVersionNotFound)
	}

	return pprocutils.GetSchemaResponse{Schema: versions[request.Version-1]}, nil
}

func (s *InMemoryService) nextID() int {
	s.idSequence++
	return s.idSequence
}
