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
	cschema "github.com/conduitio/conduit-commons/schema"
)

type CreateRequest struct {
	Subject string
	Type    cschema.Type
	Bytes   []byte
}
type CreateResponse struct {
	cschema.Schema
}

type GetRequest struct {
	Subject string
	Version int
}
type GetResponse struct {
	cschema.Schema
}
