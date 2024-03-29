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

import "errors"

var (
	// ErrUnimplemented is returned in functions of plugins that don't implement
	// a certain method.
	ErrUnimplemented = errors.New("the processor plugin does not implement " +
		"this action, please check the source code of the processor and make sure " +
		"all required processor methods are implemented")

	ErrFilterRecord = errors.New("filter out this record")
)
