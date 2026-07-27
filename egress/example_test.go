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

package egress_test

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-processor-sdk/egress"
)

// ExampleDo demonstrates that, outside a standalone (WebAssembly) processor,
// [egress.Do] refuses every call. There is no host boundary to broker the
// request through in this hosting mode, so the package defaults to deny-all
// rather than silently falling back to an unpolicied network call.
func ExampleDo() {
	_, err := egress.Do(context.Background(), egress.Request{
		Method: "GET",
		URL:    "https://api.example.com/v1/models",
	})
	fmt.Println(err)
	// Output:
	// error performing http egress request: http egress is not enabled for this processor
}
