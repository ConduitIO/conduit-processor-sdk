// Copyright © 2023 Meroxa, Inc.
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

package serve

import (
	"context"
	"fmt"
	"github.com/goccy/go-json"

	"github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit-processor-sdk/internal"
)

func Serve(p sdk.ProcessorPlugin) {
	for {
		cmd, err := internal.NextCommand()
		if err != nil {
			fmt.Printf("failed retrieving next command: %v", cmd)
			return
		}

		fmt.Printf("executing command %v with processor %v\n", cmd, p)
		resp := cmd.Execute(context.Background(), p)
		bytes, err := json.Marshal(resp)
		if err != nil {
			fmt.Printf("failed marshalling response to command: %w\n", err)
		}

		fmt.Printf("writing reply, %v bytes\n", len(bytes))
		ptr, cleanup := internal.Write(bytes)
		internal.Reply(ptr, len(bytes))
		cleanup()
	}
}
