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
	"fmt"

	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit-processor-sdk/internal"
	"github.com/goccy/go-json"
)

// Run is the 'entry point' for a processor. It runs a
// 'get a command, send a reply' loop through which it
// communicates with Conduit.
//
// A processor plugin needs to call this function in its main() function.
func Run(p sdk.ProcessorPlugin) {
	for {
		cmd, err := internal.NextCommand()
		if err != nil {
			fmt.Printf("failed retrieving next command: %v", cmd)
			return
		}

		if cmd.Name == "specify" {
			fmt.Println("getting specification")
			spec := p.Specification()

			bytes, err := json.Marshal(spec)
			if err != nil {
				fmt.Printf("failed serializing specification: %v", err)
			}
			internal.Reply(bytes)
		} else {
			fmt.Printf("got unknown command: %v\n", cmd.Name)
		}
	}
}
