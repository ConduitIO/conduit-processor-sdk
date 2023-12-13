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
	"os"

	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit-processor-sdk/internal"
)

// Serve is the 'entry point' for a processor. It runs a
// 'get a command, send a reply' loop through which it
// communicates with Conduit.
//
// A processor plugin needs to call this function in its main() function.
func Serve(p sdk.ProcessorPlugin) {
	for {
		cmd, err := internal.NextCommand()
		if err != nil {
			fmt.Printf("failed retrieving next command: %v", err)
			os.Exit(1)
		}

		resp := cmd.Execute(context.Background(), p)
		err = internal.Reply(resp)
		if err != nil {
			fmt.Printf("failed writing reply: %v\n", err)
			os.Exit(1)
		}
	}
}
