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

package run

import (
	"context"
	"errors"
	"fmt"
	"os"

	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/conduitio/conduit-processor-sdk/internal/wasm"
)

// Run is the 'entry point' for a processor. It runs a
// 'get a command, send a reply' loop through which it
// communicates with Conduit.
//
// A processor plugin needs to call this function in its main() function.
func Run(p sdk.Processor) {
	for {
		cmd, err := wasm.NextCommand()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed retrieving next command: %v", err)
			exitCode := 1
			if errors.Is(err, sdk.ErrNoMoreCommands) {
				exitCode = 0
			}
			os.Exit(exitCode)
		}

		resp := cmd.Execute(context.Background(), p)
		err = wasm.Reply(resp)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed writing reply: %v\n", err)
			os.Exit(1)
		}
	}
}
