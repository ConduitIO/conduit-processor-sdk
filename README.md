# Conduit Processor SDK

[![License](https://img.shields.io/badge/license-Apache%202-blue)](https://github.com/ConduitIO/conduit-processor-sdk/blob/main/LICENSE.md)
[![Test](https://github.com/ConduitIO/conduit-processor-sdk/actions/workflows/test.yml/badge.svg)](https://github.com/ConduitIO/conduit-processor-sdk/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/conduitio/conduit-processor-sdk)](https://goreportcard.com/report/github.com/conduitio/conduit-processor-sdk)
[![Go Reference](https://pkg.go.dev/badge/github.com/conduitio/conduit-processor-sdk.svg)](https://pkg.go.dev/github.com/conduitio/conduit-processor-sdk)

This repository contains the Go software development kit for implementing a
processor for [Conduit](https://github.com/conduitio/conduit).

## Get started

Create a new folder and initialize a fresh go module:

```sh
go mod init example.com/conduit-processor-demo
```

Add the processor SDK dependency:

```sh
go get github.com/conduitio/conduit-processor-sdk
```

You can now create a new processor by implementing the
[`Processor`](https://pkg.go.dev/github.com/conduitio/conduit-processor-sdk#Processor)
interface. Here is an example of a simple processor that adds a field to the
record:

```go
package example

import (
	"context"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

type AddFieldProcessor struct {
	sdk.UnimplementedProcessor
	Field string
	Value string
}

// Specification returns metadata about the processor.
func (p *AddFieldProcessor) Specification(context.Context) (sdk.Specification, error) {
	return sdk.Specification{
		Name:    "myAddFieldProcessor",
		Summary: "Add a field to the record.",
		Description: `This processor lets you configure a static field that will
be added to the record into field .Payload.After. If the payload is not
structured data, this processor will panic.`,
		Version: "v1.0.0",
		Author:  "John Doe",
		Parameters: map[string]config.Parameter{
			"field": {Type: config.ParameterTypeString, Description: "The name of the field to add"},
			"name":  {Type: config.ParameterTypeString, Description: "The value of the field to add"},
		},
	}, nil
}

// Configure is called by Conduit to configure the processor. It receives a map
// with the parameters defined in the Specification method.
func (p *AddFieldProcessor) Configure(ctx context.Context, config map[string]string) error {
	p.Field = config["field"]
	p.Value = config["value"]
	return nil
}

// Process is called by Conduit to process records. It receives a slice of
// records and should return a slice of processed records.
func (p *AddFieldProcessor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	out := make([]sdk.ProcessedRecord, 0, len(records))
	for _, record := range records {
		record.Payload.After.(opencdc.StructuredData)[p.Field] = p.Value
		out = append(out, sdk.SingleRecord(record))
	}
	return out
}
```

You also need to add an entrypoint to your processor, since it will be run as a
standalone WASM plugin:

```go
//go:build wasm

package main

import (
	example "example.com/add-field-processor"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

func main() {
	sdk.Run(&example.AddFieldProcessor{})
}
```

If the processor is very simple and can be reduced to a single function (e.g.
no configuration needed), then we can use `sdk.NewProcessorFunc()`, as below:

```go
//go:build wasm

package main

import (
   sdk "github.com/conduitio/conduit-processor-sdk"
)

func main() {
   sdk.Run(sdk.NewProcessorFunc(
      sdk.Specification{Name: "simple-processor"},
      func(ctx context.Context, rec opencdc.Record) (opencdc.Record, error) {
         // do something with the record
         return rec, nil
      },
   ))
}
```

With this you are set to build your processor. Note that we are building the
processor as a WebAssembly module, so you need to set `GOARCH` and `GOOS`:

```sh
GOARCH=wasm GOOS=wasip1 go build -o add-field-processor.wasm cmd/main.go
```

The produced `add-field-processor.wasm` file can be used as a processor in
Conduit. Copy it to the `processors` directory of your Conduit instance and
configure it in the `processors` section of the pipeline configuration file.

## FAQ

### Why do I need to specify `GOARCH` and `GOOS`?

Conduit uses [WebAssembly](https://webassembly.org) to run standalone processors.
This means that you need to build your processor as a WebAssembly module. You can
do this by setting the environment variables `GOARCH=wasm` and `GOOS=wasip1` when
running `go build`. This will produce a WebAssembly module that can be used as a
processor in Conduit.

### How do I use a processor?

To use a standalone WASM processor in Conduit, the following two steps need to be
done:

1. Copying the WebAssembly module to the processors directory of your Conduit
   instance. By default, that's a directory called `processors` that is in the same
   directory as Conduit. The directory can be changed with the `processors.path` flag.

   An example directory structure would be:
   ```shell
   tree .
   .
   ├── conduit
   └── processors
       └── add-field-processor.wasm
   ```
2. Use the processor in the `processors` section of the pipeline configuration file.
   using the name the processor defines in its specifications. An example configuration
   for the processor above would be:
   ```yaml
    processors:
     - id: add-foo-field
       plugin: myAddFieldProcessor
       settings:
         field: 'foo'
         value: 'bar'
   ```

### How do I log from a processor?

You can get a `zerolog.logger` instance from the context using the
[`sdk.Logger`](https://pkg.go.dev/github.com/conduitio/conduit-processor-sdk#Logger)
function. This logger is pre-configured to append logs in the format expected by
Conduit.

Keep in mind that logging in the hot path (i.e. in the `Process` method) can have
a significant impact on the performance of your processor, therefore we recommend
to use the `Trace` level for logs that are not essential for the operation of the
processor.

Example:

```go
func (p *AddFieldProcessor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
logger := sdk.Logger(ctx)
logger.Trace().Msg("Processing records")
// ...
}
```
