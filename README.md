# Conduit Processor SDK

[![License](https://img.shields.io/badge/license-Apache%202-blue)](https://github.com/ConduitIO/conduit-processor-sdk/blob/main/LICENSE.md)
[![Test](https://github.com/ConduitIO/conduit-processor-sdk/actions/workflows/test.yml/badge.svg)](https://github.com/ConduitIO/conduit-processor-sdk/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/conduitio/conduit-processor-sdk)](https://goreportcard.com/report/github.com/conduitio/conduit-processor-sdk)
[![Go Reference](https://pkg.go.dev/badge/github.com/conduitio/conduit-processor-sdk.svg)](https://pkg.go.dev/github.com/conduitio/conduit-processor-sdk)

This repository contains the Go software development kit for implementing a
processor for [Conduit](https://github.com/conduitio/conduit).

Note: if you'd like to use another language for writing processors, feel free to
[open an issue](https://github.com/ConduitIO/conduit/issues/new?assignees=&labels=feature%2Ctriage&projects=&template=1-feature-request.yml&title=Feature%3A+%3Ctitle%3E) and request a processor SDK for a specific language.

## Quick Start

Create a new folder and initialize a fresh go module:

```sh
go mod init example.com/conduit-processor-example
```

Add the processor SDK dependency:

```sh
go get github.com/conduitio/conduit-processor-sdk
```

You can now create a new processor by implementing the
[`Processor`](https://pkg.go.dev/github.com/conduitio/conduit-processor-sdk#Processor)
interface. For more details about that, check our documentation for 
[Building Standalone Processors](https://conduit.io/docs/processors/standalone/building).

On the other hand, if the processor is very simple and can be reduced to a single function (e.g. 
no configuration needed), then we can use `sdk.NewProcessorFunc()`, as below:

```go
//go:build wasm

package main

import (
    sdk "github.com/conduitio/conduit-processor-sdk"
)

func main() {
    sdk.Run(sdk.NewProcessorFunc(
        sdk.Specification{Name: "example-processor"},
        func(ctx context.Context, rec opencdc.Record) (opencdc.Record, error) {
            // do something with the record
            return rec, nil
        },
    ))
}
```

With this, you are set to build your processor. Note that we are building the
processor as a WebAssembly module, so you need to set `GOARCH` and `GOOS`:

```sh
GOARCH=wasm GOOS=wasip1 go build -o example-processor.wasm main.go
```

The produced `example-processor.wasm` file can be used as a processor in
Conduit. Copy it to the `processors` directory of your Conduit instance and
configure it in the `processors` section of the pipeline configuration file.

## Schema encoding

Before `Process`, the SDK decodes a record's key and payload with the schema the
record carries. After `Process`, it encodes them again. Data that still fits the
carried schema is encoded with it. When a processor reshapes the data (for
example, it adds a field), the SDK has to register a new schema version under the
carried subject. Two parameters, present on every processor, choose how:

| Parameter | Values | Default |
| --- | --- | --- |
| `sdk.schema.encode.payload.strategy` | `infer`, `extend` | `infer` |
| `sdk.schema.encode.key.strategy` | `infer`, `extend` | `infer` |

- `infer` extracts a schema from the values. Avro `int` becomes `long`, `date`
  becomes `timestamp-micros`, decimals become `decimal(31,10)`, and enums and named
  types are lost.
- `extend` keeps the carried definition of every field still present, adds new
  top-level fields as nullable with a null default, and leaves out removed
  top-level fields. A nil or zero-length raw part is passed through unencoded,
  with its metadata unchanged. A part whose fields were all removed (`{}`) gets a
  record schema with zero fields, registered as a new version of the carried
  subject; it encodes to zero bytes, so downstream it is an empty value that
  still carries a schema ID. A value that does not fit its carried type fails the
  record (`ErrSchemaExtendMismatch`), and a carried schema that cannot be extended
  fails it with `ErrSchemaExtendUnsupported`.

`extend` fails closed in some cases where `infer` succeeds by widening or
truncating:

- `before` and `after` with different top-level fields.
- An added `time.Time` finer than a microsecond (for example `time.Now()` on
  Linux): the added field is inferred as `timestamp-micros`, which cannot hold
  nanoseconds. Truncate the value in the processor
  (`t.Truncate(time.Microsecond)`) or attach a schema.
- A kept field whose carried type changed, or a key added to or removed from a
  nested record.
- A kept field that refers to a named type defined only in a removed field.
- One payload part structured and the other still non-empty raw bytes, which
  cannot be re-encoded with the extended schema.

`extend` is opt-in in v0.22. In v0.23 `infer` warns when it widens a carried
schema, and in v0.24 `extend` becomes the default. See
[the design document](docs/design-documents/20261106-schema-encode-extend.md).

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
   directory as Conduit. The directory can be changed with the `-processors.path` flag.

   An example directory structure would be:
   ```shell
   .
   ├── conduit
   └── processors
       └── example-processor.wasm
   ```
2. Use the processor in the `processors` section of the pipeline configuration file.
   using the name the processor defines in its specifications. For example:
   ```yaml
   processors:
     - id: my-example-processor
       plugin: example-processor
       settings:
         field: 'foo'
         value: 'bar'
   ```

### How do I log in a processor?

You can get a [`zerolog.Logger`](https://pkg.go.dev/github.com/rs/zerolog) instance
from the context using the
[`sdk.Logger`](https://pkg.go.dev/github.com/conduitio/conduit-processor-sdk#Logger)
function. This logger is pre-configured to append logs in the format expected by
Conduit.

Keep in mind that logging in the hot path (i.e. in the `Process` method) can have
a significant impact on the performance of your processor, therefore we recommend
to use the `Trace` level for logs that are not essential for the operation of the
processor.

Example:

```go
func (p *ExampleProcessor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
    logger := sdk.Logger(ctx)
    logger.Trace().Msg("Processing records")
    // ...
}
```
