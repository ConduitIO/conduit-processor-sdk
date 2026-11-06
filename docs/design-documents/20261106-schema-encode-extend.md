# Schema encode middleware: extend the carried schema instead of inferring one

## Summary

The schema encode middleware (`ProcessorWithSchemaEncode`) re-encodes a processor's structured output.
When the output no longer fits the schema the record carried (a processor added a field), it infers a
schema from the decoded Go values and registers it as a new version of the carried subject. Inference
widens types: Avro `int` becomes `long`, `date` becomes `timestamp-micros`, `decimal(14,2)` becomes
`decimal(31,10)`. One `field.set` that adds `__op` is enough to lose every source type in the record.

This document adds an opt-in strategy, `extend`, selected per part with two new parameters:

| Parameter | Values | Default |
| --- | --- | --- |
| `sdk.schema.encode.payload.strategy` | `infer`, `extend` | `infer` |
| `sdk.schema.encode.key.strategy` | `infer`, `extend` | `infer` |

With `extend`, the middleware builds the new schema from the carried one: fields still present keep
their exact carried definition, fields the processor added get an inferred nullable type with
`"default": null`, and fields the processor removed are left out. A value that does not fit its carried
type is an error, never a coerced value. `infer` is today's behavior, byte for byte.

Versioning: opt-in in v0.22, `infer` warns in v0.23 when it is about to widen a carried schema, `extend`
becomes the default in v0.24.

Roadmap: v0.22 Debezium-compatible Postgres output and Kafka to Kafka type preservation. Evidence: the
v0.21 CDC-parity smoke (smoke-20261105, wide Postgres table) and the conduit design document
`20261106-avro-encode-preserve-source-schema.md`, whose open question 1 this answers.

## Context

The pipeline `postgres -> field.set (.Payload.After.__op) -> avro.encode -> kafka`:

1. The Postgres source attaches an Avro schema with `int`, `bytes`/`decimal(14,2)` and `int`/`date`.
2. The decode middleware in front of `field.set` decodes the payload. The generic Avro decode returns
   Go `int`, `time.Time` and `*big.Rat`. None of these carry the Avro type.
3. `field.set` adds `__op`. The subject and version in the metadata are unchanged.
4. The encode middleware tries the carried schema. It does not fit (`__op` is not a field), so it calls
   `schema.KnownSerdeFactories[TypeAvro].SerdeForType` on the values and registers the result as the
   next version of the source subject: `long`, `decimal(31,10)`, `timestamp-micros`.
5. `avro.encode` (whichever strategy it uses) now sees a record that carries the inferred version.

The conduit-side `fromRecord` strategy for `avro.encode` cannot recover the types after step 4. The fix
has to be in step 4.

## Constraints

- **Invariant 6.** A value that does not fit its carried type is an error record. The middleware never
  widens, narrows, rounds or truncates a value.
- **Default unchanged.** With `infer` (the default), the registered schemas, wire bytes and metadata are
  exactly what they are today. The only visible change is two new parameters in the processor
  specification.
- **No new dependencies.** No change to `conduit-commons` or `conduit-connector-protocol`.
- **Same registry calls.** `extend` uses the `schema.Get` and `schema.Create` calls `infer` uses, so it
  works unchanged in built-in processors and in standalone (WASM) processors.

## Alternatives considered

### A. Extend the carried schema (chosen)

Described under _Decision_. Types the schema declares are never re-derived from Go values, so `int`,
`date`, decimals, enums, fixed, named types, field order, docs, defaults and custom properties survive.

### B. Make inference narrower

Infer `int` for values that fit 32 bits, `date` for midnight-UTC times, the decimal scale from the
value. Rejected: types would depend on values (two rows of one table register different schemas), a
`*big.Rat` still has no declared precision, and it changes the default for every pipeline at once.

### C. Decode into type-preserving Go values

Decode Avro `int` to `int32`, `date` to a date type, decimals to a type with precision and scale.
Rejected: it changes the Go types every processor sees (`custom.javascript`, `field.*`, every WASM
processor), it needs new value types in `conduit-commons`, and inference would still lose enums, named
types and properties.

### D. Turn the encode middleware off on shape-changing processors

`sdk.schema.encode.payload.enabled: false` works today: the payload stays structured and the metadata
keeps naming the source schema, so a later `avro.encode` `fromRecord` sees it. Rejected as the fix,
kept as the workaround: every shape-changing processor needs it, a destination that expects raw data
gets structured data, and the default path still widens types.

## Decision

### Configuration

```yaml
processors:
  - id: add-op
    plugin: field.set
    settings:
      field: .Payload.After.__op
      value: c
      sdk.schema.encode.payload.strategy: extend
```

In Go, `ProcessorWithSchemaEncodeConfig` gains `PayloadStrategy` and `KeyStrategy`
(`SchemaEncodeStrategy`, empty means `infer`), so a processor can change its own default through
`MiddlewareOptions`. Any other value fails `Configure`.

### When the strategy applies

Only on the path that infers today: the processor kept the subject and version, and the data does not
fit the carried schema losslessly. Every other path is unchanged:

- Data that still fits the carried schema is encoded with it (existing fidelity path).
- A processor that attached a different subject or version gets that schema (existing path).
- A record with no schema metadata is left structured with a warning (existing path).

### Building the extended schema

For one part (the key) or the structured payload parts (`before` and `after`, which share one schema).
A structured part with no fields (`{}`) is a structured part like any other; only nil and zero-length raw
data are absent (see _Empty parts_):

1. The carried schema must be Avro, have no schema references, and be a top-level `record`. Otherwise
   `ErrSchemaExtendUnsupported`.
2. All structured parts must have the same top-level field names. Otherwise `ErrSchemaExtendMismatch`
   naming the field and the part that lacks it: one schema cannot describe both, and filling the gap
   with a null or a default would invent data. This includes `before` with fields next to `after` as
   `{}`. Decided by DeVaris for the v0.22 opt-in; revisit before `extend` becomes the default in v0.24.
3. **Kept fields.** Each carried field whose name is a key of the data is copied from the carried schema
   text as raw JSON (only insignificant whitespace is removed), in the carried order. Top-level schema
   attributes (`name`, `namespace`, `doc`, custom properties) are kept too.
4. **Removed fields.** Each carried field that is not a key of the data is left out. Removal is the
   processor's intent (decision by DeVaris); it is not an error. The rule applies to every field: when
   the data is `{}`, every carried field is removed and the new version is a record schema with zero
   fields (`"fields": []`) that keeps the carried `name`, `namespace` and other top-level attributes
   (decision by DeVaris). It is registered as a new version of the carried subject and attached, so the
   record's schema metadata matches its data instead of naming the carried schema.
5. **Added fields.** Each key the carried schema lacks gets a type inferred from its value with the
   existing extractor (the value from `after` when it is non-null, else from `before`). The type is made
   nullable: a union that already has `null` gets `null` moved to the first branch, anything else
   becomes `["null", T]`, and the field gets `"default": null`. Added fields are appended in name order,
   so the same added set always produces the same schema text.
6. If nothing was added or removed, the carried schema is used as it is and nothing is registered. The
   lossless path before the strategy normally catches this case; it is kept so that a value mismatch
   alone never registers a reformatted copy of the carried schema.
7. The extended schema must parse. A parse failure (an added record whose inferred name collides with a
   carried named type, or a kept field that refers to a named type defined only in a removed field) is
   `ErrSchemaExtendUnsupported`.
8. **Lossless check.** Every part is encoded with the extended schema and decoded back, and the result is
   compared with the processor's value (the existing `encodeLossless`). Any difference or codec error is
   `ErrSchemaExtendMismatch` naming the field path. This catches a type change, a null in a non-null
   field, a decimal finer than its scale, a time finer than its logical type, and a key added or removed
   inside a nested record.
9. Only then is the schema registered with `schema.Create` under the carried subject, as `infer` does,
   and attached to the record. A registration error fails the record.

Steps 2, 3, 5 and 8 carry `// Invariant 6` comments in the code.

### Empty parts

A part that is nil or zero-length raw data is absent: it is not encoded, does not contribute to the
schema, and its metadata is left as it came in (decision by DeVaris). For the key, and for a payload
whose `before` and `after` are both absent, this is the existing "not structured" path, so the record
keeps its data and all of its metadata. A payload with one absent part and one structured part encodes
only the structured part; the absent part stays as it is.

Structured data with no fields (`{}`) is not absent: it is a part whose fields were all removed, and gets
the zero-field schema described in step 4. A zero-field Avro record encodes to zero bytes, so the encoded
part is `RawData` of length zero. The decode middleware treats a zero-length payload part as absent and
does not decode it (it does decode a zero-length key), so a later processor sees that payload part as
raw, empty data while the metadata names the zero-field schema. Both describe the same value: a record
with no fields.

### Errors

| Error | When |
| --- | --- |
| `ErrSchemaExtendMismatch` | A value does not fit the extended schema exactly (step 8), or the parts disagree on top-level fields (step 2). |
| `ErrSchemaExtendUnsupported` | The carried schema is not an Avro record, has references, or the extended schema does not parse. |

Both are exported sentinels, wrapped in the `ErrorRecord` with the part (`key`, `payload.before`,
`payload.after`), the subject and version, and the field path. Field values are never in the message.
Conduit maps them to its own stable error codes (see _Conduit_).

## Failure modes

| Case | Behavior | Why |
| --- | --- | --- |
| No carried schema (no subject/version metadata) | Data left structured, warning once (unchanged) | Nothing to extend |
| Carried schema unavailable from the registry | Record fails with the `schema.Get` error (unchanged) | Never fall back to inference on a transient error; it would register a lossy version |
| Processor changed a field's type (`int` to `string`) | `ErrSchemaExtendMismatch` | The carried type is kept; the codec rejects the value |
| Processor wrote a finer value (`1.234` into `decimal(14,2)`, 01:00 into `date`) | `ErrSchemaExtendMismatch` | The codec truncates silently on released commons; the round trip catches it |
| Processor removed a top-level field | Field left out of the new version | User intent |
| Processor removed every top-level field (`{}`) | Zero-field record schema (carried name and namespace) registered as a new version and attached; data encodes to zero bytes | The removal rule applied to every field; the metadata matches the data |
| Part is nil or zero-length raw data | Passed through; data and metadata unchanged; nothing registered | Absent, not a value |
| Processor added a top-level field | Nullable field, default null, appended in name order | Backward-compatible extension |
| Processor added or removed a key inside a nested record | `ErrSchemaExtendMismatch` | Only top-level extension is defined; the nested record type is kept exactly |
| Null into a non-null carried field | `ErrSchemaExtendMismatch` | Codec error |
| Value into a carried `["null", T]` field | Encoded as `T` | Fits |
| Multi-branch union fields | Encoded when the decoded value round-trips, else mismatch | Branch resolution is the codec's; the round trip proves it |
| Carried `["null", record]` or `["null", map]` field (conduit-commons#306) | `ErrSchemaExtendMismatch` | The decoded form does not re-encode with that schema. `infer` falls back to extraction here; `extend` fails closed rather than widen. Fixed when #306 is |
| Arrays and maps | Kept exactly; contents checked by the round trip | |
| Key and payload | Independent strategies, schemas and subjects | They are separate parts with separate metadata |
| `before` and `after` with different top-level fields (including one of them `{}`) | `ErrSchemaExtendMismatch` naming the field; nothing registered | One schema describes both; filling the gap would invent nulls. Decided for the v0.22 opt-in, revisit before the v0.24 default |
| Added field null in one record and set in the next | Different inferred `T`, so a different schema text and a new version | Same as `infer`; added fields should have a stable type |
| Added record collides with a carried named type | `ErrSchemaExtendUnsupported` | Fail closed |
| Kept field refers to a named type defined in a removed field | `ErrSchemaExtendUnsupported` | Fail closed; inlining the definition is not done |
| Carried schema has references | `ErrSchemaExtendUnsupported` | Copying references under the same subject is not needed for the sources this targets |
| Non-Avro carried schema | `ErrSchemaExtendUnsupported` | Only Avro can be extended |
| Registry refuses the new version (compatibility) | Record fails with the `schema.Create` error | Conduit does not change a subject's compatibility |
| Crash mid-record | Nothing new: the middleware is stateless; the record is re-read and re-encoded | Registration of the same text returns the same version (the SDK caches `Create` by subject and text, and registries deduplicate) |

### Subject, version and compatibility

The new version is registered under the carried subject, exactly where `infer` registers it, so the
subject names do not change between strategies. Compared with the carried version, the new version only
adds nullable fields with a null default (backward compatible) and removes fields (backward compatible,
not forward compatible). Under the registry default `BACKWARD` rule it is accepted. A subject set to
`FORWARD` or `FULL` refuses a version that removes a field; the record then fails with the registry
error. A subject whose latest version was registered by `infer` is different: the check runs against that
latest version, and an `extend` version with `int` or `decimal(14,2)` where the latest has `long` or
`decimal(31,10)` is not `BACKWARD` compatible, so the registry may refuse it. Use a new subject (new
topic) or set that subject's compatibility deliberately.

## Upgrade and rollback

- **Upgrade:** nothing changes until a processor sets a strategy to `extend`. Opting in changes the
  schema registered under the source subject for records that processor reshapes. Consumers that read
  the widened types keep working only where Avro promotion allows (`int` to `long` yes, `decimal(14,2)` to
  `decimal(31,10)` no), which is why this is opt-in.
- **Rollback:** remove the setting or set `infer`. New records are inferred again; records already
  written keep their schema ID and stay readable. A processor built against an older SDK does not know
  the parameter: the middleware does not consume it, so it reaches the processor's own configuration
  parsing, which fails with "unrecognized parameter" for processors that validate their settings (the
  built-ins do). Roll back the config before the binary.
- No serialized state, position or checkpoint format changes.

## Versioning and deprecation plan

- **v0.22:** `extend` ships opt-in. Release notes and processor docs describe it.
- **v0.23:** when `infer` is about to register an inferred schema for a record that carried one, it logs
  a warning once per subject and version naming `sdk.schema.encode.*.strategy: extend`.
- **v0.24:** the default becomes `extend`. `infer` stays selectable for at least two more minor versions
  before any removal is considered (announce, warn, remove).

## Observability

- The error sentinels above, with part, subject, version and field path, in the `ErrorRecord`; conduit
  surfaces them in the DLQ and `inspect`.
- Registered versions are visible in the schema registry; with `extend` a reshaping processor registers
  one new version per distinct added and removed field set.
- No new metric in the SDK: the SDK has no metrics surface. Conduit can count the error codes.

## Decided questions

Decided by DeVaris on 2026-11-06:

1. **Every field removed (`{}`).** Register and attach a zero-field record schema, keeping the carried
   record name and namespace, as a new version of the carried subject (step 4). Conduit's `avro.encode`
   `fromRecord` does the same for `{}`: it drops every declared field and produces the same schema text,
   registered under its configured subject.
2. **`before` and `after` with different top-level fields.** Keep failing with `ErrSchemaExtendMismatch`
   for the v0.22 opt-in (step 2). Revisit before `extend` becomes the default in v0.24.
3. **Empty parts (nil, zero-length raw).** Pass through unchanged, metadata untouched (_Empty parts_).

## Conduit

Built-in processors run this middleware (`pkg/plugin/processor/builtin/registry.go` wraps every built-in
with `sdk.DefaultProcessorMiddleware`), and standalone processors run it inside the WASM module. So
bumping the SDK is enough to make the parameters accepted. Conduit then needs to regenerate the built-in
processor specs and docs (the two parameters appear on every processor), map the two sentinels to stable
error codes, document the setting with `avro.encode` `fromRecord`, and update `llms.txt`.
