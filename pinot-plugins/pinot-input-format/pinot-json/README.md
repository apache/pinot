<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pinot JSON input format

This plugin reads JSON records into Pinot. It provides:

- `JSONMessageDecoder` — a `StreamMessageDecoder` for real-time (streaming) ingestion.
- `JSONRecordReader` — a `RecordReader` for batch ingestion of newline-delimited JSON files.
- `JSONRecordExtractor` — the shared extractor that turns a parsed JSON object into a Pinot `GenericRow`.

By default the stream decoder reads **UTF-8 text JSON**. Floating JSON literals are parsed as `BigDecimal` so
high-precision decimals survive ingestion; integers still narrow to `INT` / `LONG` / `BIG_DECIMAL`. The
decoder can additionally decode several **binary** JSON encodings, selected with the `jsonFormat` decoder
property described below.

This precision-preserving parse applies only to **JSON text** (and PostgreSQL `jsonb`, whose wire body is
text JSON). Avro and Protobuf extractors already carry typed numeric values; Avro's `decimal` logical type
is already `BigDecimal`. Smile and CBOR keep native IEEE floats. SQLite JSONB still decodes its ASCII floats
as `DOUBLE` (including JSON5 `Infinity` / `NaN`).

Batch `JSONRecordReader` uses the same BigDecimal-aware reader, so stream and batch text JSON now agree.

**Upgrade note.** Floating JSON literals that previously became `java.lang.Double` are now
`java.math.BigDecimal` until schema conversion. Typed `DOUBLE` / `FLOAT` / `BIG_DECIMAL` columns still
convert through `PinotDataType`. Custom `recordExtractorClass` implementations and Groovy transforms that
do `instanceof Double` (or otherwise assume Jackson's default `Double`) must accept `BigDecimal`. A
`STRING` column stores `BigDecimal.toPlainString()` rather than `Double.toString()` (so `1.23e10` becomes
`12300000000` instead of `1.23E10`). There is no opt-out flag.

> The `jsonFormat` property applies to the **stream decoder** (`JSONMessageDecoder`) only. Batch ingestion via
> `JSONRecordReader` always reads text JSON.

## `jsonFormat` decoder property

Set the decoder property `jsonFormat` (full stream-config key `stream.<type>.decoder.prop.jsonFormat`) to one
of:

| `jsonFormat`     | Payload encoding                                                        |
|------------------|-------------------------------------------------------------------------|
| *(unset)* / `TEXT` | UTF-8 text JSON. The historical default encoding; decimals are `BigDecimal`. |
| `POSTGRES_JSONB` | PostgreSQL `jsonb` binary wire format (version byte + text JSON).        |
| `SQLITE_JSONB`   | SQLite 3.45+ JSONB binary format.                                       |
| `SMILE`          | [Jackson Smile](https://github.com/FasterXML/smile-format-specification) binary JSON. |
| `CBOR`           | [CBOR](https://www.rfc-editor.org/rfc/rfc8949.html) (RFC 8949).         |
| `AUTO`           | Detect the encoding per message from its leading bytes (opt-in).        |

The value is case-insensitive. An unrecognized value fails table creation with a message listing the supported
values.

Every format still feeds the same extractor and schema conversion, so the same table config, schema, and
transform functions work regardless of the wire encoding. Integers narrow to `INT` / `LONG` / `BIG_DECIMAL`.
Text JSON and PostgreSQL `jsonb` keep floating literals as `BigDecimal` until the target column type is
applied. Smile and CBOR keep native IEEE floats. Objects and arrays become nested JSON. (The binary formats
can additionally carry native `float` and `byte[]` scalars, which Pinot converts to the target column type.)

## Default behavior and `AUTO`

An unset `jsonFormat` means `TEXT`. Existing tables keep that encoding; the only behavior change is that
text JSON floating literals that previously became `DOUBLE` now remain `BigDecimal` until schema conversion.

`AUTO` is opt-in rather than the default. Detection is a heuristic over a few leading bytes; it never
mis-routes a well-formed text JSON document (a top-level `{` or `[`, optionally after whitespace, matches none
of the binary signatures), but it can claim a *corrupt* message that text decoding would otherwise have
rejected. Turning that on implicitly for every existing stream would be a silent behavior change, so you ask
for it explicitly.

When `AUTO` is set, each message is classified in this order, falling back to text JSON:

| Order | Format           | Signature                                                                    |
|-------|------------------|------------------------------------------------------------------------------|
| 1     | `SMILE`          | 3-byte header `3A 29 0A` (`:)\n`).                                            |
| 2     | `CBOR`           | Self-describe tag `D9 D9 F7` (RFC 8949 §3.4.6). CBOR **without** the tag is not detected. |
| 3     | `POSTGRES_JSONB` | Version byte `01` followed by a JSON document start.                          |
| 4     | `SQLITE_JSONB`   | A top-level object whose declared size exactly fills the payload.            |
| 5     | `TEXT`           | First non-whitespace byte is `{` or `[`.                                      |

Pin an explicit format instead of `AUTO` when you know the encoding: it skips detection and, for formats
without a strong magic number (notably tag-less CBOR), is the only way to decode them.

The SQLite signature is intentionally narrow, but not zero-risk: a message consisting of the single byte
`0x0C` is a valid, exactly-filling empty SQLite object, so under `AUTO` a stray `0x0C` decodes to an empty row
rather than being rejected. This is the heuristic nature of `AUTO`; pin `TEXT` (or your actual format) to avoid
it entirely.

## Examples

All examples use a Kafka real-time table; substitute your stream type (`stream.<type>.…`) as needed.

### 1. Text JSON (default — no configuration needed)

```json
{
  "streamConfigMaps": [
    {
      "streamType": "kafka",
      "stream.kafka.topic.name": "events",
      "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder",
      "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory",
      "stream.kafka.broker.list": "localhost:9092"
    }
  ]
}
```

### 2. A pinned binary format (SQLite JSONB)

Add a single decoder property:

```json
"stream.kafka.decoder.prop.jsonFormat": "SQLITE_JSONB"
```

Full block:

```json
{
  "streamConfigMaps": [
    {
      "streamType": "kafka",
      "stream.kafka.topic.name": "events",
      "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder",
      "stream.kafka.decoder.prop.jsonFormat": "SQLITE_JSONB",
      "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory",
      "stream.kafka.broker.list": "localhost:9092"
    }
  ]
}
```

Swap the value for `POSTGRES_JSONB`, `SMILE`, or `CBOR` to decode those encodings.

### 3. Auto-detection for a mixed stream

```json
"stream.kafka.decoder.prop.jsonFormat": "AUTO"
```

Decodes text JSON, Smile, CBOR (with the self-describe tag), PostgreSQL `jsonb`, and SQLite JSONB messages on
the same topic, one classification per message.

## Format notes

### PostgreSQL `jsonb`

Despite `jsonb`'s compact on-disk layout, that layout never leaves the server. `jsonb_send` renders the value
as text and emits a version byte (`1`) followed by the UTF-8 JSON text; `jsonb_recv` reverses it. Every
standard binary producer — the v3 extended-query protocol, `COPY ... WITH (FORMAT binary)`, and logical
replication via `pgoutput` — goes through that same function. This parser strips the version byte and parses
the text body, so its value types are identical to `TEXT`.

### SQLite JSONB

Implements the [SQLite JSONB](https://sqlite.org/jsonb.html) format (SQLite 3.45+), including the JSON5 element
types (hex/`+`-prefixed integers, `Infinity`/`NaN` floats, and `\x`/`\'`/`\v` string escapes). Per SQLite's
validity rule, the top-level element must exactly fill the message; a payload that declares a shorter size is
rejected rather than decoded into a partial row.

### Smile and CBOR

Decoded with Jackson. Smile carries its 3-byte header by default (used for `AUTO` detection). CBOR has no
mandatory magic number, so `AUTO` recognizes it only when the optional `D9 D9 F7` self-describe tag is present;
otherwise pin `jsonFormat: CBOR`.

## Diagnostics

When a message cannot be decoded, the decoder throws with a bounded, log-safe description of the payload: a
leading window rendered as text when it looks like text and as hex otherwise, prefixed with the total byte
length. Binary payloads therefore appear as a short hex prefix rather than an unbounded dump of unreadable
bytes.
