/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.data.readers;

import java.io.Serializable;
import java.util.Set;
import javax.annotation.Nullable;


/// Extracts fields from input records of format `T` and writes them into a [GenericRow]. The output value for each
/// field is one of the following Java types — every `RecordExtractor` implementation must adhere to this contract so
/// downstream ingestion transforms see a uniform type matrix:
///
/// **Primitive types** — preserved as their boxed Java type:
/// - `Boolean`
/// - `Integer` (`Byte` / `Short` widen to `Integer` so all small ints unify behind a single Pinot type)
/// - `Long`
/// - `Float`
/// - `Double`
/// - `String`
/// - `byte[]` — format-native byte representations (`ByteBuffer` for Avro, `ByteString` for ProtoBuf, ORC's
///   binary `ColumnVector`, etc.) are materialized into `byte[]`
///
/// **Logical types** — format-specific markers (Avro logical type, Parquet logical type, ORC schema type, JDBC
/// column type) decoded into a uniform Java type regardless of the underlying physical encoding:
/// - `DECIMAL` → `BigDecimal`. Avro `decimal`, Parquet `DECIMAL`, JDBC `DECIMAL` / `NUMERIC`. `BigInteger` widens
///   to `BigDecimal` since Pinot has no `BigInteger` type. Always converted — raw bytes aren't interpretable
///   without external precision / scale
/// - `TIMESTAMP` → `Timestamp` (sub-millisecond precision preserved via `Timestamp#getNanos`).
///   Avro `timestamp-millis` / `timestamp-micros`, ORC `TIMESTAMP` / `TIMESTAMP_INSTANT`,
///   Parquet `INT96` / `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS`, JDBC `TIMESTAMP`, plus native
///   `Instant` / `OffsetDateTime` / `ZonedDateTime` / `LocalDateTime` inputs
/// - `DATE` → `LocalDate`. Avro `date`, ORC `DATE`, Parquet `DATE`. TZ-independent — a calendar date is the same
///   date everywhere
/// - `TIME` → `LocalTime`. Avro `time-millis` / `time-micros`, Parquet `TIME_MILLIS` / `TIME_MICROS` / `TIME_NANOS`.
///   TZ-independent with full nanosecond precision
/// - `UUID` → `java.util.UUID`. Avro `uuid`, Parquet `UUID`. The downstream type transformer adapts to the Pinot
///   column's storage type — `STRING` column gets the canonical UUID string, `BYTES` column gets the 16-byte
///   big-endian form
///
/// Format-specific extractors may expose an `extractRawTimeValues` flag (Avro, Parquet) that bypasses
/// TIMESTAMP / DATE / TIME conversion and surfaces the raw underlying integer (`Integer` for `DATE` /
/// `TIME_MILLIS`, `Long` for the others). DECIMAL and UUID always convert.
///
/// **Multi-value:** `Collection` and array values (except `byte[]`) become `Object[]` with each element
/// recursively converted under the same contract.
///
/// **Map / nested complex:** map values, format-native nested records (Avro `record`, ProtoBuf nested `Message`,
/// Thrift nested `TBase`, ORC `struct` / `map`, JSON object) become `Map<String, Object>` with each value
/// recursively converted. Map keys are stringified at the extractor boundary via [BaseRecordExtractor#stringifyMapKey]
/// — `byte[]` keys base64-encode, `Timestamp` keys serialize via `Timestamp#toInstant().toString()` (ISO-8601
/// UTC, JVM-TZ-stable, full nanos), everything else falls back to `toString()`.
///
/// **Fallback:** any other type — Avro `Utf8`, ProtoBuf `EnumValueDescriptor`, Thrift `TEnum`, etc. — falls back to
/// `value.toString()`.
///
/// **Null:** preserved as `null`.
///
/// The converted values feed ingestion transforms, so empty arrays, empty maps, null map entries, and other "preserve
/// the input shape" subtleties matter. The data-type transformer (later in the ingestion pipeline) coerces these to
/// the column's declared Pinot type.
///
/// @param <T> The format of the input record
public interface RecordExtractor<T> extends Serializable {

  /// Initializes the extractor.
  ///
  /// @param fields the include list — only these fields are written to the output [GenericRow]. `null` or empty means
  ///        extract every field present in the input record.
  /// @param config format-specific configuration (e.g. Avro logical-type toggle, Thrift field-id map, CSV multi-value
  ///        delimiter). May be `null` for formats that don't need config.
  void init(@Nullable Set<String> fields, @Nullable RecordExtractorConfig config);

  /// Extracts fields from `from` into `to`, preserving the input shape per the type contract documented on
  /// [RecordExtractor]. Only fields in the include list passed to [#init] are written; unset / cleared optional
  /// fields surface as `null` (per format).
  ///
  /// @param from the input record in the format's native representation
  /// @param to the row to populate (re-used by the caller across calls)
  /// @return the same `to` instance, populated
  GenericRow extract(T from, GenericRow to);
}
