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
/// **Single-value** — preserved as its boxed Java type:
/// - `Boolean`
/// - `Integer` (sub-int integer types — `Byte`, `Short` — are widened to `Integer` so all small ints unify behind a
///   single Pinot type)
/// - `Long`
/// - `Float`
/// - `Double`
/// - [java.math.BigDecimal] ([java.math.BigInteger] widens to `BigDecimal` since Pinot has no `BigInteger` type)
/// - `String`
/// - `byte[]` — format-native byte representations ([java.nio.ByteBuffer] for Avro, `ByteString` for ProtoBuf, ORC's
///   binary `ColumnVector`, etc.) are materialized into `byte[]`
/// - [java.time.LocalDate] — date-only logical types (Avro `date`, ORC `DATE`, Parquet `DATE`).
///   TZ-independent — a calendar date is the same date everywhere
/// - [java.time.LocalTime] — time-of-day logical types (Avro `time-millis` / `time-micros`, Parquet
///   `TIME_MILLIS` / `TIME_MICROS` / `TIME_NANOS`). TZ-independent with full nanosecond precision
/// - [java.sql.Timestamp] — full date-time logical types (Avro `timestamp-millis` / `timestamp-micros`, ORC
///   `TIMESTAMP` / `TIMESTAMP_INSTANT`, Parquet `INT96` / `TIMESTAMP_MILLIS` / `TIMESTAMP_MICROS`, JDBC `TIMESTAMP`).
///   The base bridges [java.time.Instant] / [java.time.OffsetDateTime] / [java.time.ZonedDateTime] losslessly,
///   and bridges [java.time.LocalDateTime] interpreting its wall-clock as UTC
///
/// **Multi-value:** [java.util.Collection] and array values (except `byte[]`) become `Object[]` with each element
/// recursively converted under the same contract.
///
/// **Map / nested complex:** map values, format-native nested records (Avro `record`, ProtoBuf nested `Message`,
/// Thrift nested `TBase`, ORC `struct` / `map`, JSON object) become `Map<Object, Object>` (or `Map<String, Object>`
/// for record-like inputs) with each value recursively converted.
///
/// **Fallback:** any other type — Avro `Utf8`, ProtoBuf `EnumValueDescriptor`, Thrift `TEnum`, etc. — falls back to
/// `value.toString()`.
///
/// **Null:** preserved as `null`.
///
/// The default conversion paths (single-value → [BaseRecordExtractor#convertSingleValue], multi-value →
/// [BaseRecordExtractor#convertMultiValue], map → [BaseRecordExtractor#convertMap], nested record →
/// [BaseRecordExtractor#convertRecord]) live in [BaseRecordExtractor]; format-specific extractors override only
/// the parts that need format-specific handling (e.g. Avro maps `GenericFixed` → `byte[]`).
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

  /// Converts a non-null field value to the type matrix documented on [RecordExtractor] — single-value (boxed
  /// number / boolean / `String` / `byte[]`), multi-value (`Object[]`), or `Map<Object, Object>`. Java `null`
  /// inputs are short-circuited by callers and never reach this method.
  ///
  /// Conversion preserves the input *shape* — empty `Object[]`, empty `Map`, map entries whose value is `null` etc.
  /// all round-trip unchanged so ingestion transforms can see them. The data-type transformer later in the pipeline
  /// coerces values to the column's declared Pinot [org.apache.pinot.spi.data.FieldSpec.DataType].
  ///
  /// @param value the non-null field value to convert
  /// @return the converted value. The default base implementation never returns `null`; the return is marked
  ///         nullable so format-specific overrides retain the option to translate format-native null sentinels.
  @Nullable
  Object convert(Object value);
}
