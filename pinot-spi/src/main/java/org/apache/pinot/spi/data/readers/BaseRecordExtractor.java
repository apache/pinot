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

import java.sql.Timestamp;
import java.util.Base64;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;


/// Default [RecordExtractor] base providing include-list resolution via [#init] and the [#stringifyMapKey] helper.
///
/// @param <T> the format of the input record
public abstract class BaseRecordExtractor<T> implements RecordExtractor<T> {

  /// Include-list resolved from [#init]'s `fields` argument: empty when [#_extractAll] is `true`, otherwise the
  /// (immutable) set of column names to populate. Subclasses read this in their `extract` loop.
  protected Set<String> _fields = Set.of();

  /// `true` when `init(null/empty, ...)` was called — extract every field the input record exposes. Initialized to
  /// `true` so the pre-`init` state is self-consistent with [#_fields] being empty (both interpretations of "no
  /// include list yet" agree on extract-all).
  protected boolean _extractAll = true;

  /// {@inheritDoc}
  ///
  /// Resolves `fields` into [#_fields] / [#_extractAll] (`null` or empty → extract-all; otherwise an immutable
  /// copy), then delegates format-specific configuration to [#initConfig] (default no-op). Format-specific
  /// extractors override [#initConfig] for config rather than reimplementing this method.
  @Override
  public void init(@Nullable Set<String> fields, @Nullable RecordExtractorConfig config) {
    if (CollectionUtils.isEmpty(fields)) {
      _extractAll = true;
      _fields = Set.of();
    } else {
      _extractAll = false;
      _fields = Set.copyOf(fields);
    }
    initConfig(config);
  }

  /// Format-specific config hook called from [#init] after [#_fields] / [#_extractAll] are resolved. Default is a
  /// no-op; override when the extractor needs to read a [RecordExtractorConfig] subtype or wire up per-instance
  /// state (descriptor caches, multi-value delimiters, etc.).
  protected void initConfig(@Nullable RecordExtractorConfig config) {
  }

  /// Stringifies a map key per the `RecordExtractor` `Map<String, Object>` contract. Single source of truth
  /// for map-key stringification across every format extractor:
  /// - `byte[]` → base64 (matches Jackson's `byte[]` value serialization, so a serialized map reads
  ///   uniformly across keys and values).
  /// - `Timestamp` → ISO-8601 UTC via `Timestamp#toInstant().toString()` — JVM-TZ-stable and preserves
  ///   sub-millisecond nanos so distinct Parquet `TIMESTAMP_MICROS` / `TIMESTAMP_NANOS` / `INT96` keys
  ///   don't collapse into colliding entries. (Diverges from the value-side `WRITE_DATES_AS_TIMESTAMPS=true`
  ///   numeric-millis convention; matches Jackson's `WRITE_DATE_KEYS_AS_TIMESTAMPS=false` default for
  ///   date-typed map keys.)
  /// - Everything else (`String`, `Integer`, `Long`, `Float`, `Double`, `Boolean`, `BigDecimal`, `UUID`,
  ///   `LocalDate`, `LocalTime`) has a stable, TZ-independent `toString()`.
  public static String stringifyMapKey(Object key) {
    if (key instanceof byte[]) {
      return Base64.getEncoder().encodeToString((byte[]) key);
    }
    if (key instanceof Timestamp) {
      return ((Timestamp) key).toInstant().toString();
    }
    return key.toString();
  }
}
