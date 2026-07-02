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
package org.apache.pinot.segment.local.io.codec;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecSpecParser;


/// Registry of known [ChunkCodecHandler] instances, looked up by
/// [ChunkCodecHandler#name()].
///
/// In v1 the built-in set is **closed**: production always uses the immutable [#DEFAULT]
/// instance, which holds the hardcoded built-ins and rejects [#register()]. There is
/// intentionally no plugin registration path yet — a segment whose header names an unknown codec
/// cannot be decoded, so opening the set to third parties requires a controller-side
/// version/codec gate first (see `docs/design/codec-pipeline-v7.md` §12). The mutable
/// constructor + [#register()] exist only for tests.
///
/// The mutable registry is not thread-safe for concurrent writes; tests must complete all
/// registrations before sharing it across threads.
///
/// Lookup is case-insensitive.
public final class CodecRegistry {

  /// Default, immutable registry containing all built-in codecs.
  /// Safe for concurrent reads; does not allow [#register()].
  public static final CodecRegistry DEFAULT;

  static {
    Map<String, ChunkCodecHandler<?>> m = new LinkedHashMap<>();
    m.put(DeltaCodecDefinition.INSTANCE.name().toUpperCase(Locale.ROOT), DeltaCodecDefinition.INSTANCE);
    m.put(DeltaDeltaCodecDefinition.INSTANCE.name().toUpperCase(Locale.ROOT), DeltaDeltaCodecDefinition.INSTANCE);
    m.put(T64CodecDefinition.INSTANCE.name().toUpperCase(Locale.ROOT), T64CodecDefinition.INSTANCE);
    m.put(GorillaCodecDefinition.INSTANCE.name().toUpperCase(Locale.ROOT), GorillaCodecDefinition.INSTANCE);
    m.put(ZstdCodecDefinition.INSTANCE.name().toUpperCase(Locale.ROOT), ZstdCodecDefinition.INSTANCE);
    // Accept the legacy enum spelling ZSTANDARD as an alias for ZSTD so users familiar with
    // FieldConfig.CompressionCodec.ZSTANDARD aren't blocked. Canonicalization still emits "ZSTD",
    // so the on-disk name is unaffected.
    m.put("ZSTANDARD", ZstdCodecDefinition.INSTANCE);
    m.put(Lz4CodecDefinition.INSTANCE.name().toUpperCase(Locale.ROOT), Lz4CodecDefinition.INSTANCE);
    m.put(SnappyCodecDefinition.INSTANCE.name().toUpperCase(Locale.ROOT), SnappyCodecDefinition.INSTANCE);
    m.put(GzipCodecDefinition.INSTANCE.name().toUpperCase(Locale.ROOT), GzipCodecDefinition.INSTANCE);
    DEFAULT = new CodecRegistry(Collections.unmodifiableMap(m));
  }

  private final Map<String, ChunkCodecHandler<?>> _codecs;
  private final boolean _immutable;

  /// Creates a new empty, mutable registry. Intended for tests only — production code should use
  /// [#DEFAULT]. Custom codecs must complete all [#register()] calls before sharing the
  /// registry across threads.
  @VisibleForTesting
  public CodecRegistry() {
    _codecs = new LinkedHashMap<>();
    _immutable = false;
  }

  private CodecRegistry(Map<String, ChunkCodecHandler<?>> codecs) {
    _codecs = codecs;
    _immutable = true;
  }

  /// Registers a codec. Throws if a codec with the same name (case-insensitive) is already present,
  /// or if this registry is immutable.
  ///
  /// @return `this` for fluent chaining
  public CodecRegistry register(ChunkCodecHandler<?> codec) {
    if (_immutable) {
      throw new UnsupportedOperationException("CodecRegistry.DEFAULT is immutable");
    }
    String key = codec.name().toUpperCase(Locale.ROOT);
    if (CodecSpecParser.PIPELINE_KEYWORD.equalsIgnoreCase(key)) {
      throw new IllegalArgumentException(
          "'" + CodecSpecParser.PIPELINE_KEYWORD + "' is a reserved DSL keyword and cannot be used as a codec name");
    }
    if (_codecs.containsKey(key)) {
      throw new IllegalArgumentException("A codec named '" + key + "' is already registered");
    }
    _codecs.put(key, codec);
    return this;
  }

  /// Looks up a codec by name (case-insensitive).
  ///
  /// @return the matching codec handler, or `null` if not found
  @Nullable
  public ChunkCodecHandler<?> get(String name) {
    return _codecs.get(name.toUpperCase(Locale.ROOT));
  }

  /// Looks up a codec by name, throwing [IllegalArgumentException] if not found.
  public ChunkCodecHandler<?> getOrThrow(String name) {
    ChunkCodecHandler<?> codec = get(name);
    if (codec == null) {
      throw new IllegalArgumentException(
          "Unknown codec '" + name + "'. Known codecs: " + _codecs.keySet());
    }
    return codec;
  }
}
