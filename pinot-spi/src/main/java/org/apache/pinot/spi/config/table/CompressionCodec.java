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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Immutable compression codec specification with optional parameters.
 *
 * <p>A {@code CompressionCodec} captures a canonicalized (uppercase) codec name together with an
 * immutable parameter map. The canonical string form is either {@code NAME} (no parameters)
 * or {@code NAME(level)} when a compression level is present.
 *
 * <p>Well-known codecs are available as static constants (e.g. {@link #SNAPPY}, {@link #ZSTANDARD}).
 * Parameterized codecs are created via {@link #of(String, Map)} or parsed from strings like
 * {@code "ZSTD(3)"} via {@link #fromString(String)}.
 *
 * <p>This class replaces both the former {@code FieldConfig.CompressionCodec} enum and the
 * {@code ChunkCompressionType} enum, unifying codec representation across config and segment layers.
 *
 * <p>This class is immutable and thread-safe.
 */
public final class CompressionCodec {

  // ---------------------------------------------------------------------------
  // Registry and capability maps (must be declared BEFORE the constants that call register())
  // ConcurrentHashMap for thread-safe plugin registration at runtime
  // ---------------------------------------------------------------------------

  private static final Map<String, CompressionCodec> KNOWN_CODECS = new ConcurrentHashMap<>();
  private static final Map<String, boolean[]> CAPABILITIES = new ConcurrentHashMap<>();

  /**
   * Registers a compression codec with its capabilities and returns the canonical instance.
   *
   * <p>Built-in codecs are registered at class-load time. Plugins may call this method to
   * register custom codecs so they are recognized by {@link #of(String)}, {@link #fromString(String)},
   * and the capability query methods.
   *
   * @param name uppercase canonical codec name
   * @param applicableToRawIndex whether the codec can be used for raw forward indexes
   * @param applicableToDictEncodedIndex whether the codec can be used for dictionary-encoded indexes
   * @return the registered {@code CompressionCodec} instance
   */
  public static CompressionCodec register(String name, boolean applicableToRawIndex,
      boolean applicableToDictEncodedIndex) {
    CompressionCodec codec = new CompressionCodec(name, Collections.emptyMap());
    KNOWN_CODECS.put(name, codec);
    CAPABILITIES.put(name, new boolean[]{applicableToRawIndex, applicableToDictEncodedIndex});
    return codec;
  }

  // ---------------------------------------------------------------------------
  // Well-known codec constants
  // ---------------------------------------------------------------------------

  public static final CompressionCodec PASS_THROUGH = register("PASS_THROUGH", true, false);
  public static final CompressionCodec SNAPPY = register("SNAPPY", true, false);
  public static final CompressionCodec ZSTANDARD = register("ZSTANDARD", true, false);
  public static final CompressionCodec LZ4 = register("LZ4", true, false);
  public static final CompressionCodec GZIP = register("GZIP", true, false);
  public static final CompressionCodec MV_ENTRY_DICT = register("MV_ENTRY_DICT", false, true);
  public static final CompressionCodec CLP = register("CLP", false, false);
  public static final CompressionCodec CLPV2 = register("CLPV2", false, false);
  public static final CompressionCodec CLPV2_ZSTD = register("CLPV2_ZSTD", false, false);
  public static final CompressionCodec CLPV2_LZ4 = register("CLPV2_LZ4", false, false);
  public static final CompressionCodec DELTA = register("DELTA", false, false);
  public static final CompressionCodec DELTADELTA = register("DELTADELTA", false, false);

  // Wire-format only: used in segment file headers but not as a user-facing config value
  public static final CompressionCodec LZ4_LENGTH_PREFIXED = register("LZ4_LENGTH_PREFIXED", true, false);

  // ---------------------------------------------------------------------------
  // Wire format: stable integer IDs for segment file headers (must never change)
  // ---------------------------------------------------------------------------

  private static final CompressionCodec[] WIRE_ID_TO_CODEC = {
      PASS_THROUGH,         // 0
      SNAPPY,               // 1
      ZSTANDARD,            // 2
      LZ4,                  // 3
      LZ4_LENGTH_PREFIXED,  // 4
      GZIP,                 // 5
      DELTA,                // 6
      DELTADELTA            // 7
  };

  private static final Map<String, Integer> CODEC_NAME_TO_WIRE_ID;

  static {
    Map<String, Integer> map = new HashMap<>();
    for (int i = 0; i < WIRE_ID_TO_CODEC.length; i++) {
      map.put(WIRE_ID_TO_CODEC[i].getName(), i);
    }
    CODEC_NAME_TO_WIRE_ID = Collections.unmodifiableMap(map);
  }

  // ---------------------------------------------------------------------------
  // Alias map for parser (ConcurrentHashMap for thread-safe plugin registration)
  // ---------------------------------------------------------------------------

  private static final Map<String, String> ALIASES = new ConcurrentHashMap<>();

  static {
    ALIASES.put("ZSTD", "ZSTANDARD");
  }

  /**
   * Registers an alias that the parser will normalize to the given canonical name.
   *
   * <p>For example, {@code registerAlias("ZSTD", "ZSTANDARD")} causes {@code fromString("ZSTD")}
   * to produce a {@code CompressionCodec} with name {@code "ZSTANDARD"}.
   *
   * @param alias the alias (will be uppercased)
   * @param canonicalName the canonical codec name the alias maps to
   */
  public static void registerAlias(String alias, String canonicalName) {
    ALIASES.put(alias.toUpperCase(Locale.ENGLISH), canonicalName.toUpperCase(Locale.ENGLISH));
  }

  // ---------------------------------------------------------------------------
  // Fields
  // ---------------------------------------------------------------------------

  private final String _name;
  private final Map<String, String> _params;

  private CompressionCodec(String name, Map<String, String> params) {
    _name = name;
    _params = params;
  }

  // ---------------------------------------------------------------------------
  // Factory methods
  // ---------------------------------------------------------------------------

  /**
   * Creates a {@code CompressionCodec} with the given canonicalized name and parameter map.
   *
   * @param name   uppercase codec name (e.g. {@code "ZSTANDARD"})
   * @param params parameter map; the map is copied defensively
   * @return a {@code CompressionCodec} instance
   */
  public static CompressionCodec of(String name, Map<String, String> params) {
    Objects.requireNonNull(name, "Codec name must not be null");
    Objects.requireNonNull(params, "Params must not be null");
    if (params.isEmpty()) {
      return of(name);
    }
    return new CompressionCodec(name, Collections.unmodifiableMap(new LinkedHashMap<>(params)));
  }

  /**
   * Creates a {@code CompressionCodec} with the given canonicalized name and no parameters.
   * Returns the well-known constant if one exists.
   *
   * @param name uppercase codec name (e.g. {@code "SNAPPY"})
   * @return a {@code CompressionCodec} instance
   */
  public static CompressionCodec of(String name) {
    Objects.requireNonNull(name, "Codec name must not be null");
    CompressionCodec known = KNOWN_CODECS.get(name);
    return known != null ? known : new CompressionCodec(name, Collections.emptyMap());
  }

  /**
   * Parses a raw codec specification string into a {@code CompressionCodec}.
   *
   * <p>Accepted formats:
   * <ul>
   *   <li>{@code NAME} — codec with no parameters (e.g. {@code "SNAPPY"})</li>
   *   <li>{@code NAME(level)} — codec with a single integer level (e.g. {@code "ZSTD(3)"})</li>
   * </ul>
   *
   * <p>Parsing is case-insensitive; the resulting name is always uppercase.
   * Known aliases (e.g. {@code ZSTD} to {@code ZSTANDARD}) are normalized automatically.
   *
   * @param raw the raw string, e.g. {@code "ZSTD(3)"}, {@code "SNAPPY"}, {@code "lz4(5)"}
   * @return the parsed {@code CompressionCodec}
   * @throws IllegalArgumentException if the input is null, empty, or malformed
   */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static CompressionCodec fromString(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      throw new IllegalArgumentException("Codec specification must not be null or empty");
    }
    String trimmed = raw.trim();

    int openParen = trimmed.indexOf('(');
    int closeParen = trimmed.indexOf(')');

    if (openParen < 0 && closeParen < 0) {
      return of(canonicalize(trimmed));
    }

    if (openParen < 0 || closeParen < 0) {
      throw new IllegalArgumentException("Unbalanced parentheses in codec specification: " + raw);
    }
    if (closeParen != trimmed.length() - 1) {
      throw new IllegalArgumentException("Unexpected characters after closing parenthesis in: " + raw);
    }
    if (openParen == 0) {
      throw new IllegalArgumentException("Missing codec name before '(' in: " + raw);
    }

    String namePart = trimmed.substring(0, openParen);
    String argPart = trimmed.substring(openParen + 1, closeParen);

    if (argPart.isEmpty()) {
      throw new IllegalArgumentException("Empty parentheses in codec specification: " + raw);
    }
    if (argPart.contains(",")) {
      throw new IllegalArgumentException("Multiple arguments are not supported in codec specification: " + raw);
    }

    int level;
    try {
      level = Integer.parseInt(argPart.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Expected integer level argument but got '" + argPart.trim() + "' in: " + raw);
    }

    String name = canonicalize(namePart);
    Map<String, String> params = Collections.singletonMap("level", String.valueOf(level));
    return of(name, params);
  }

  // ---------------------------------------------------------------------------
  // Getters
  // ---------------------------------------------------------------------------

  /**
   * Returns the canonicalized, uppercase codec name.
   */
  public String getName() {
    return _name;
  }

  /**
   * Returns the immutable parameter map.
   */
  public Map<String, String> getParams() {
    return _params;
  }

  /**
   * Convenience method that returns the compression level if the {@code "level"} parameter
   * is present and parseable as an integer.
   *
   * @return an {@link OptionalInt} containing the level, or empty if not set
   */
  public OptionalInt getLevel() {
    String level = _params.get("level");
    if (level == null) {
      return OptionalInt.empty();
    }
    return OptionalInt.of(Integer.parseInt(level));
  }

  // ---------------------------------------------------------------------------
  // Capability queries
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} if this codec is applicable to raw (non-dictionary-encoded) forward indexes.
   */
  public boolean isApplicableToRawIndex() {
    boolean[] caps = CAPABILITIES.get(_name);
    return caps != null && caps[0];
  }

  /**
   * Returns {@code true} if this codec is applicable to dictionary-encoded forward indexes.
   */
  public boolean isApplicableToDictEncodedIndex() {
    boolean[] caps = CAPABILITIES.get(_name);
    return caps != null && caps[1];
  }

  // ---------------------------------------------------------------------------
  // Wire format methods
  // ---------------------------------------------------------------------------

  /**
   * Returns the stable integer wire ID for this codec, used in segment file headers.
   *
   * @throws IllegalArgumentException if the codec has no wire ID (e.g. CLP, MV_ENTRY_DICT)
   */
  public static int toWireId(CompressionCodec codec) {
    Integer wireId = CODEC_NAME_TO_WIRE_ID.get(codec.getName());
    if (wireId == null) {
      throw new IllegalArgumentException("No wire ID for codec: " + codec.getName());
    }
    return wireId;
  }

  /**
   * Returns the {@code CompressionCodec} constant for the given wire ID read from a segment file header.
   *
   * @param wireId the integer ID (0..7)
   * @return the corresponding {@code CompressionCodec}
   * @throws IllegalArgumentException if the wire ID is out of range
   */
  public static CompressionCodec fromWireId(int wireId) {
    if (wireId < 0 || wireId >= WIRE_ID_TO_CODEC.length) {
      throw new IllegalArgumentException("Invalid wire ID: " + wireId);
    }
    return WIRE_ID_TO_CODEC[wireId];
  }

  // ---------------------------------------------------------------------------
  // Serialization
  // ---------------------------------------------------------------------------

  /**
   * Returns the canonical string form: {@code NAME} or {@code NAME(level)}.
   * This value is also used as the JSON serialized form via {@link JsonValue}.
   */
  @JsonValue
  @Override
  public String toString() {
    if (_params.isEmpty()) {
      return _name;
    }
    String level = _params.get("level");
    if (level != null) {
      return _name + "(" + level + ")";
    }
    return _name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompressionCodec)) {
      return false;
    }
    CompressionCodec that = (CompressionCodec) o;
    return _name.equals(that._name) && _params.equals(that._params);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _params);
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private static String canonicalize(String name) {
    String upper = name.toUpperCase(Locale.ENGLISH);
    return ALIASES.getOrDefault(upper, upper);
  }
}
