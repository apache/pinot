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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;


/**
 * Immutable specification of a compression codec with optional parameters.
 *
 * <p>A {@code CodecSpec} captures a canonicalized (uppercase) codec name together with an
 * immutable parameter map. The canonical string form is either {@code NAME} (no parameters)
 * or {@code NAME(level)} when a level parameter is present.
 *
 * <p>Instances are created via the {@link #of} factory methods or deserialized from JSON
 * strings using the parser invoked by the {@link JsonCreator} factory.
 *
 * <p>This class is immutable and thread-safe.
 */
public final class CodecSpec {

  private final String _name;
  private final Map<String, String> _params;

  private CodecSpec(String name, Map<String, String> params) {
    _name = name;
    _params = params;
  }

  /**
   * Creates a {@code CodecSpec} with the given canonicalized name and parameter map.
   *
   * @param name   uppercase codec name (e.g. {@code "ZSTANDARD"})
   * @param params parameter map; the map is copied defensively
   * @return a new {@code CodecSpec}
   */
  public static CodecSpec of(String name, Map<String, String> params) {
    Objects.requireNonNull(name, "Codec name must not be null");
    Objects.requireNonNull(params, "Params must not be null");
    return new CodecSpec(name, params.isEmpty() ? Collections.emptyMap()
        : Collections.unmodifiableMap(new LinkedHashMap<>(params)));
  }

  /**
   * Creates a {@code CodecSpec} with the given canonicalized name and no parameters.
   *
   * @param name uppercase codec name (e.g. {@code "SNAPPY"})
   * @return a new {@code CodecSpec}
   */
  public static CodecSpec of(String name) {
    return of(name, Collections.emptyMap());
  }

  /**
   * Jackson delegating creator that parses a string representation into a {@code CodecSpec}.
   *
   * @param raw the raw string such as {@code "ZSTD(3)"} or {@code "SNAPPY"}
   * @return the parsed {@code CodecSpec}
   */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static CodecSpec fromString(String raw) {
    return CodecSpecParser.parse(raw);
  }

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
    if (!(o instanceof CodecSpec)) {
      return false;
    }
    CodecSpec that = (CodecSpec) o;
    return _name.equals(that._name) && _params.equals(that._params);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _params);
  }
}
