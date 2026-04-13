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
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;


/**
 * Immutable, canonical representation of a configured compression codec and its optional parameters.
 *
 * <p>This class is thread-safe because it is immutable.
 */
public final class CompressionCodecSpec implements Serializable {
  private final String _name;
  private final Map<String, String> _params;

  private CompressionCodecSpec(String name, Map<String, String> params) {
    _name = name;
    _params = params;
  }

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static CompressionCodecSpec fromString(String rawSpec) {
    return CompressionCodecSpecParser.parse(rawSpec);
  }

  public static CompressionCodecSpec of(String codecName) {
    return of(codecName, Collections.emptyMap());
  }

  public static CompressionCodecSpec of(String codecName, @Nullable Integer level) {
    if (level == null) {
      return of(codecName);
    }
    return of(codecName, Collections.singletonMap(CompressionCodecCapabilities.LEVEL_PARAM, Integer.toString(level)));
  }

  public static CompressionCodecSpec of(String codecName, Map<String, String> params) {
    String canonicalName = CompressionCodecCapabilities.canonicalizeCodecName(codecName);
    return new CompressionCodecSpec(canonicalName, normalizeParams(params));
  }

  @Nullable
  public static CompressionCodecSpec fromCompressionCodec(@Nullable CompressionCodec codec) {
    return codec != null ? of(codec, null) : null;
  }

  public static CompressionCodecSpec of(CompressionCodec codec, @Nullable Integer level) {
    Objects.requireNonNull(codec, "Compression codec must not be null.");
    CompressionCodecCapabilities.validateLevelSupport(codec.name(), level, toCanonicalString(codec.name(), level));
    return of(codec.name(), level);
  }

  public String getName() {
    return _name;
  }

  public Map<String, String> getParams() {
    return _params;
  }

  public boolean hasLevel() {
    return _params.containsKey(CompressionCodecCapabilities.LEVEL_PARAM);
  }

  @Nullable
  public Integer getLevel() {
    String level = _params.get(CompressionCodecCapabilities.LEVEL_PARAM);
    return level != null ? Integer.valueOf(level) : null;
  }

  @Nullable
  public CompressionCodec getCodec() {
    return CompressionCodecCapabilities.toCompressionCodec(_name);
  }

  @JsonValue
  public String toConfigString() {
    if (_params.isEmpty()) {
      return _name;
    }
    if (_params.size() == 1 && _params.containsKey(CompressionCodecCapabilities.LEVEL_PARAM)) {
      return _name + "(" + _params.get(CompressionCodecCapabilities.LEVEL_PARAM) + ")";
    }
    StringBuilder builder = new StringBuilder(_name).append('(');
    boolean first = true;
    for (Map.Entry<String, String> entry : _params.entrySet()) {
      if (!first) {
        builder.append(',');
      }
      builder.append(entry.getKey()).append('=').append(entry.getValue());
      first = false;
    }
    return builder.append(')').toString();
  }

  private static Map<String, String> normalizeParams(Map<String, String> params) {
    Objects.requireNonNull(params, "Compression codec parameters must not be null.");
    if (params.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, String> normalizedParams = new LinkedHashMap<>(params.size());
    for (Map.Entry<String, String> entry : params.entrySet()) {
      String key = Objects.requireNonNull(entry.getKey(), "Compression codec parameter name must not be null.");
      String value = Objects.requireNonNull(entry.getValue(),
          String.format("Compression codec parameter '%s' must not be null.", key));
      if (CompressionCodecCapabilities.LEVEL_PARAM.equals(key)) {
        value = Integer.toString(parseLevel(value, key));
      }
      normalizedParams.put(key, value);
    }
    return Collections.unmodifiableMap(normalizedParams);
  }

  private static int parseLevel(String levelValue, String paramName) {
    String trimmedLevelValue = levelValue.trim();
    if (trimmedLevelValue.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Invalid compression level in parameter '%s'. Expected a non-empty integer.", paramName));
    }
    try {
      return Integer.parseInt(trimmedLevelValue);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid compression level '%s' in parameter '%s'. Expected an integer.", levelValue,
              paramName), e);
    }
  }

  private static String toCanonicalString(String codecName, @Nullable Integer level) {
    return level != null ? codecName + "(" + level + ")" : codecName;
  }

  @Override
  public String toString() {
    return toConfigString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompressionCodecSpec)) {
      return false;
    }
    CompressionCodecSpec that = (CompressionCodecSpec) o;
    return Objects.equals(_name, that._name) && Objects.equals(_params, that._params);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _params);
  }
}
