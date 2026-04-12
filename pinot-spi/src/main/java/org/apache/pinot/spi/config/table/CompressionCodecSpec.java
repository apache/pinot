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
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;


/**
 * Immutable, canonical representation of a configured compression codec and its optional level.
 *
 * <p>This class is thread-safe because it is immutable.
 */
public final class CompressionCodecSpec implements Serializable {
  private final CompressionCodec _codec;
  @Nullable
  private final Integer _level;

  private CompressionCodecSpec(CompressionCodec codec, @Nullable Integer level) {
    _codec = codec;
    _level = level;
  }

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public static CompressionCodecSpec fromString(String rawSpec) {
    return CompressionCodecSpecParser.parse(rawSpec);
  }

  @Nullable
  public static CompressionCodecSpec fromCompressionCodec(@Nullable CompressionCodec codec) {
    return codec != null ? new CompressionCodecSpec(codec, null) : null;
  }

  public static CompressionCodecSpec of(CompressionCodec codec, @Nullable Integer level) {
    CompressionCodecCapabilities.validateLevelSupport(codec, level, toCanonicalString(codec, level));
    return new CompressionCodecSpec(codec, level);
  }

  public CompressionCodec getCodec() {
    return _codec;
  }

  @Nullable
  public Integer getLevel() {
    return _level;
  }

  public boolean hasLevel() {
    return _level != null;
  }

  @JsonValue
  public String toConfigString() {
    return toCanonicalString(_codec, _level);
  }

  private static String toCanonicalString(CompressionCodec codec, @Nullable Integer level) {
    return level != null ? codec.name() + "(" + level + ")" : codec.name();
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
    return _codec == that._codec && Objects.equals(_level, that._level);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_codec, _level);
  }
}
