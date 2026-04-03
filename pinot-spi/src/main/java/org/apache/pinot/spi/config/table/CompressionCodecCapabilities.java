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

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;


/**
 * Central registry for compression codec aliases and optional level capabilities.
 *
 * <p>This class is thread-safe because it exposes only immutable static state.
 */
public final class CompressionCodecCapabilities {
  private static final Map<CompressionCodec, LevelRange> LEVEL_RANGES = new EnumMap<>(CompressionCodec.class);

  static {
    LEVEL_RANGES.put(CompressionCodec.ZSTANDARD, new LevelRange(1, 22));
    LEVEL_RANGES.put(CompressionCodec.GZIP, new LevelRange(0, 9));
    LEVEL_RANGES.put(CompressionCodec.LZ4, new LevelRange(1, 17));
  }

  private CompressionCodecCapabilities() {
  }

  public static CompressionCodec canonicalizeAlias(String codecName) {
    String normalizedCodecName = codecName.toUpperCase(Locale.ROOT);
    if ("ZSTD".equals(normalizedCodecName)) {
      return CompressionCodec.ZSTANDARD;
    }
    try {
      return CompressionCodec.valueOf(normalizedCodecName);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unknown compression codec '%s'.", normalizedCodecName), e);
    }
  }

  public static boolean supportsLevel(CompressionCodec codec) {
    return LEVEL_RANGES.containsKey(codec);
  }

  @Nullable
  public static Integer getMinLevel(CompressionCodec codec) {
    LevelRange range = LEVEL_RANGES.get(codec);
    return range != null ? range._min : null;
  }

  @Nullable
  public static Integer getMaxLevel(CompressionCodec codec) {
    LevelRange range = LEVEL_RANGES.get(codec);
    return range != null ? range._max : null;
  }

  public static void validateLevelSupport(CompressionCodec codec, @Nullable Integer level, String rawSpec) {
    if (level == null) {
      return;
    }
    LevelRange range = LEVEL_RANGES.get(codec);
    if (range == null) {
      throw new IllegalArgumentException(
          String.format("Compression codec '%s' does not support a level parameter: '%s'.", codec, rawSpec));
    }
    if (level < range._min || level > range._max) {
      throw new IllegalArgumentException(
          String.format("Compression level %d is out of range for codec '%s'; expected [%d, %d].", level, codec,
              range._min, range._max));
    }
  }

  private static final class LevelRange {
    private final int _min;
    private final int _max;

    private LevelRange(int min, int max) {
      _min = min;
      _max = max;
    }
  }
}
