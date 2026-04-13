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

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;


/**
 * String-based codec canonicalization and capability metadata for compression codecs.
 *
 * <p>This class is thread-safe because it exposes only immutable static state.
 */
public final class CompressionCodecCapabilities {
  public static final String LEVEL_PARAM = "level";

  private static final Map<String, String> ALIASES = createAliases();
  private static final Map<String, LevelRange> LEVEL_RANGES = createLevelRanges();

  private CompressionCodecCapabilities() {
  }

  public static String canonicalizeCodecName(String codecName) {
    if (codecName == null) {
      throw new IllegalArgumentException("Compression codec name must not be null.");
    }
    String normalizedCodecName = codecName.trim();
    if (normalizedCodecName.isEmpty()) {
      throw new IllegalArgumentException("Compression codec name must not be empty.");
    }
    String upperCaseCodecName = normalizedCodecName.toUpperCase(Locale.ROOT);
    return ALIASES.getOrDefault(upperCaseCodecName, upperCaseCodecName);
  }

  public static String canonicalizeAlias(String codecName) {
    return canonicalizeCodecName(codecName);
  }

  public static boolean supportsLevel(String codecName) {
    return LEVEL_RANGES.containsKey(canonicalizeCodecName(codecName));
  }

  @Nullable
  public static Integer getMinLevel(String codecName) {
    LevelRange range = LEVEL_RANGES.get(canonicalizeCodecName(codecName));
    return range != null ? range._min : null;
  }

  @Nullable
  public static Integer getMaxLevel(String codecName) {
    LevelRange range = LEVEL_RANGES.get(canonicalizeCodecName(codecName));
    return range != null ? range._max : null;
  }

  public static void validateLevelSupport(String codecName, @Nullable Integer level, String rawSpec) {
    if (level == null) {
      return;
    }
    String canonicalCodecName = canonicalizeCodecName(codecName);
    LevelRange range = LEVEL_RANGES.get(canonicalCodecName);
    if (range == null) {
      throw new IllegalArgumentException(
          String.format("Compression codec '%s' does not support a level parameter: '%s'.", canonicalCodecName,
              rawSpec));
    }
    if (level < range._min || level > range._max) {
      throw new IllegalArgumentException(
          String.format("Compression level %d is out of range for codec '%s'; expected [%d, %d].", level,
              canonicalCodecName, range._min, range._max));
    }
  }

  @Nullable
  public static CompressionCodec toCompressionCodec(String codecName) {
    String canonicalCodecName = canonicalizeCodecName(codecName);
    try {
      return CompressionCodec.valueOf(canonicalCodecName);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static Map<String, String> createAliases() {
    Map<String, String> aliases = new HashMap<>();
    aliases.put("ZSTD", "ZSTANDARD");
    return Collections.unmodifiableMap(aliases);
  }

  private static Map<String, LevelRange> createLevelRanges() {
    Map<String, LevelRange> levelRanges = new HashMap<>();
    levelRanges.put("ZSTANDARD", new LevelRange(1, 22));
    levelRanges.put("GZIP", new LevelRange(0, 9));
    levelRanges.put("LZ4", new LevelRange(1, 17));
    return Collections.unmodifiableMap(levelRanges);
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
