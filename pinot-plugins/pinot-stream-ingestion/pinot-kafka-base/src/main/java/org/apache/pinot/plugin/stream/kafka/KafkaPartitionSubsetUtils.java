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
package org.apache.pinot.plugin.stream.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


/**
 * Utilities for parsing and validating Kafka partition subset configuration
 * (stream.kafka.partition.ids) from stream config.
 */
public final class KafkaPartitionSubsetUtils {

  static final int MAX_TOTAL_PARTITION_IDS = 10_000;

  private KafkaPartitionSubsetUtils() {
  }

  /**
   * Reads the optional partition ID specification from the stream config map.
   * Supports three formats, which can be mixed in a single value:
   * <ul>
   *   <li>Individual IDs: {@code "0,2,5"}</li>
   *   <li>Inclusive ranges (both start and end are included): {@code "0-399"}</li>
   *   <li>Mixed: {@code "0-99,200,300-399"}</li>
   * </ul>
   * Returns a sorted, deduplicated list for stable ordering when used for partition group metadata.
   * Duplicate IDs in the config are silently removed; this ensures stable ordering and prevents
   * duplicate processing of the same partition.
   *
   * <p>The total number of unique partition IDs must not exceed {@link #MAX_TOTAL_PARTITION_IDS}
   * (currently 10,000). This guard prevents accidental OOM from typos or overly broad ranges.
   *
   * @param streamConfigMap table stream config map (e.g. from
   *                        {@link org.apache.pinot.spi.stream.StreamConfig#getStreamConfigsMap()})
   * @return Sorted list of unique partition IDs when stream.kafka.partition.ids is set and non-empty;
   *         null when not set or blank
   * @throws IllegalArgumentException if the value contains invalid entries or exceeds
   *         {@link #MAX_TOTAL_PARTITION_IDS}
   */
  @Nullable
  public static List<Integer> getPartitionIdsFromConfig(Map<String, String> streamConfigMap) {
    String key = KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.PARTITION_IDS);
    String value = streamConfigMap.get(key);
    if (StringUtils.isBlank(value)) {
      return null;
    }
    String[] parts = value.split(",");
    Set<Integer> idSet = new HashSet<>();
    for (String part : parts) {
      String trimmed = part.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      int hyphenIndex = trimmed.indexOf('-', 1);
      if (hyphenIndex > 0) {
        parseRange(trimmed, hyphenIndex, key, value, idSet);
      } else {
        parseSingleId(trimmed, key, value, idSet);
      }
      if (idSet.size() > MAX_TOTAL_PARTITION_IDS) {
        throw new IllegalArgumentException(
            "Invalid " + key + " value: total partition count " + idSet.size()
                + " exceeds maximum allowed " + MAX_TOTAL_PARTITION_IDS + ", got '" + value + "'");
      }
    }
    if (idSet.isEmpty()) {
      return null;
    }
    List<Integer> ids = new ArrayList<>(idSet);
    Collections.sort(ids);
    return ids;
  }

  private static void parseSingleId(String trimmed, String key, String originalValue, Set<Integer> idSet) {
    try {
      int partitionId = Integer.parseInt(trimmed);
      if (partitionId < 0) {
        throw new IllegalArgumentException(
            "Invalid " + key + " value: partition ID must be non-negative in '" + trimmed + "', got '"
                + originalValue + "'");
      }
      idSet.add(partitionId);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid " + key + " value: expected integer or range in '" + trimmed + "', got '" + originalValue + "'",
          e);
    }
  }

  // Parses an inclusive range token like "0-399" (both start and end are included).
  private static void parseRange(String trimmed, int hyphenIndex, String key, String originalValue,
      Set<Integer> idSet) {
    String startStr = trimmed.substring(0, hyphenIndex).trim();
    String endStr = trimmed.substring(hyphenIndex + 1).trim();
    int start;
    int end;
    try {
      start = Integer.parseInt(startStr);
      end = Integer.parseInt(endStr);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid " + key + " value: invalid range '" + trimmed + "', got '" + originalValue + "'", e);
    }
    if (start < 0 || end < 0) {
      throw new IllegalArgumentException(
          "Invalid " + key + " value: partition IDs must be non-negative, got '" + originalValue + "'");
    }
    if (start > end) {
      throw new IllegalArgumentException(
          "Invalid " + key + " value: range start must be <= end in '" + trimmed + "', got '" + originalValue + "'");
    }
    long rangeSize = (long) end - start + 1;
    if (idSet.size() + rangeSize > MAX_TOTAL_PARTITION_IDS) {
      throw new IllegalArgumentException(
          "Invalid " + key + " value: total partition count exceeds maximum allowed " + MAX_TOTAL_PARTITION_IDS
              + ", got '" + originalValue + "'");
    }
    for (long i = start; i <= end; i++) {
      idSet.add((int) i);
    }
  }
}
