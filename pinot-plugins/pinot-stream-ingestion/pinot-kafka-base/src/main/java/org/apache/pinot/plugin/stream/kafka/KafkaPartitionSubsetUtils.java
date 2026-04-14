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

  private KafkaPartitionSubsetUtils() {
  }

  /**
   * Reads the optional comma-separated partition ID list from the stream config map.
   * Returns a sorted, deduplicated list for stable ordering when used for partition group metadata.
   * Duplicate IDs in the config are silently removed; this ensures stable ordering and prevents
   * duplicate processing of the same partition.
   *
   * @param streamConfigMap table stream config map (e.g. from
   *                        {@link org.apache.pinot.spi.stream.StreamConfig#getStreamConfigsMap()})
   * @return Sorted list of unique partition IDs when stream.kafka.partition.ids is set and non-empty;
   *         null when not set or blank
   * @throws IllegalArgumentException if the value contains invalid (non-integer) entries
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
      try {
        int partitionId = Integer.parseInt(trimmed);
        if (partitionId < 0) {
          throw new IllegalArgumentException("Invalid " + key
              + " value: partition IDs must be non-negative, got '" + value + "'");
        }
        idSet.add(partitionId);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid " + key + " value: expected comma-separated integers, got '" + value + "'", e);
      }
    }
    if (idSet.isEmpty()) {
      return null;
    }
    List<Integer> ids = new ArrayList<>(idSet);
    Collections.sort(ids);
    return ids;
  }
}
