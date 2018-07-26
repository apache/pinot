/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class LLCUtils {
  /**
   * Compute the table of a sorted list of segments grouped by Kafka partition.
   *
   * @param segmentSet is the set of segment names that need to be sorted.
   * @return map of Stream partition to sorted set of segment names
   */
  public static Map<String, SortedSet<SegmentName>> sortSegmentsByStreamPartition(Set<String> segmentSet) {
    Map<String, SortedSet<SegmentName>> sortedSegmentsByStreamPartition = new HashMap<>();
    for (String segment : segmentSet) {
      // Ignore segments that are not low level consumer segments
      if (!SegmentName.isLowLevelConsumerSegmentName(segment)) {
        continue;
      }

      final LLCSegmentName segmentName = new LLCSegmentName(segment);
      String streamPartitionId = segmentName.getPartitionRange();
      SortedSet<SegmentName> segmentsForPartition = sortedSegmentsByStreamPartition.get(streamPartitionId);

      // Create sorted set if necessary
      if (segmentsForPartition == null) {
        segmentsForPartition = new TreeSet<>();

        sortedSegmentsByStreamPartition.put(streamPartitionId, segmentsForPartition);
      }

      segmentsForPartition.add(segmentName);
    }
    return sortedSegmentsByStreamPartition;
  }
}
