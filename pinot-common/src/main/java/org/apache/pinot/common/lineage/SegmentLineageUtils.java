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
package org.apache.pinot.common.lineage;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Util class for Segment Lineage
 */
public class SegmentLineageUtils {
  private SegmentLineageUtils() {
  }

  // Monotonic counter used as a tiebreaker within the same millisecond. Combined with the
  // wall-clock millisecond prefix this makes IDs lexicographically ordered by creation time,
  // which lets toJsonObject() sort by ID rather than needing a separate timestamp comparison.
  private static final AtomicLong SEQUENCE = new AtomicLong();

  /**
   * Generate a time-ordered lineage entry id.
   * Format: {@code <16-hex-ms-timestamp>_<16-hex-sequence>}.
   * IDs from the same JVM sort lexicographically in creation order, including entries created within the
   * same millisecond (the sequence counter provides the tiebreaker). Across JVMs (e.g. two controllers
   * writing concurrently) the counter is independent, so ordering within the same millisecond is best-effort.
   */
  public static String generateLineageEntryId() {
    return String.format("%016x_%016x", System.currentTimeMillis(), SEQUENCE.getAndIncrement());
  }

  /**
   * Use the segment lineage metadata to filters out either merged segments or original segments in place
   * to make sure that the final segments contain no duplicate data.
   */
  public static void filterSegmentsBasedOnLineageInPlace(Set<String> segments, SegmentLineage segmentLineage) {
    if (segmentLineage != null) {
      for (LineageEntry lineageEntry : segmentLineage.getLineageEntries().values()) {
        if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
          lineageEntry.getSegmentsFrom().forEach(segments::remove);
        } else {
          lineageEntry.getSegmentsTo().forEach(segments::remove);
        }
      }
    }
  }

  /**
   * Returns the set of segments that participate in a live lineage entry and therefore must not be deleted by
   * external callers.
   * <ul>
   *   <li>{@code IN_PROGRESS} entries lock both {@code segmentsFrom} and {@code segmentsTo}.</li>
   *   <li>{@code COMPLETED} entries lock {@code segmentsFrom}</li>
   *   <li>{@code REVERTED} entries lock nothing.</li>
   * </ul>
   */
  public static Set<String> getDeleteBlockedSegments(SegmentLineage segmentLineage) {
    Set<String> blocked = new HashSet<>();
    for (LineageEntry lineageEntry : segmentLineage.getLineageEntries().values()) {
      switch (lineageEntry.getState()) {
        case IN_PROGRESS:
          blocked.addAll(lineageEntry.getSegmentsFrom());
          blocked.addAll(lineageEntry.getSegmentsTo());
          break;
        case COMPLETED:
          blocked.addAll(lineageEntry.getSegmentsFrom());
          break;
        case REVERTED:
        default:
          break;
      }
    }
    return blocked;
  }
}
