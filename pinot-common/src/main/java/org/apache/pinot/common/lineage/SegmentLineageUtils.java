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

import java.util.Set;
import java.util.UUID;


/**
 * Util class for Segment Lineage
 */
public class SegmentLineageUtils {
  private SegmentLineageUtils() {
  }

  /**
   * Generate lineage entry id using UUID.
   *
   * @return lineage entry id
   */
  public static String generateLineageEntryId() {
    return UUID.randomUUID().toString();
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
}
