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
package org.apache.pinot.controller.helix.core;

import java.util.List;


/// Thrown when a public deletion request targets a realtime segment with a `CONSUMING` replica in IdealState.
///
/// The full, immutable blocked-segment list is retained for programmatic callers, while the exception message bounds
/// both the number and length of displayed names so an accidental delete-all request cannot create an unbounded HTTP
/// error response. Instances are immutable and safe to share across threads after construction.
public class ConsumingSegmentDeletionException extends RuntimeException {
  private static final int MAX_DISPLAYED_SEGMENTS = 10;
  private static final int MAX_DISPLAYED_SEGMENT_NAME_LENGTH = 128;

  private final String _tableNameWithType;
  private final List<String> _blockingSegments;

  /// Creates an exception for a rejected realtime-table delete request.
  ///
  /// @param tableNameWithType realtime table resource name
  /// @param blockingSegments target segments with at least one `CONSUMING` replica
  public ConsumingSegmentDeletionException(String tableNameWithType, List<String> blockingSegments) {
    super(buildMessage(tableNameWithType, blockingSegments));
    _tableNameWithType = tableNameWithType;
    _blockingSegments = List.copyOf(blockingSegments);
  }

  /// Returns the realtime table resource name associated with the rejected request.
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  /// Returns the complete immutable list of blocking segments.
  public List<String> getBlockingSegments() {
    return _blockingSegments;
  }

  private static String buildMessage(String tableNameWithType, List<String> blockingSegments) {
    int blockedCount = blockingSegments.size();
    int displayedCount = Math.min(blockedCount, MAX_DISPLAYED_SEGMENTS);
    StringBuilder displayedSegments = new StringBuilder("[");
    for (int i = 0; i < displayedCount; i++) {
      if (i > 0) {
        displayedSegments.append(", ");
      }
      String segmentName = blockingSegments.get(i);
      if (segmentName.length() > MAX_DISPLAYED_SEGMENT_NAME_LENGTH) {
        displayedSegments.append(segmentName, 0, MAX_DISPLAYED_SEGMENT_NAME_LENGTH - 3).append("...");
      } else {
        displayedSegments.append(segmentName);
      }
    }
    displayedSegments.append(']');

    return "Cannot delete " + blockedCount + " segment(s) from realtime table: " + tableNameWithType
        + " because at least one replica is in CONSUMING state. Blocking segments (showing " + displayedCount
        + " of " + blockedCount + "): " + displayedSegments + ". Pause the table, poll "
        + "/tables/{tableName}/pauseStatus until consumingSegments is empty, then retry the deletion.";
  }
}
