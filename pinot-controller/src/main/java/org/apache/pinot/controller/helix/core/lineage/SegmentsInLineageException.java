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
package org.apache.pinot.controller.helix.core.lineage;

import java.util.Collections;
import java.util.List;


/// Thrown when a segment deletion request targets segments that participate in a live segment lineage entry.
///
/// A segment is considered lineage-locked when some lineage entry has state `IN_PROGRESS` and the segment appears
/// in either `segmentsFrom` or `segmentsTo`, or has state `COMPLETED` and the segment appears in `segmentsFrom`.
/// Such segments must be cleaned up through the lineage lifecycle (end / revert / retention) rather than via the
/// generic delete path. The whole batch is rejected so callers never observe a partial commit.
public class SegmentsInLineageException extends RuntimeException {
  private final String _tableNameWithType;
  private final List<String> _blockingSegments;

  public SegmentsInLineageException(String tableNameWithType, List<String> blockingSegments) {
    super(buildMessage(tableNameWithType, blockingSegments));
    _tableNameWithType = tableNameWithType;
    _blockingSegments = Collections.unmodifiableList(blockingSegments);
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public List<String> getBlockingSegments() {
    return _blockingSegments;
  }

  private static String buildMessage(String tableNameWithType, List<String> blockingSegments) {
    return "Cannot delete segments from table: " + tableNameWithType
        + " because they participate in a live segment lineage entry. Use the lineage lifecycle "
        + "(endReplaceSegments / revertReplaceSegments with forceRevert=true) to clean them up. "
        + "Blocking segments: " + blockingSegments;
  }
}
