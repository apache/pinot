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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


public class DefaultLineageManager implements LineageManager {
  private static final long REPLACED_SEGMENTS_RETENTION_IN_MILLIS = TimeUnit.DAYS.toMillis(1L); // 1 day
  private static final long LINEAGE_ENTRY_CLEANUP_RETENTION_IN_MILLIS = TimeUnit.DAYS.toMillis(1L); // 1 day

  protected ControllerConf _controllerConf;

  public DefaultLineageManager(ControllerConf controllerConf) {
    _controllerConf = controllerConf;
  }

  @Override
  public void updateLineageForStartReplaceSegments(TableConfig tableConfig, String lineageEntryId,
      Map<String, String> customMap, SegmentLineage lineage) {
  }

  @Override
  public void updateLineageForEndReplaceSegments(TableConfig tableConfig, String lineageEntryId,
      Map<String, String> customMap, SegmentLineage lineage) {
  }

  @Override
  public void updateLineageForRevertReplaceSegments(TableConfig tableConfig, String lineageEntryId,
      Map<String, String> customMap, SegmentLineage lineage) {
  }

  /**
   * This method:
   * 1. Update lineage metadata by removing lineage entries
   * 2. Find segments that need to be deleted
   */
  @Override
  public void updateLineageForRetention(TableConfig tableConfig, SegmentLineage lineage, List<String> allSegments,
      List<String> segmentsToDelete, Set<String> consumingSegments) {
    // 1. The original segments can be deleted once the merged segments are successfully uploaded
    // 2. The zombie lineage entry & merged segments should be deleted if the segment replacement failed in
    //    the middle
    Set<String> segmentsForTable = new HashSet<>(allSegments);
    Iterator<LineageEntry> lineageEntryIterator = lineage.getLineageEntries().values().iterator();
    while (lineageEntryIterator.hasNext()) {
      LineageEntry lineageEntry = lineageEntryIterator.next();
      if (lineageEntry.getState() == LineageEntryState.COMPLETED) {
        Set<String> sourceSegments = new HashSet<>(lineageEntry.getSegmentsFrom());
        sourceSegments.retainAll(segmentsForTable);
        if (sourceSegments.isEmpty()) {
          // If the lineage state is 'COMPLETED' and segmentFrom are removed, it is safe clean up the lineage entry
          lineageEntryIterator.remove();
        } else {
          // If the lineage state is 'COMPLETED' and we already preserved the original segments for the required
          // retention, it is safe to delete all segments from 'segmentsFrom'
          if (shouldDeleteReplacedSegments(tableConfig, lineageEntry)) {
            segmentsToDelete.addAll(sourceSegments);
          }
        }
      } else if (lineageEntry.getState() == LineageEntryState.REVERTED || (
          lineageEntry.getState() == LineageEntryState.IN_PROGRESS && lineageEntry.getTimestamp()
              < System.currentTimeMillis() - LINEAGE_ENTRY_CLEANUP_RETENTION_IN_MILLIS)) {
        // If the lineage state is 'IN_PROGRESS' or 'REVERTED', we need to clean up the zombie lineage
        // entry and its segments
        Set<String> destinationSegments = new HashSet<>(lineageEntry.getSegmentsTo());
        destinationSegments.retainAll(segmentsForTable);
        if (destinationSegments.isEmpty()) {
          // If the lineage state is 'IN_PROGRESS or REVERTED' and source segments are already removed, it is safe
          // to clean up the lineage entry. Deleting lineage will allow the task scheduler to re-schedule the source
          // segments to be merged again.
          lineageEntryIterator.remove();
        } else {
          // If the lineage state is 'IN_PROGRESS', it is safe to delete all segments from 'segmentsTo'
          segmentsToDelete.addAll(destinationSegments);
        }
      }
    }
  }


  /**
   * Helper function to decide whether we should delete segmentsFrom (replaced segments) given a lineage entry.
   *
   * The replaced segments are safe to delete if the following conditions are all satisfied
   * 1) Table is "APPEND"
   * 2) It has been more than 24 hours since the lineage entry became "COMPLETED" state.
   *
   * @param tableConfig a table config
   * @param lineageEntry lineage entry
   * @return True if we can safely delete the replaced segments. False otherwise.
   */
  private boolean shouldDeleteReplacedSegments(TableConfig tableConfig, LineageEntry lineageEntry) {
    // TODO: Currently, we preserve the replaced segments for 1 day for REFRESH tables only. Once we support
    // data rollback for APPEND tables, we should remove this check.
    String batchSegmentIngestionType = IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig);
    if (!batchSegmentIngestionType.equalsIgnoreCase("REFRESH")
        || lineageEntry.getTimestamp() < System.currentTimeMillis() - REPLACED_SEGMENTS_RETENTION_IN_MILLIS) {
      return true;
    }
    return false;
  }
}
