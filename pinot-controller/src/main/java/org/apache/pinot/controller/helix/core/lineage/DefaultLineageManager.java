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
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultLineageManager implements LineageManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLineageManager.class);
  // Default grace window before replaced (segmentsFrom) segments are dropped, used when the table does not
  // configure replacedSegmentsRetentionPeriod. REFRESH tables keep a longer window because their replaced
  // segments are the only copy of the previous snapshot and may be needed for a lineage rollback; other
  // (e.g. APPEND) tables only need enough of a window for brokers to catch up on the latest lineage/IS/EV.
  private static final long REFRESH_REPLACED_SEGMENTS_RETENTION_IN_MILLIS = TimeUnit.HOURS.toMillis(24L); // 24 hrs
  private static final long DEFAULT_REPLACED_SEGMENTS_RETENTION_IN_MILLIS = TimeUnit.HOURS.toMillis(4L); // 4 hrs
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
    String tableNameWithType = tableConfig.getTableName();
    long lineageCleanupRetentionMs = getRetentionMsFromConfig(
        tableConfig.getValidationConfig().getLineageEntryCleanupRetentionPeriod(),
        LINEAGE_ENTRY_CLEANUP_RETENTION_IN_MILLIS, tableNameWithType, "lineageEntryCleanupRetentionPeriod");
    // When replacedSegmentsRetentionPeriod is configured it is honored as-is for every table type. When it is
    // absent we fall back to a longer default for REFRESH tables (their replaced segments back a potential
    // lineage rollback) and a shorter default for other table types (only a broker-catchup window is needed).
    String batchSegmentIngestionType = IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig);
    long defaultReplacedSegmentsRetentionMs = batchSegmentIngestionType.equalsIgnoreCase("REFRESH")
        ? REFRESH_REPLACED_SEGMENTS_RETENTION_IN_MILLIS : DEFAULT_REPLACED_SEGMENTS_RETENTION_IN_MILLIS;
    long replacedSegmentsRetentionMs = getRetentionMsFromConfig(
        tableConfig.getValidationConfig().getReplacedSegmentsRetentionPeriod(),
        defaultReplacedSegmentsRetentionMs, tableNameWithType, "replacedSegmentsRetentionPeriod");
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
          if (shouldDeleteReplacedSegments(lineageEntry, replacedSegmentsRetentionMs)) {
            segmentsToDelete.addAll(sourceSegments);
          }
        }
      } else if (lineageEntry.getState() == LineageEntryState.REVERTED || (
          lineageEntry.getState() == LineageEntryState.IN_PROGRESS && lineageEntry.getTimestamp()
              < System.currentTimeMillis() - lineageCleanupRetentionMs)) {
        // If the lineage state is 'IN_PROGRESS' or 'REVERTED', we need to clean up the zombie lineage
        // entry and its segments
        Set<String> destinationSegments = new HashSet<>(lineageEntry.getSegmentsTo());
        destinationSegments.retainAll(segmentsForTable);
        if (destinationSegments.isEmpty()) {
          // If the lineage state is 'IN_PROGRESS or REVERTED' and the destination segments are no longer in the
          // ideal state, it is safe to clean up the lineage entry. Deleting lineage will allow the task scheduler
          // to re-schedule the source segments to be merged again.
          // A destination segment may still have a znode lingering in the property store even though it never
          // reached (or has already left) the ideal state — e.g. a crash between creating the destination metadata
          // and the ideal-state update. Schedule the destination segments for deletion so such orphans are reaped
          // along with the lineage entry. Names that have no znode are a no-op in the deletion path.
          segmentsToDelete.addAll(lineageEntry.getSegmentsTo());
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
   * The replaced segments are safe to delete once the lineage entry has been in "COMPLETED" state for longer
   * than {@code replacedSegmentsRetentionMs}. The retention applies uniformly to all batch ingestion types —
   * any replacement protocol (REFRESH-table snapshot replace, APPEND-table minion-driven replace,
   * segment-group merge) gets the same configurable grace window before its replaced segments are dropped.
   * The window is configurable via {@code replacedSegmentsRetentionPeriod} in table config; when it is unset
   * the default is resolved per ingestion type by the caller (longer for REFRESH, shorter for others).
   *
   * @param lineageEntry lineage entry
   * @param replacedSegmentsRetentionMs configured retention in ms for replaced segments
   * @return True if we can safely delete the replaced segments. False otherwise.
   */
  private boolean shouldDeleteReplacedSegments(LineageEntry lineageEntry, long replacedSegmentsRetentionMs) {
    // Strict < means a 0ms retention won't delete on the exact same millisecond; this is intentional to
    // avoid edge-case races and is consistent with the existing behavior for non-zero retention values.
    return lineageEntry.getTimestamp() < (System.currentTimeMillis() - replacedSegmentsRetentionMs);
  }

  private static long getRetentionMsFromConfig(@Nullable String period, long defaultMs, String tableNameWithType,
      String configFieldName) {
    if (!StringUtils.isEmpty(period)) {
      try {
        long ms = TimeUtils.convertPeriodToMillis(period);
        if (ms <= 0) {
          LOGGER.debug("Retention period '{}' for config field '{}' resolves to {}ms for table: {}: cleanup will run "
              + "immediately with no rollback window", period, configFieldName, ms, tableNameWithType);
        }
        return ms;
      } catch (Exception e) {
        LOGGER.warn("Unable to parse retention period '{}' for config field '{}' on table: {}, using default: {}ms",
            period, configFieldName, tableNameWithType, defaultMs);
      }
    }
    return defaultMs;
  }
}
