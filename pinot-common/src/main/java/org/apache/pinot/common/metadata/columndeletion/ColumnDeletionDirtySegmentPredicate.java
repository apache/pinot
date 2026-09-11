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
package org.apache.pinot.common.metadata.columndeletion;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.data.SchemaDiff;


/// Dirty-segment predicate for an explicit column deletion.
///
/// A live segment is dirty for deletion D when:
/// <ol>
///   <li>it predates D's immutable epoch (segment creation time, or a recorded eligibility time), and</li>
///   <li>it has no successful deletion refresh marker whose CRC matches the current segment CRC, and</li>
///   <li>cheap physical-absence metadata, if supplied, does not prove the logical column is gone.</li>
/// </ol>
///
/// Missing metadata is never treated as proof of cleanliness. {@code RefreshSegmentTask.time} alone
/// is not a successful marker because today's refresh executor can write that key on a no-op skip
/// without changing CRC.
///
/// Callers pass only live segments (IdealState / ZK metadata still present, not replaced). This
/// helper does not consult IdealState itself. In-flight tasks that could still install an older CRC
/// are also the caller's responsibility.
///
/// Thread-safe: the helper is stateless.
public final class ColumnDeletionDirtySegmentPredicate {
  /// Existing refresh-task processed-time key. Not sufficient by itself to mark a segment clean.
  public static final String REFRESH_SEGMENT_TASK_TIME_KEY = "RefreshSegmentTask.time";
  /// Optional custom-map key used when {@link SegmentZKMetadata#getCreationTime()} is missing.
  public static final String ELIGIBILITY_TIME_KEY = "ColumnDeletion.eligibilityTime";

  private static final String DELETION_CRC_KEY_PREFIX = "ColumnDeletion.";
  private static final String DELETION_CRC_KEY_SUFFIX = ".crc";

  private ColumnDeletionDirtySegmentPredicate() {
  }

  /// Custom-map key that records the CRC installed for a successful deletion refresh.
  public static String processedCrcKey(String deletionId) {
    return DELETION_CRC_KEY_PREFIX + deletionId + DELETION_CRC_KEY_SUFFIX;
  }

  /// True when {@code segment} still needs reclamation for {@code entry}.
  public static boolean isDirty(SegmentZKMetadata segment, ColumnDeletionEntry entry, boolean ignoreCase,
      @Nullable Set<String> columnsProvenAbsent) {
    if (isColumnProvenAbsent(entry.getColumnName(), ignoreCase, columnsProvenAbsent)) {
      return false;
    }
    if (!predatesDeletion(segment, entry.getDeletionEpochMs())) {
      return false;
    }
    return !hasSuccessfulRefreshMarker(segment, entry.getDeletionId());
  }

  /// Segment names from {@code liveSegments} that are dirty for {@code entry}, sorted.
  public static List<String> findDirtySegmentNames(Collection<SegmentZKMetadata> liveSegments,
      ColumnDeletionEntry entry, boolean ignoreCase, @Nullable Map<String, Set<String>> provenAbsentColumnsBySegment) {
    Set<String> dirty = new TreeSet<>();
    for (SegmentZKMetadata segment : liveSegments) {
      Set<String> provenAbsent =
          provenAbsentColumnsBySegment == null ? null : provenAbsentColumnsBySegment.get(segment.getSegmentName());
      if (isDirty(segment, entry, ignoreCase, provenAbsent)) {
        dirty.add(segment.getSegmentName());
      }
    }
    return List.copyOf(dirty);
  }

  /// Union of dirty segment names across every active (non-complete) deletion on the table.
  public static List<String> findDirtySegmentNames(Collection<SegmentZKMetadata> liveSegments,
      ColumnDeletionMetadata metadata, boolean ignoreCase,
      @Nullable Map<String, Set<String>> provenAbsentColumnsBySegment) {
    Set<String> dirty = new TreeSet<>();
    for (ColumnDeletionEntry entry : metadata.getEntries().values()) {
      if (!entry.blocksReAdd()) {
        continue;
      }
      dirty.addAll(findDirtySegmentNames(liveSegments, entry, ignoreCase, provenAbsentColumnsBySegment));
    }
    return List.copyOf(dirty);
  }

  static boolean predatesDeletion(SegmentZKMetadata segment, long deletionEpochMs) {
    long eligibilityTimeMs = segment.getCreationTime();
    if (eligibilityTimeMs < 0) {
      Map<String, String> customMap = segment.getCustomMap();
      if (customMap != null) {
        String recorded = customMap.get(ELIGIBILITY_TIME_KEY);
        if (recorded != null) {
          eligibilityTimeMs = Long.parseLong(recorded);
        }
      }
    }
    if (eligibilityTimeMs < 0) {
      // Missing ctime and eligibility time cannot prove the segment postdates the deletion.
      return true;
    }
    return eligibilityTimeMs < deletionEpochMs;
  }

  static boolean hasSuccessfulRefreshMarker(SegmentZKMetadata segment, String deletionId) {
    Map<String, String> customMap = segment.getCustomMap();
    if (customMap == null) {
      return false;
    }
    String markedCrc = customMap.get(processedCrcKey(deletionId));
    if (markedCrc == null) {
      return false;
    }
    long currentCrc = segment.getCrc();
    return currentCrc >= 0 && markedCrc.equals(Long.toString(currentCrc));
  }

  private static boolean isColumnProvenAbsent(String columnName, boolean ignoreCase,
      @Nullable Set<String> columnsProvenAbsent) {
    if (columnsProvenAbsent == null || columnsProvenAbsent.isEmpty()) {
      return false;
    }
    for (String provenAbsent : columnsProvenAbsent) {
      if (SchemaDiff.columnNamesEqual(columnName, provenAbsent, ignoreCase)) {
        return true;
      }
    }
    return false;
  }
}
