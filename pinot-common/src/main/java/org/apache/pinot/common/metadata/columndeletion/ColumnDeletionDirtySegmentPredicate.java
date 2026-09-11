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


/// Dirty-segment predicate for an explicit column deletion.
///
/// A live segment is dirty for deletion D when any of these hold:
/// <ol>
///   <li>a {@link ColumnDeletionPhysicalOverride} says the logical column, or an OPEN_STRUCT child
///       of it, is still physically present (this overrides {@code ctime} and CRC markers), or</li>
///   <li>the segment predates D's immutable epoch (creation time, or a recorded eligibility time)
///       and has no successful deletion CRC marker, and the caller did not positively prove the
///       logical column absent.</li>
/// </ol>
///
/// Missing metadata is never treated as proof of cleanliness. {@code RefreshSegmentTask.time} is
/// not a successful marker: today's refresh executor can write that key on a no-op skip without
/// changing CRC.
///
/// This helper does not read segment files. Callers that can see physical names pass a
/// {@link ColumnDeletionPhysicalOverride}. It also does not mark
/// {@link ColumnDeletionState#COMPLETE}. An empty {@code liveSegments} list yields an empty dirty
/// set and is not cleanup: deep-store objects and later uploads can still hold the column.
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
      @Nullable ColumnDeletionPhysicalOverride physicalOverride) {
    if (physicalOverride != null && physicalOverride.indicatesPresent(entry.getColumnName(), ignoreCase)) {
      return true;
    }
    if (physicalOverride != null && physicalOverride.provesLogicalColumnAbsent(entry.getColumnName(), ignoreCase)) {
      return false;
    }
    if (!predatesDeletion(segment, entry.getDeletionEpochMs())) {
      return false;
    }
    return !hasSuccessfulRefreshMarker(segment, entry.getDeletionId());
  }

  /// Segment names from {@code liveSegments} that are dirty for {@code entry}, sorted.
  ///
  /// An empty result is not {@link ColumnDeletionState#COMPLETE}, including when
  /// {@code liveSegments} itself is empty.
  public static List<String> findDirtySegmentNames(Collection<SegmentZKMetadata> liveSegments,
      ColumnDeletionEntry entry, boolean ignoreCase,
      @Nullable Map<String, ColumnDeletionPhysicalOverride> physicalOverrideBySegment) {
    Set<String> dirty = new TreeSet<>();
    for (SegmentZKMetadata segment : liveSegments) {
      ColumnDeletionPhysicalOverride override =
          physicalOverrideBySegment == null ? null : physicalOverrideBySegment.get(segment.getSegmentName());
      if (isDirty(segment, entry, ignoreCase, override)) {
        dirty.add(segment.getSegmentName());
      }
    }
    return List.copyOf(dirty);
  }

  /// Union of dirty segment names across every active (non-complete) deletion on the table.
  ///
  /// An empty result is not {@link ColumnDeletionState#COMPLETE}, including when
  /// {@code liveSegments} itself is empty.
  public static List<String> findDirtySegmentNames(Collection<SegmentZKMetadata> liveSegments,
      ColumnDeletionMetadata metadata, boolean ignoreCase,
      @Nullable Map<String, ColumnDeletionPhysicalOverride> physicalOverrideBySegment) {
    Set<String> dirty = new TreeSet<>();
    for (ColumnDeletionEntry entry : metadata.getEntries().values()) {
      if (!entry.blocksReAdd()) {
        continue;
      }
      dirty.addAll(findDirtySegmentNames(liveSegments, entry, ignoreCase, physicalOverrideBySegment));
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
    // RefreshSegmentTask.time is intentionally ignored. A matching CRC for this deletionId is required.
    String markedCrc = customMap.get(processedCrcKey(deletionId));
    if (markedCrc == null) {
      return false;
    }
    long currentCrc = segment.getCrc();
    return currentCrc >= 0 && markedCrc.equals(Long.toString(currentCrc));
  }
}
