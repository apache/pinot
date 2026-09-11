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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class ColumnDeletionDirtySegmentPredicateTest {
  private static final ColumnDeletionEntry DELETION =
      new ColumnDeletionEntry("city", "del-1", 1_000L, 3, ColumnDeletionState.RECLAIMING);

  @Test
  public void testCreationTimeBeforeEpochIsDirty() {
    SegmentZKMetadata segment = segment("s1", 500L, 99L, Map.of());
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(segment, DELETION, false, null)).isTrue();
  }

  @Test
  public void testCreationTimeAtOrAfterEpochIsClean() {
    SegmentZKMetadata atEpoch = segment("s1", 1_000L, 99L, Map.of());
    SegmentZKMetadata afterEpoch = segment("s2", 1_001L, 99L, Map.of());
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(atEpoch, DELETION, false, null)).isFalse();
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(afterEpoch, DELETION, false, null)).isFalse();
  }

  @Test
  public void testMissingCreationTimeUsesEligibilityTime() {
    SegmentZKMetadata dirty = segment("s1", -1L, 99L,
        Map.of(ColumnDeletionDirtySegmentPredicate.ELIGIBILITY_TIME_KEY, "400"));
    SegmentZKMetadata clean = segment("s2", -1L, 99L,
        Map.of(ColumnDeletionDirtySegmentPredicate.ELIGIBILITY_TIME_KEY, "1500"));
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(dirty, DELETION, false, null)).isTrue();
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(clean, DELETION, false, null)).isFalse();
  }

  @Test
  public void testMissingCreationAndEligibilityIsDirty() {
    SegmentZKMetadata segment = segment("s1", -1L, 99L, Map.of());
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(segment, DELETION, false, null)).isTrue();
  }

  @Test
  public void testRefreshTaskTimeAloneIsNotSuccess() {
    SegmentZKMetadata segment = segment("s1", 500L, 99L,
        Map.of(ColumnDeletionDirtySegmentPredicate.REFRESH_SEGMENT_TASK_TIME_KEY, "2000"));
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(segment, DELETION, false, null)).isTrue();
  }

  @Test
  public void testMatchingDeletionCrcMarkerIsClean() {
    SegmentZKMetadata segment = segment("s1", 500L, 42L,
        Map.of(ColumnDeletionDirtySegmentPredicate.processedCrcKey("del-1"), "42"));
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(segment, DELETION, false, null)).isFalse();
  }

  @Test
  public void testMismatchedDeletionCrcMarkerIsDirty() {
    SegmentZKMetadata segment = segment("s1", 500L, 42L,
        Map.of(ColumnDeletionDirtySegmentPredicate.processedCrcKey("del-1"), "7"));
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(segment, DELETION, false, null)).isTrue();
  }

  @Test
  public void testCrcMarkerWithUnknownCurrentCrcIsDirty() {
    SegmentZKMetadata segment = segment("s1", 500L, -1L,
        Map.of(ColumnDeletionDirtySegmentPredicate.processedCrcKey("del-1"), "-1"));
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(segment, DELETION, false, null)).isTrue();
  }

  @Test
  public void testPhysicalAbsenceProofIsClean() {
    SegmentZKMetadata segment = segment("s1", 500L, 99L, Map.of());
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(segment, DELETION, false, Set.of("city"))).isFalse();
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(segment, DELETION, false, Set.of("other"))).isTrue();
    assertThat(ColumnDeletionDirtySegmentPredicate.isDirty(segment, DELETION, true, Set.of("CITY"))).isFalse();
  }

  @Test
  public void testFindDirtySegmentNamesSkipsReplacedAndComplete() {
    SegmentZKMetadata dirty = segment("dirty", 100L, 1L, Map.of());
    SegmentZKMetadata cleanNew = segment("newSeg", 5_000L, 1L, Map.of());
    SegmentZKMetadata cleaned = segment("cleaned", 100L, 9L,
        Map.of(ColumnDeletionDirtySegmentPredicate.processedCrcKey("del-1"), "9"));

    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    metadata.addEntry(DELETION);
    metadata.addEntry(new ColumnDeletionEntry("zip", "del-2", 1_000L, 3, ColumnDeletionState.COMPLETE));

    List<String> dirtyNames =
        ColumnDeletionDirtySegmentPredicate.findDirtySegmentNames(List.of(dirty, cleanNew, cleaned), metadata, false,
            null);
    assertThat(dirtyNames).containsExactly("dirty");
  }

  @Test
  public void testUnionAcrossActiveDeletions() {
    SegmentZKMetadata cityDirty = segment("s1", 100L, 1L, Map.of());
    SegmentZKMetadata zipDirty = segment("s2", 100L, 1L,
        Map.of(ColumnDeletionDirtySegmentPredicate.processedCrcKey("del-1"), "1"));

    ColumnDeletionMetadata metadata = new ColumnDeletionMetadata("foo_OFFLINE");
    metadata.addEntry(DELETION);
    metadata.addEntry(new ColumnDeletionEntry("zip", "del-2", 1_000L, 3, ColumnDeletionState.FAILED));

    List<String> dirtyNames =
        ColumnDeletionDirtySegmentPredicate.findDirtySegmentNames(List.of(cityDirty, zipDirty), metadata, false, null);
    assertThat(dirtyNames).containsExactly("s1", "s2");
  }

  @Test
  public void testProvenAbsenceBySegment() {
    SegmentZKMetadata s1 = segment("s1", 100L, 1L, Map.of());
    SegmentZKMetadata s2 = segment("s2", 100L, 1L, Map.of());
    Map<String, Set<String>> proven = Map.of("s1", Set.of("city"));

    List<String> dirtyNames =
        ColumnDeletionDirtySegmentPredicate.findDirtySegmentNames(List.of(s1, s2), DELETION, false, proven);
    assertThat(dirtyNames).containsExactly("s2");
  }

  private static SegmentZKMetadata segment(String name, long creationTime, long crc, Map<String, String> customMap) {
    SegmentZKMetadata metadata = new SegmentZKMetadata(name);
    if (creationTime >= 0) {
      metadata.setCreationTime(creationTime);
    }
    if (crc >= 0) {
      metadata.setCrc(crc);
    }
    if (!customMap.isEmpty()) {
      metadata.setCustomMap(new HashMap<>(customMap));
    }
    return metadata;
  }
}
