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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentLineageTest {
  @Test
  public void testSegmentLineage() {
    SegmentLineage segmentLineage = new SegmentLineage("test_OFFLINE");
    String id = SegmentLineageUtils.generateLineageEntryId();
    segmentLineage.addLineageEntry(id,
        new LineageEntry(Arrays.asList("s1", "s2", "s3"), Arrays.asList("s4", "s5"), LineageEntryState.COMPLETED,
            11111L));
    LineageEntry lineageEntry = segmentLineage.getLineageEntry(id);
    Assert.assertEquals(lineageEntry.getSegmentsFrom(), Arrays.asList("s1", "s2", "s3"));
    Assert.assertEquals(lineageEntry.getSegmentsTo(), Arrays.asList("s4", "s5"));
    Assert.assertEquals(lineageEntry.getState(), LineageEntryState.COMPLETED);
    Assert.assertEquals(lineageEntry.getTimestamp(), 11111L);

    String id2 = SegmentLineageUtils.generateLineageEntryId();
    segmentLineage.addLineageEntry(id2,
        new LineageEntry(Arrays.asList("s6", "s6", "s8"), Arrays.asList("s9", "s10"), LineageEntryState.COMPLETED,
            22222L));
    LineageEntry lineageEntry2 = segmentLineage.getLineageEntry(id2);
    Assert.assertEquals(lineageEntry2.getSegmentsFrom(), Arrays.asList("s6", "s6", "s8"));
    Assert.assertEquals(lineageEntry2.getSegmentsTo(), Arrays.asList("s9", "s10"));
    Assert.assertEquals(lineageEntry2.getState(), LineageEntryState.COMPLETED);
    Assert.assertEquals(lineageEntry2.getTimestamp(), 22222L);

    String id3 = SegmentLineageUtils.generateLineageEntryId();
    segmentLineage.addLineageEntry(id3,
        new LineageEntry(Arrays.asList("s5", "s9"), Arrays.asList("s11"), LineageEntryState.IN_PROGRESS, 33333L));
    LineageEntry lineageEntry3 = segmentLineage.getLineageEntry(id3);
    Assert.assertEquals(lineageEntry3.getSegmentsFrom(), Arrays.asList("s5", "s9"));
    Assert.assertEquals(lineageEntry3.getSegmentsTo(), Arrays.asList("s11"));
    Assert.assertEquals(lineageEntry3.getState(), LineageEntryState.IN_PROGRESS);
    Assert.assertEquals(lineageEntry3.getTimestamp(), 33333L);

    String id4 = SegmentLineageUtils.generateLineageEntryId();
    segmentLineage.addLineageEntry(id4,
        new LineageEntry(new ArrayList<>(), Arrays.asList("s12"), LineageEntryState.IN_PROGRESS, 44444L));
    LineageEntry lineageEntry4 = segmentLineage.getLineageEntry(id4);
    Assert.assertEquals(lineageEntry4.getSegmentsFrom(), new ArrayList<>());
    Assert.assertEquals(lineageEntry4.getSegmentsTo(), Arrays.asList("s12"));
    Assert.assertEquals(lineageEntry4.getState(), LineageEntryState.IN_PROGRESS);
    Assert.assertEquals(lineageEntry4.getTimestamp(), 44444L);
    Assert.assertFalse(lineageEntry4.isAutoCompleteLineageEntry());

    // Entry opting into observer-driven completion: should round-trip the flag and emit a 5-tuple
    // in the ZNRecord list field, while the other entries stay 4-tuples.
    String id5 = SegmentLineageUtils.generateLineageEntryId();
    segmentLineage.addLineageEntry(id5,
        new LineageEntry(Arrays.asList("s13", "s14"), Arrays.asList("s15"), LineageEntryState.IN_PROGRESS, 55555L,
            true));
    LineageEntry lineageEntry5 = segmentLineage.getLineageEntry(id5);
    Assert.assertEquals(lineageEntry5.getSegmentsFrom(), Arrays.asList("s13", "s14"));
    Assert.assertEquals(lineageEntry5.getSegmentsTo(), Arrays.asList("s15"));
    Assert.assertEquals(lineageEntry5.getState(), LineageEntryState.IN_PROGRESS);
    Assert.assertEquals(lineageEntry5.getTimestamp(), 55555L);
    Assert.assertTrue(lineageEntry5.isAutoCompleteLineageEntry());

    // Test the convesion from the segment lineage to the znRecord
    ZNRecord znRecord = segmentLineage.toZNRecord();
    Assert.assertEquals(znRecord.getId(), "test_OFFLINE");

    Map<String, List<String>> listFields = znRecord.getListFields();
    List<String> entry = listFields.get(id);
    Assert.assertEquals(entry.get(0), String.join(",", Arrays.asList("s1", "s2", "s3")));
    Assert.assertEquals(entry.get(1), String.join(",", Arrays.asList("s4", "s5")));
    Assert.assertEquals(entry.get(2), LineageEntryState.COMPLETED.toString());
    Assert.assertEquals(entry.get(3), Long.toString(11111L));

    List<String> entry2 = listFields.get(id2);
    Assert.assertEquals(entry2.get(0), String.join(",", Arrays.asList("s6", "s6", "s8")));
    Assert.assertEquals(entry2.get(1), String.join(",", Arrays.asList("s9", "s10")));
    Assert.assertEquals(entry2.get(2), LineageEntryState.COMPLETED.toString());
    Assert.assertEquals(entry2.get(3), Long.toString(22222L));

    List<String> entry3 = listFields.get(id3);
    Assert.assertEquals(entry3.get(0), String.join(",", Arrays.asList("s5", "s9")));
    Assert.assertEquals(entry3.get(1), String.join(",", Arrays.asList("s11")));
    Assert.assertEquals(entry3.get(2), LineageEntryState.IN_PROGRESS.toString());
    Assert.assertEquals(entry3.get(3), Long.toString(33333L));

    List<String> entry4 = listFields.get(id4);
    Assert.assertEquals(entry4.get(0), "");
    Assert.assertEquals(entry4.get(1), String.join(",", Arrays.asList("s12")));
    Assert.assertEquals(entry4.get(2), LineageEntryState.IN_PROGRESS.toString());
    Assert.assertEquals(entry4.get(3), Long.toString(44444L));
    // Default-false entries must stay 4-tuples on the wire to keep old readers happy.
    Assert.assertEquals(entry4.size(), 4);

    List<String> entry5 = listFields.get(id5);
    Assert.assertEquals(entry5.get(0), String.join(",", Arrays.asList("s13", "s14")));
    Assert.assertEquals(entry5.get(1), String.join(",", Arrays.asList("s15")));
    Assert.assertEquals(entry5.get(2), LineageEntryState.IN_PROGRESS.toString());
    Assert.assertEquals(entry5.get(3), Long.toString(55555L));
    Assert.assertEquals(entry5.get(4), Boolean.toString(true));

    // Test the conversion from the znRecord to the segment lineage
    SegmentLineage segmentLineageFromZNRecord = SegmentLineage.fromZNRecord(segmentLineage.toZNRecord());
    Assert.assertEquals(segmentLineageFromZNRecord.getLineageEntry(id), lineageEntry);
    Assert.assertEquals(segmentLineageFromZNRecord.getLineageEntry(id2), lineageEntry2);
    Assert.assertEquals(segmentLineageFromZNRecord.getLineageEntry(id3), lineageEntry3);
    Assert.assertEquals(segmentLineageFromZNRecord.getLineageEntry(id4), lineageEntry4);
    Assert.assertEquals(segmentLineageFromZNRecord.getLineageEntry(id5), lineageEntry5);

    // Try to delete by iterating through the lineage entry ids
    for (String lineageId : segmentLineage.getLineageEntryIds()) {
      segmentLineage.deleteLineageEntry(lineageId);
    }
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 0);
  }

  @Test
  public void teatSegmentLineageEntryEquals() {
    LineageEntry expectedLineageEntry =
        new LineageEntry(Arrays.asList("seg1", "seg2"), Arrays.asList("seg3", "seg4"), LineageEntryState.IN_PROGRESS,
            12345L);
    LineageEntry actualLineageEntry =
        new LineageEntry(Arrays.asList("seg1", "seg2"), Arrays.asList("seg3", "seg4"), LineageEntryState.IN_PROGRESS,
            12345L);
    Assert.assertEquals(actualLineageEntry, expectedLineageEntry);
    actualLineageEntry =
        new LineageEntry(Arrays.asList("seg1", "seg2"), Arrays.asList("seg3", "seg4"), LineageEntryState.IN_PROGRESS,
            12346L);
    Assert.assertNotEquals(actualLineageEntry, expectedLineageEntry);
    actualLineageEntry =
        new LineageEntry(Arrays.asList("seg1"), Arrays.asList("seg3", "seg4"), LineageEntryState.IN_PROGRESS, 12345L);
    Assert.assertNotEquals(actualLineageEntry, expectedLineageEntry);
    actualLineageEntry =
        new LineageEntry(Arrays.asList("seg1"), Arrays.asList("seg3", "seg4"), LineageEntryState.COMPLETED, 12345L);
    Assert.assertNotEquals(actualLineageEntry, expectedLineageEntry);
    actualLineageEntry =
        new LineageEntry(Arrays.asList("seg1"), Arrays.asList("seg3", "seg4"), LineageEntryState.REVERTED, 12345L);
    Assert.assertNotEquals(actualLineageEntry, expectedLineageEntry);

    // Entries that differ only in autoCompleteLineageEntry must not be equal.
    LineageEntry flaggedEntry =
        new LineageEntry(Arrays.asList("seg1", "seg2"), Arrays.asList("seg3", "seg4"), LineageEntryState.IN_PROGRESS,
            12345L, true);
    Assert.assertNotEquals(flaggedEntry, expectedLineageEntry);
  }

  @Test
  public void testLegacyFourTupleReadsAsAutoCompleteFalse() {
    // A ZNRecord written by an older controller (4-tuple list fields) must still parse, with the
    // missing 5th element defaulting to false. Hand-build the ZNRecord to simulate the legacy wire
    // format directly.
    ZNRecord legacy = new ZNRecord("test_OFFLINE");
    legacy.setListField("legacy-1", Arrays.asList("a,b", "c", LineageEntryState.IN_PROGRESS.toString(), "99999"));
    SegmentLineage lineage = SegmentLineage.fromZNRecord(legacy);
    LineageEntry parsed = lineage.getLineageEntry("legacy-1");
    Assert.assertEquals(parsed.getSegmentsFrom(), Arrays.asList("a", "b"));
    Assert.assertEquals(parsed.getSegmentsTo(), Arrays.asList("c"));
    Assert.assertEquals(parsed.getState(), LineageEntryState.IN_PROGRESS);
    Assert.assertEquals(parsed.getTimestamp(), 99999L);
    Assert.assertFalse(parsed.isAutoCompleteLineageEntry());
  }
}
