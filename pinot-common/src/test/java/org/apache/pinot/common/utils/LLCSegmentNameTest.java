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
package org.apache.pinot.common.utils;

import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the realtime segment name builder.
 */
public class LLCSegmentNameTest {

  @Test
  public void testSegmentNameBuilder() {
    LLCSegmentName llcSegmentName = new LLCSegmentName("myTable", 0, 1, 1465508537069L);
    String segmentName = llcSegmentName.getSegmentName();
    assertEquals(segmentName, "myTable__0__1__20160609T2142Z");
    assertTrue(LLCSegmentName.isLLCSegment(segmentName));
    assertEquals(llcSegmentName.getTableName(), "myTable");
    assertEquals(llcSegmentName.getTopicPartitionId().toMultiTopicPinotPartitionId(), 0);
    assertEquals(llcSegmentName.getSequenceNumber(), 1);

    // Invalid segment name
    assertFalse(LLCSegmentName.isLLCSegment("a__abc__1__3__4__54__g__gg___h"));
  }

  @Test
  public void testLLCSegmentName() {
    String tableName = "myTable";
    final int partitionGroupId = 4;
    final int sequenceNumber = 27;
    final long msSinceEpoch = 1466200248000L;
    final String creationTime = "20160617T2150Z";
    final long creationTimeInMs = 1466200200000L;
    final String segmentName = "myTable__4__27__" + creationTime;

    LLCSegmentName segName1 = new LLCSegmentName(tableName, partitionGroupId, sequenceNumber, msSinceEpoch);
    Assert.assertEquals(segName1.getSegmentName(), segmentName);
    Assert.assertEquals(segName1.getTopicPartitionId().toMultiTopicPinotPartitionId(), partitionGroupId);
    Assert.assertEquals(segName1.getCreationTime(), creationTime);
    Assert.assertEquals(segName1.getCreationTimeMs(), creationTimeInMs);
    Assert.assertEquals(segName1.getSequenceNumber(), sequenceNumber);
    Assert.assertEquals(segName1.getTableName(), tableName);

    LLCSegmentName segName2 = new LLCSegmentName(segmentName);
    Assert.assertEquals(segName2.getSegmentName(), segmentName);
    Assert.assertEquals(segName2.getTopicPartitionId().toMultiTopicPinotPartitionId(), partitionGroupId);
    Assert.assertEquals(segName2.getCreationTime(), creationTime);
    Assert.assertEquals(segName2.getCreationTimeMs(), creationTimeInMs);
    Assert.assertEquals(segName2.getSequenceNumber(), sequenceNumber);
    Assert.assertEquals(segName2.getTableName(), tableName);

    Assert.assertEquals(segName1, segName2);

    LLCSegmentName segName3 = new LLCSegmentName(tableName, partitionGroupId + 1, sequenceNumber - 1, msSinceEpoch);
    Assert.assertTrue(segName1.compareTo(segName3) < 0);
    LLCSegmentName segName4 = new LLCSegmentName(tableName, partitionGroupId + 1, sequenceNumber + 1, msSinceEpoch);
    Assert.assertTrue(segName1.compareTo(segName4) < 0);
    LLCSegmentName segName5 = new LLCSegmentName(tableName, partitionGroupId - 1, sequenceNumber + 1, msSinceEpoch);
    Assert.assertTrue(segName1.compareTo(segName5) > 0);
    LLCSegmentName segName6 = new LLCSegmentName(tableName, partitionGroupId, sequenceNumber + 1, msSinceEpoch);
    Assert.assertTrue(segName1.compareTo(segName6) < 0);

    LLCSegmentName segName7 =
        new LLCSegmentName(tableName + "NotGood", partitionGroupId, sequenceNumber + 1, msSinceEpoch);
    try {
      segName1.compareTo(segName7);
      Assert.fail("Not failing when comparing " + segName1.getSegmentName() + " and " + segName7.getSegmentName());
    } catch (Exception e) {
      // expected
    }
    LLCSegmentName[] testSorted = new LLCSegmentName[]{segName3, segName1, segName4, segName5, segName6};
    Arrays.sort(testSorted);
    Assert.assertEquals(testSorted, new LLCSegmentName[]{segName5, segName1, segName6, segName3, segName4});
  }

  @Test
  public void testMultiTopicFormatParsing() {
    String newFormatName = "myTable__1__3__5__20250101T0000Z";
    LLCSegmentName seg = new LLCSegmentName(newFormatName);
    assertEquals(seg.getTableName(), "myTable");
    assertEquals(seg.getTopicPartitionId().getTopicId(), 1);
    assertEquals(seg.getTopicPartitionId().getPartitionId(), 3);
    assertEquals(seg.getSequenceNumber(), 5);
    assertEquals(seg.getCreationTime(), "20250101T0000Z");
    assertTrue(seg.isMultiTopicFormat());
    assertEquals(seg.getSegmentName(), newFormatName);
    assertEquals(seg.getTopicPartitionId().toMultiTopicPinotPartitionId(), 10003);
  }

  @Test
  public void testMultiTopicFormatConstruction() {
    TopicPartitionId tpId = new TopicPartitionId(1, 3);
    LLCSegmentName seg = new LLCSegmentName("myTable", tpId, 5, 1420070400000L, true);
    assertTrue(seg.isMultiTopicFormat());
    assertEquals(seg.getTopicPartitionId().getTopicId(), 1);
    assertEquals(seg.getTopicPartitionId().getPartitionId(), 3);
    assertEquals(seg.getSequenceNumber(), 5);
    assertTrue(seg.getSegmentName().startsWith("myTable__1__3__5__"));

    // Round-trip
    LLCSegmentName parsed = new LLCSegmentName(seg.getSegmentName());
    assertEquals(parsed.getTopicPartitionId(), tpId);
    assertEquals(parsed.getSequenceNumber(), 5);
    assertTrue(parsed.isMultiTopicFormat());
  }

  @Test
  public void testOldFormatConstruction() {
    TopicPartitionId tpId = new TopicPartitionId(7);
    LLCSegmentName seg = new LLCSegmentName("myTable", tpId, 3, 1420070400000L, false);
    assertFalse(seg.isMultiTopicFormat());
    assertEquals(seg.getTopicPartitionId().getPartitionId(), 7);
    assertTrue(seg.getSegmentName().startsWith("myTable__7__3__"));
  }

  @Test
  public void testIsLLCSegmentBothFormats() {
    assertTrue(LLCSegmentName.isLLCSegment("myTable__4__27__20160617T2150Z"));
    assertTrue(LLCSegmentName.isLLCSegment("myTable__1__3__5__20250101T0000Z"));
    assertFalse(LLCSegmentName.isLLCSegment(
        "uploaded__myTable__3__20250101T0000Z__abc123"));
    assertFalse(LLCSegmentName.isLLCSegment("not_a_segment"));
  }

  @Test
  public void testOfDisambiguation() {
    assertNotNull(LLCSegmentName.of("myTable__4__27__20160617T2150Z"));
    assertNotNull(LLCSegmentName.of("myTable__1__3__5__20250101T0000Z"));
    assertNull(LLCSegmentName.of("uploaded__myTable__3__20250101T0000Z__abc123"));
    assertNull(LLCSegmentName.of("not_a_segment"));
  }

  @Test
  public void testGetSequenceNumberBothFormats() {
    assertEquals(LLCSegmentName.getSequenceNumber("myTable__4__27__20160617T2150Z"), 27);
    assertEquals(LLCSegmentName.getSequenceNumber("myTable__1__3__5__20250101T0000Z"), 5);
  }

  @Test
  public void testCreateNextSegmentOldToOld() {
    LLCSegmentName prev = new LLCSegmentName("myTable__3__5__20250101T0000Z");
    LLCSegmentName next = LLCSegmentName.createNextSegment(prev, false, 1420070400000L);
    assertFalse(next.isMultiTopicFormat());
    assertEquals(next.getTopicPartitionId().getPartitionId(), 3);
    assertEquals(next.getSequenceNumber(), 6);
  }

  @Test
  public void testCreateNextSegmentNewToNew() {
    LLCSegmentName prev = new LLCSegmentName("myTable__1__3__5__20250101T0000Z");
    LLCSegmentName next = LLCSegmentName.createNextSegment(prev, true, 1420070400000L);
    assertTrue(next.isMultiTopicFormat());
    assertEquals(next.getTopicPartitionId().getTopicId(), 1);
    assertEquals(next.getTopicPartitionId().getPartitionId(), 3);
    assertEquals(next.getSequenceNumber(), 6);
  }

  @Test
  public void testCreateNextSegmentOldToNew() {
    // Old format with composite partitionId 10003 (topic 1, partition 3), parsed with multi-stream context
    LLCSegmentName prev = new LLCSegmentName("myTable__10003__5__20250101T0000Z", true);
    LLCSegmentName next = LLCSegmentName.createNextSegment(prev, true, 1420070400000L);
    assertTrue(next.isMultiTopicFormat());
    assertEquals(next.getTopicPartitionId().getTopicId(), 1);
    assertEquals(next.getTopicPartitionId().getPartitionId(), 3);
    assertEquals(next.getSequenceNumber(), 6);
    // Map key consistency
    assertEquals(
        prev.getTopicPartitionId().toMultiTopicPinotPartitionId(),
        next.getTopicPartitionId().toMultiTopicPinotPartitionId());
  }

  @Test
  public void testCreateNextSegmentNewToOld() {
    LLCSegmentName prev = new LLCSegmentName("myTable__1__3__5__20250101T0000Z");
    LLCSegmentName next = LLCSegmentName.createNextSegment(prev, false, 1420070400000L);
    assertFalse(next.isMultiTopicFormat());
    assertEquals(next.getTopicPartitionId().getPartitionId(), 10003);
    assertEquals(next.getSequenceNumber(), 6);
    // Map key consistency
    assertEquals(
        prev.getTopicPartitionId().toMultiTopicPinotPartitionId(),
        next.getTopicPartitionId().toMultiTopicPinotPartitionId());
  }

  @Test
  public void testContextAwareParsing() {
    // Old format with hasMultipleStreams=true decomposes composite
    LLCSegmentName withContext = new LLCSegmentName("myTable__10003__5__20250101T0000Z", true);
    assertEquals(withContext.getTopicPartitionId().getTopicId(), 1);
    assertEquals(withContext.getTopicPartitionId().getPartitionId(), 3);
    assertFalse(withContext.isMultiTopicFormat());

    // Old format with hasMultipleStreams=false keeps raw partition
    LLCSegmentName withoutContext = new LLCSegmentName("myTable__10003__5__20250101T0000Z", false);
    assertEquals(withoutContext.getTopicPartitionId().getTopicId(), 0);
    assertEquals(withoutContext.getTopicPartitionId().getPartitionId(), 10003);

    // Small partition ID stays unchanged even with hasMultipleStreams=true
    LLCSegmentName small = new LLCSegmentName("myTable__5__3__20250101T0000Z", true);
    assertEquals(small.getTopicPartitionId().getTopicId(), 0);
    assertEquals(small.getTopicPartitionId().getPartitionId(), 5);

    // New format ignores hasMultipleStreams — always decomposed
    LLCSegmentName newFormat = new LLCSegmentName("myTable__1__3__5__20250101T0000Z", false);
    assertEquals(newFormat.getTopicPartitionId().getTopicId(), 1);
    assertEquals(newFormat.getTopicPartitionId().getPartitionId(), 3);
    assertTrue(newFormat.isMultiTopicFormat());

    // Cross-format consistency: same logical partition gives same TopicPartitionId
    assertEquals(withContext.getTopicPartitionId(),
        new LLCSegmentName("myTable__1__3__7__20250101T0000Z").getTopicPartitionId());
  }
}
