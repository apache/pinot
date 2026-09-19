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
import org.apache.pinot.spi.stream.StreamPartitionIdentity;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests for the realtime segment name builder.
public class LLCSegmentNameTest {

  @Test
  public void testSegmentNameBuilder() {
    LLCSegmentName llcSegmentName = new LLCSegmentName("myTable", 0, 1, 1465508537069L);
    String segmentName = llcSegmentName.getSegmentName();
    assertEquals(segmentName, "myTable__0__1__20160609T2142Z");
    assertTrue(LLCSegmentName.isLLCSegment(segmentName));
    assertEquals(llcSegmentName.getTableName(), "myTable");
    assertEquals(llcSegmentName.getPartitionGroupId(), 0);
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
    Assert.assertEquals(segName1.getPartitionGroupId(), partitionGroupId);
    Assert.assertEquals(segName1.getCreationTime(), creationTime);
    Assert.assertEquals(segName1.getCreationTimeMs(), creationTimeInMs);
    Assert.assertEquals(segName1.getSequenceNumber(), sequenceNumber);
    Assert.assertEquals(segName1.getTableName(), tableName);

    LLCSegmentName segName2 = new LLCSegmentName(segmentName);
    Assert.assertEquals(segName2.getSegmentName(), segmentName);
    Assert.assertEquals(segName2.getPartitionGroupId(), partitionGroupId);
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
  public void testV1FormatVersionAndIdentity() {
    LLCSegmentName llcSegmentName = new LLCSegmentName("myTable", 10000, 1, 1465508537069L);
    assertEquals(llcSegmentName.getFormatVersion(), LLCSegmentName.FORMAT_VERSION_V1);
    assertFalse(llcSegmentName.isV2());
    assertEquals(llcSegmentName.getPartitionGroupId(), 10000);
    assertEquals(llcSegmentName.getStreamPartitionIdentity(), StreamPartitionIdentity.v1(10000));
    assertEquals(LLCSegmentName.getSequenceNumber(llcSegmentName.getSegmentName()), 1);
    assertThrows(IllegalStateException.class, llcSegmentName::getTopicId);
    assertThrows(IllegalStateException.class, llcSegmentName::getPartitionId);
  }

  @Test
  public void testV2ParseFormatRoundTrip() {
    long msSinceEpoch = 1465508537069L;
    String segmentName = LLCSegmentName.formatV2("orders", 1, 0, 17, msSinceEpoch);
    assertEquals(segmentName, "orders__v2__1__0__17__20160609T2142Z");
    assertTrue(LLCSegmentName.isLLCSegment(segmentName));
    assertEquals(LLCSegmentName.getSequenceNumber(segmentName), 17);

    LLCSegmentName parsed = new LLCSegmentName(segmentName);
    assertEquals(parsed.getSegmentName(), segmentName);
    assertEquals(parsed.getTableName(), "orders");
    assertEquals(parsed.getFormatVersion(), LLCSegmentName.FORMAT_VERSION_V2);
    assertTrue(parsed.isV2());
    assertEquals(parsed.getTopicId(), 1);
    assertEquals(parsed.getPartitionId(), 0);
    assertEquals(parsed.getSequenceNumber(), 17);
    assertEquals(parsed.getCreationTime(), "20160609T2142Z");
    assertEquals(parsed.getStreamPartitionIdentity(), StreamPartitionIdentity.v2(1, 0));
    assertEquals(LLCSegmentName.of(segmentName), parsed);
    assertThrows(IllegalStateException.class, parsed::getPartitionGroupId);
  }

  @Test
  public void testV2HistoricalCollisionPair() {
    long msSinceEpoch = 1465508537069L;
    LLCSegmentName topic0Large = new LLCSegmentName(LLCSegmentName.formatV2("t", 0, 10000, 0, msSinceEpoch));
    LLCSegmentName topic1Zero = new LLCSegmentName(LLCSegmentName.formatV2("t", 1, 0, 0, msSinceEpoch));
    assertNotEquals(topic0Large.getSegmentName(), topic1Zero.getSegmentName());
    assertNotEquals(topic0Large.getStreamPartitionIdentity(), topic1Zero.getStreamPartitionIdentity());
    assertNotEquals(topic0Large, topic1Zero);
    assertTrue(topic0Large.compareTo(topic1Zero) < 0);

    LLCSegmentName v1Packed = new LLCSegmentName("t", 10000, 0, msSinceEpoch);
    assertTrue(v1Packed.compareTo(topic0Large) < 0);
    assertTrue(v1Packed.compareTo(topic1Zero) < 0);
  }

  @Test
  public void testV2NumericBoundaries() {
    long msSinceEpoch = 1465508537069L;
    for (int partitionId : new int[]{0, 9999, 10000, 10001, Integer.MAX_VALUE}) {
      String name = LLCSegmentName.formatV2("t", 0, partitionId, 3, msSinceEpoch);
      LLCSegmentName parsed = new LLCSegmentName(name);
      assertEquals(parsed.getPartitionId(), partitionId);
      assertEquals(parsed.getTopicId(), 0);
      assertEquals(parsed.getSequenceNumber(), 3);
    }
    LLCSegmentName maxTopic = new LLCSegmentName(LLCSegmentName.formatV2("t", Integer.MAX_VALUE, 0, 0, msSinceEpoch));
    assertEquals(maxTopic.getTopicId(), Integer.MAX_VALUE);
  }

  @Test
  public void testUploadedAndFivePartNamesAreNotLlc() {
    String uploaded = "uploaded__table__0__20220101T0000Z__suffix";
    assertTrue(UploadedRealtimeSegmentName.isUploadedRealtimeSegmentName(uploaded));
    assertFalse(LLCSegmentName.isLLCSegment(uploaded));
    assertNull(LLCSegmentName.of(uploaded));

    // #18830 5-part grammar: no v2 token, same token count as uploaded names.
    String fivePart = "table__1__0__17__20160609T2142Z";
    assertFalse(LLCSegmentName.isLLCSegment(fivePart));
    assertNull(LLCSegmentName.of(fivePart));
  }

  @Test
  public void testV2MalformedNames() {
    assertFalse(LLCSegmentName.isLLCSegment("orders__v3__1__0__17__20160609T2142Z"));
    assertNull(LLCSegmentName.of("orders__v3__1__0__17__20160609T2142Z"));
    assertFalse(LLCSegmentName.isLLCSegment("orders__V2__1__0__17__20160609T2142Z"));
    assertNull(LLCSegmentName.of("orders__v2__1__0__17"));
    assertNull(LLCSegmentName.of("orders__v2__1__0__17__20160609T2142Z__extra"));
    assertTrue(LLCSegmentName.isLLCSegment("orders__v2__x__0__17__20160609T2142Z"));
    assertNull(LLCSegmentName.of("orders__v2__x__0__17__20160609T2142Z"));
    assertNull(LLCSegmentName.of("orders__v2__-1__0__17__20160609T2142Z"));
    assertNull(LLCSegmentName.of("orders__v2__1__-1__17__20160609T2142Z"));
    assertThrows(IllegalArgumentException.class, () -> new LLCSegmentName("orders__v2__-1__0__17__20160609T2142Z"));
    assertThrows(IllegalArgumentException.class, () -> LLCSegmentName.formatV2("bad__table", 0, 0, 0, 0L));
    assertThrows(IllegalArgumentException.class, () -> LLCSegmentName.formatV2("t", -1, 0, 0, 0L));
    assertThrows(IllegalArgumentException.class, () -> LLCSegmentName.formatV2("t", 0, -1, 0, 0L));
    assertThrows(IllegalArgumentException.class, () -> LLCSegmentName.getSequenceNumber(uploadedFivePart()));
  }

  private static String uploadedFivePart() {
    return "uploaded__table__0__20220101T0000Z__suffix";
  }
}
