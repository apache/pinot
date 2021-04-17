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
import static org.testng.Assert.fail;


/**
 * Tests for the realtime segment name builder.
 */
public class SegmentNameBuilderTest {
  @Test
  public void testSegmentNameBuilder() {
    final String oldV1Name = "myTable_REALTIME__Server_1.2.3.4_1234__myTable_REALTIME_1234567_0__0__23456789";
    final String groupId = "myTable_REALTIME_1234567_0";
    final String partitionRange = "ALL";
    final String sequenceNumber = "1234567";
    HLCSegmentName shortNameSegment = new HLCSegmentName(groupId, partitionRange, sequenceNumber);
    HLCSegmentName longNameSegment = new HLCSegmentName(oldV1Name);
    final String shortV1Name = shortNameSegment.getSegmentName();
    LLCSegmentName llcSegment = new LLCSegmentName("myTable", 0, 1, 1465508537069L);
    final String v2Name = llcSegment.getSegmentName();

    assertEquals(shortV1Name, "myTable_REALTIME_1234567_0__ALL__1234567");
    assertEquals(v2Name, "myTable__0__1__20160609T2142Z");

    // Check v1/v2 format detection
    assertEquals(SegmentName.getSegmentType(oldV1Name), SegmentName.RealtimeSegmentType.HLC_LONG);
    assertEquals(SegmentName.getSegmentType(shortV1Name), SegmentName.RealtimeSegmentType.HLC_SHORT);
    assertEquals(SegmentName.getSegmentType(v2Name), SegmentName.RealtimeSegmentType.LLC);

    // Check table name
    assertEquals(longNameSegment.getTableName(), "myTable_REALTIME");
    assertEquals(shortNameSegment.getTableName(), "myTable_REALTIME");
    assertEquals(llcSegment.getTableName(), "myTable");

    // Check partition range
    assertEquals(longNameSegment.getPartitionRange(), "0");
    assertEquals(shortNameSegment.getPartitionRange(), "ALL");
    assertEquals(llcSegment.getPartitionGroupId(), 0);

    // Check groupId
    assertEquals(longNameSegment.getGroupId(), "myTable_REALTIME_1234567_0");
    assertEquals(shortNameSegment.getGroupId(), "myTable_REALTIME_1234567_0");
    try {
      llcSegment.getGroupId();
      fail("extractGroupIdName with a v2 name did not throw an exception");
    } catch (Exception e) {
      // Expected
    }

    // Check sequence number
    assertEquals(longNameSegment.getSequenceNumber(), 23456789);
    assertEquals(longNameSegment.getSequenceNumberStr(), "23456789");
    assertEquals(shortNameSegment.getSequenceNumberStr(), "1234567");
    assertEquals(shortNameSegment.getSequenceNumber(), 1234567);
    assertEquals(llcSegment.getSequenceNumber(), 1);

    assertEquals(llcSegment.getSegmentType(), SegmentName.RealtimeSegmentType.LLC);
    assertEquals(longNameSegment.getSegmentType(), SegmentName.RealtimeSegmentType.HLC_LONG);
    assertEquals(shortNameSegment.getSegmentType(), SegmentName.RealtimeSegmentType.HLC_SHORT);

    assertEquals(SegmentName.getSegmentType(llcSegment.getSegmentName()), SegmentName.RealtimeSegmentType.LLC);
    assertEquals(SegmentName.getSegmentType(longNameSegment.getSegmentName()),
        SegmentName.RealtimeSegmentType.HLC_LONG);
    assertEquals(SegmentName.getSegmentType(shortNameSegment.getSegmentName()),
        SegmentName.RealtimeSegmentType.HLC_SHORT);

    // Invalid segment names
    assertEquals(SegmentName.getSegmentType(longNameSegment.getSegmentName() + "__"),
        SegmentName.RealtimeSegmentType.UNSUPPORTED);
    assertEquals(SegmentName.getSegmentType("a__abc__1__3__4__54__g__gg___h"),
        SegmentName.RealtimeSegmentType.UNSUPPORTED);
  }

  @Test
  public void testExceptions() {
    // Nulls should not work
    /*
    try {
      SegmentNameBuilder.Realtime.buildHighLevelConsumerSegmentName(null, null, null);
      fail("Exception not thrown");
    } catch (Exception e) {
      // Expected
    }
    */

    // Double underscores should not work
    try {
      HLCSegmentName segName = new HLCSegmentName("__", "a", "b");
      //      SegmentNameBuilder.Realtime.buildHighLevelConsumerSegmentName("__", "a", "b");
      fail("Exception not thrown");
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void LLCSegNameTest() {
    final String tableName = "myTable";
    final int partitionGroupId = 4;
    final int sequenceNumber = 27;
    final long msSinceEpoch = 1466200248000L;
    final String creationTime = "20160617T2150Z";
    final String segmentName = "myTable__4__27__" + creationTime;

    LLCSegmentName segName1 = new LLCSegmentName(tableName, partitionGroupId, sequenceNumber, msSinceEpoch);
    Assert.assertEquals(segName1.getSegmentName(), segmentName);
    Assert.assertEquals(segName1.getPartitionGroupId(), partitionGroupId);
    Assert.assertEquals(segName1.getCreationTime(), creationTime);
    Assert.assertEquals(segName1.getSequenceNumber(), sequenceNumber);
    Assert.assertEquals(segName1.getTableName(), tableName);

    LLCSegmentName segName2 = new LLCSegmentName(segmentName);
    Assert.assertEquals(segName2.getSegmentName(), segmentName);
    Assert.assertEquals(segName2.getPartitionGroupId(), partitionGroupId);
    Assert.assertEquals(segName2.getCreationTime(), creationTime);
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
    Assert.assertTrue(testSorted[0] == segName5);
    Assert.assertTrue(testSorted[1] == segName1);
    Assert.assertTrue(testSorted[2] == segName6);
    Assert.assertTrue(testSorted[3] == segName3);
    Assert.assertTrue(testSorted[4] == segName4);
  }
}
