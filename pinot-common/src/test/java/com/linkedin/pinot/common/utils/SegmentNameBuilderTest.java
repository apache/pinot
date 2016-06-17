/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import org.testng.Assert;
import org.testng.annotations.Test;
import static com.linkedin.pinot.common.utils.SegmentNameBuilder.Realtime.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


/**
 * Tests for the realtime segment name builder.
 */
public class SegmentNameBuilderTest {
  @Test
  public void testSegmentNameBuilder() {
    String oldV1Name = "myTable_REALTIME__Server_1.2.3.4_1234__myTable_REALTIME_1234567_0__0__23456789";
    String shortV1Name = SegmentNameBuilder.Realtime.buildHighLevelConsumerSegmentName("myTable_REALTIME_1234567_0", "ALL", "1234567");
    LLCSegmentNameHolder holder = new LLCSegmentNameHolder("myTable", 0, 1, 1465508537069L);
    final String v2Name = holder.getSegmentName();

    assertEquals(shortV1Name, "myTable_REALTIME_1234567_0__ALL__1234567");
    assertEquals(v2Name, "myTable__0__1__20160609T2142Z");

    // Check v1/v2 format detection
    assertEquals(isRealtimeV1Name(oldV1Name), true);
    assertEquals(isRealtimeV1Name(shortV1Name), true);
    assertEquals(isRealtimeV1Name(v2Name), false);

    assertEquals(isRealtimeV2Name(oldV1Name), false);
    assertEquals(isRealtimeV2Name(shortV1Name), false);
    assertEquals(isRealtimeV2Name(v2Name), true);

    // Check table name
    assertEquals(extractTableName(oldV1Name), "myTable_REALTIME");
    assertEquals(extractTableName(shortV1Name), "myTable_REALTIME");
    assertEquals(holder.getTableName(), "myTable");

    // Check partition range
    assertEquals(extractPartitionRange(oldV1Name), "0");
    assertEquals(extractPartitionRange(shortV1Name), "ALL");
    assertEquals(holder.getPartitionId(), 0);

    // Check groupId
    assertEquals(extractGroupIdName(oldV1Name), "myTable_REALTIME_1234567_0");
    assertEquals(extractGroupIdName(shortV1Name), "myTable_REALTIME_1234567_0");
    try {
      extractGroupIdName(v2Name);
      fail("extractGroupIdName with a v2 name did not throw an exception");
    } catch (Exception e) {
      // Expected
    }

    // Check sequence number
    assertEquals(extractSequenceNumber(oldV1Name), "23456789");
    assertEquals(extractSequenceNumber(shortV1Name), "1234567");
    assertEquals(holder.getSequenceNumber(), 1);
  }

  @Test
  public void testExceptions() {
    // Nulls should not work
    try {
      SegmentNameBuilder.Realtime.buildHighLevelConsumerSegmentName(null, null, null);
      fail("Exception not thrown");
    } catch (Exception e) {
      // Expected
    }

    // Double underscores should not work
    try {
      SegmentNameBuilder.Realtime.buildHighLevelConsumerSegmentName("__", "a", "b");
      fail("Exception not thrown");
    } catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void LLCHolderTest() {
    final String tableName = "myTable";
    final int partitionId = 4;
    final int sequenceNumber = 27;
    final long msSinceEpoch = 1466200248000L;
    final String creationTime = "20160617T2150Z";
    final String segmentName = "myTable__4__27__" + creationTime;

    LLCSegmentNameHolder holder1 = new LLCSegmentNameHolder(tableName, partitionId, sequenceNumber, msSinceEpoch);
    Assert.assertEquals(holder1.getSegmentName(), segmentName);
    Assert.assertEquals(holder1.getPartitionId(), partitionId);
    Assert.assertEquals(holder1.getCreationTime(), creationTime);
    Assert.assertEquals(holder1.getSequenceNumber(), sequenceNumber);
    Assert.assertEquals(holder1.getTableName(), tableName);

    LLCSegmentNameHolder holder2 = new LLCSegmentNameHolder(segmentName);
    Assert.assertEquals(holder2.getSegmentName(), segmentName);
    Assert.assertEquals(holder2.getPartitionId(), partitionId);
    Assert.assertEquals(holder2.getCreationTime(), creationTime);
    Assert.assertEquals(holder2.getSequenceNumber(), sequenceNumber);
    Assert.assertEquals(holder2.getTableName(), tableName);

    Assert.assertEquals(holder1, holder2);

    LLCSegmentNameHolder holder3 = new LLCSegmentNameHolder(tableName, partitionId+1, sequenceNumber-1, msSinceEpoch);
    Assert.assertTrue(holder1.compareTo(holder3) < 0);
    LLCSegmentNameHolder holder4 = new LLCSegmentNameHolder(tableName, partitionId+1, sequenceNumber+1, msSinceEpoch);
    Assert.assertTrue(holder1.compareTo(holder4) < 0);
    LLCSegmentNameHolder holder5 = new LLCSegmentNameHolder(tableName, partitionId-1, sequenceNumber+1, msSinceEpoch);
    Assert.assertTrue(holder1.compareTo(holder5) > 0);
    LLCSegmentNameHolder holder6 = new LLCSegmentNameHolder(tableName, partitionId, sequenceNumber+1, msSinceEpoch);
    Assert.assertTrue(holder1.compareTo(holder6) < 0);

    LLCSegmentNameHolder holder7 = new LLCSegmentNameHolder(tableName+"NotGood", partitionId, sequenceNumber+1, msSinceEpoch);
    try {
      holder1.compareTo(holder7);
      Assert.fail("Not failing when comparing " + holder1.getSegmentName() + " and " + holder7.getSegmentName());
    } catch (Exception e) {
      // expected
    }
  }
}
