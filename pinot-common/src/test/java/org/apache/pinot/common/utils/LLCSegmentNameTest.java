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
}
