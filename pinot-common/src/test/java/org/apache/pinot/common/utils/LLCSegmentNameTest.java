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

import org.testng.Assert;
import org.testng.annotations.Test;


public class LLCSegmentNameTest {

  @Test
  public void testCreateLlcSegmentNameUsingFullSegmentName() {
    String segmentName = "tableName__1__2__20211101T1948Z";
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    Assert.assertEquals(llcSegmentName.getSegmentName(), segmentName);
    Assert.assertEquals(llcSegmentName.getSegmentType(), SegmentName.RealtimeSegmentType.LLC);
    Assert.assertEquals(llcSegmentName.getTableName(), "tableName");
    Assert.assertEquals(llcSegmentName.getSequenceNumber(), 2);
    Assert.assertEquals(llcSegmentName.getSequenceNumberStr(), "2");
    Assert.assertEquals(llcSegmentName.getCreationTime(), "20211101T1948Z");
    Assert.assertEquals(llcSegmentName.getCreationTimeMs(), 1635796080000L);
    Assert.assertEquals(llcSegmentName.getPartitionGroupId(), 1);
    Assert.assertEquals(llcSegmentName.getPartitionRange(), "1");
    Assert.assertThrows(RuntimeException.class, () -> llcSegmentName.getGroupId());
  }

  @Test
  public void testCreateLlcSegmentNameUsingSegmentNameParts() {
    LLCSegmentName llcSegmentName = new LLCSegmentName("tableName", 1, 0, 1635796137000L);
    Assert.assertEquals(llcSegmentName.getSegmentName(), "tableName__1__0__20211101T1948Z");
    Assert.assertEquals(llcSegmentName.getSegmentType(), SegmentName.RealtimeSegmentType.LLC);
    Assert.assertEquals(llcSegmentName.getTableName(), "tableName");
    Assert.assertEquals(llcSegmentName.getSequenceNumber(), 0);
    Assert.assertEquals(llcSegmentName.getSequenceNumberStr(), "0");
    Assert.assertEquals(llcSegmentName.getCreationTime(), "20211101T1948Z");
    Assert.assertEquals(llcSegmentName.getCreationTimeMs(), 1635796080000L);
    Assert.assertEquals(llcSegmentName.getPartitionGroupId(), 1);
    Assert.assertEquals(llcSegmentName.getPartitionRange(), "1");
    Assert.assertThrows(RuntimeException.class, () -> llcSegmentName.getGroupId());
  }
}
