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

import static org.testng.Assert.*;


public class UploadedRealtimeSegmentNameTest {

  @Test
  public void testSegmentNameParsing() {
    String segmentName = "uploaded_table_name_1_2_1234567890";
    UploadedRealtimeSegmentName uploadedSegmentName = new UploadedRealtimeSegmentName(segmentName);

    Assert.assertEquals(uploadedSegmentName.getTableName(), "table_name");
    Assert.assertEquals(uploadedSegmentName.getPartitionId(), 1);
    Assert.assertEquals(uploadedSegmentName.getSequenceId(), 2);
    Assert.assertEquals(uploadedSegmentName.getCreationTime(), 1234567890L);
  }

  @Test
  public void testSegmentNameGeneration() {
    String tableName = "tableName";
    int partitionId = 1;
    int sequenceId = 2;
    long creationTime = 1234567890L;

    UploadedRealtimeSegmentName uploadedSegmentName =
        new UploadedRealtimeSegmentName(tableName, partitionId, sequenceId, creationTime);
    String expectedSegmentName = "uploaded_tableName_1_2_1234567890";

    Assert.assertEquals(uploadedSegmentName.getSegmentName(), expectedSegmentName);
  }
}
