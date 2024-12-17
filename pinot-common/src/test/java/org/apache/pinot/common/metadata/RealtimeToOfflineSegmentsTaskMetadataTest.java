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
package org.apache.pinot.common.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.minion.ExpectedRealtimeToOfflineTaskResultInfo;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Tests for converting to and from ZNRecord to {@link RealtimeToOfflineSegmentsTaskMetadata}
 */
public class RealtimeToOfflineSegmentsTaskMetadataTest {

  @Test
  public void testToFromZNRecord() {
    RealtimeToOfflineSegmentsTaskMetadata metadata =
        new RealtimeToOfflineSegmentsTaskMetadata("testTable_REALTIME", 1000);
    ZNRecord znRecord = metadata.toZNRecord();
    assertEquals(znRecord.getId(), "testTable_REALTIME");
    assertEquals(znRecord.getSimpleField("windowStartMs"), "1000");

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(znRecord);
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getTableNameWithType(), "testTable_REALTIME");
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getWindowStartMs(), 1000);
  }

  @Test
  public void testToFromZNRecordWithWindowIntervalAndExpectedResults() {
    List<ExpectedRealtimeToOfflineTaskResultInfo> expectedRealtimeToOfflineTaskResultInfoList = new ArrayList<>();
    ExpectedRealtimeToOfflineTaskResultInfo expectedRealtimeToOfflineTaskResultInfo =
        new ExpectedRealtimeToOfflineTaskResultInfo(
            Arrays.asList("githubEvents__0__0__20241213T2002Z", "githubEvents__0__0__20241213T2003Z"),
            Arrays.asList("githubEventsOffline__0__0__20241213T2002Z", "githubEventsOffline__0__0__20241213T2003Z"),
            "1");
    ExpectedRealtimeToOfflineTaskResultInfo expectedRealtimeToOfflineTaskResultInfo1 =
        new ExpectedRealtimeToOfflineTaskResultInfo(
            Arrays.asList("githubEvents__0__0__20241213T2102Z", "githubEvents__0__0__20241213T2203Z"),
            Arrays.asList("githubEventsOffline__0__0__20241213T2032Z", "githubEventsOffline__0__0__20241213T2403Z"),
            "2");
    expectedRealtimeToOfflineTaskResultInfoList.add(expectedRealtimeToOfflineTaskResultInfo);
    expectedRealtimeToOfflineTaskResultInfoList.add(expectedRealtimeToOfflineTaskResultInfo1);

    RealtimeToOfflineSegmentsTaskMetadata metadata =
        new RealtimeToOfflineSegmentsTaskMetadata("testTable_REALTIME", 1000, 2000,
            expectedRealtimeToOfflineTaskResultInfoList);
    ZNRecord znRecord = metadata.toZNRecord();
    assertEquals(znRecord.getId(), "testTable_REALTIME");
    assertEquals(znRecord.getSimpleField("windowStartMs"), "1000");
    assertEquals(znRecord.getSimpleField("windowEndMs"), "2000");
    Map<String, List<String>> listFields = znRecord.getListFields();

    for (String id : listFields.keySet()) {
      List<String> fields = listFields.get(id);
      assertEquals(fields.size(), 3);
      String taskID = fields.get(2);
      switch (taskID) {
        case "1":
          assertEquals(fields.get(0), "githubEvents__0__0__20241213T2002Z,githubEvents__0__0__20241213T2003Z");
          assertEquals(fields.get(1),
              "githubEventsOffline__0__0__20241213T2002Z,githubEventsOffline__0__0__20241213T2003Z");
          break;
        case "2":
          assertEquals(fields.get(0), "githubEvents__0__0__20241213T2102Z,githubEvents__0__0__20241213T2203Z");
          assertEquals(fields.get(1),
              "githubEventsOffline__0__0__20241213T2032Z,githubEventsOffline__0__0__20241213T2403Z");
          break;
        default:
          throw new RuntimeException("invalid taskID");
      }
    }

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(znRecord);
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getTableNameWithType(), "testTable_REALTIME");
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getWindowStartMs(), 1000);
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getWindowEndMs(), 2000);
    List<ExpectedRealtimeToOfflineTaskResultInfo> expectedRealtimeToOfflineTaskResultInfoList1 =
        realtimeToOfflineSegmentsTaskMetadata.getExpectedRealtimeToOfflineSegmentsTaskResultList();

    for (ExpectedRealtimeToOfflineTaskResultInfo expectedRealtimeToOfflineTaskResultInfo2
        : expectedRealtimeToOfflineTaskResultInfoList1) {
      String taskID = expectedRealtimeToOfflineTaskResultInfo2.getTaskID();

      switch (taskID) {
        case "1":
          assert isEqual(expectedRealtimeToOfflineTaskResultInfo2, expectedRealtimeToOfflineTaskResultInfo);
          break;
        case "2":
          assert isEqual(expectedRealtimeToOfflineTaskResultInfo2, expectedRealtimeToOfflineTaskResultInfo1);
          break;
        default:
          throw new RuntimeException("invalid taskID");
      }
    }
  }

  private boolean isEqual(ExpectedRealtimeToOfflineTaskResultInfo expectedRealtimeToOfflineTaskResultInfo1,
      ExpectedRealtimeToOfflineTaskResultInfo expectedRealtimeToOfflineTaskResultInfo2) {
    return Objects.equals(expectedRealtimeToOfflineTaskResultInfo1.getSegmentsFrom(),
        expectedRealtimeToOfflineTaskResultInfo2.getSegmentsFrom()) && Objects.equals(
        expectedRealtimeToOfflineTaskResultInfo1.getSegmentsTo(),
        expectedRealtimeToOfflineTaskResultInfo2.getSegmentsTo()) && Objects.equals(
        expectedRealtimeToOfflineTaskResultInfo1.getId(), expectedRealtimeToOfflineTaskResultInfo2.getId())
        && Objects.equals(
        expectedRealtimeToOfflineTaskResultInfo1.getTaskID(), expectedRealtimeToOfflineTaskResultInfo2.getTaskID());
  }
}
