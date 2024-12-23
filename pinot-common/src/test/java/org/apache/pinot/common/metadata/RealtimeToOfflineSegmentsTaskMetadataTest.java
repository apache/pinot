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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.minion.ExpectedSubtaskResult;
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
    assertEquals(znRecord.getSimpleField("watermarkMs"), "1000");

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(znRecord);
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getTableNameWithType(), "testTable_REALTIME");
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getWindowStartMs(), 1000);
  }

  @Test
  public void testToFromZNRecordWithWindowIntervalAndExpectedResults() {
    Map<String, ExpectedSubtaskResult> idVsExpectedRealtimeToOfflineTaskResult =
        new HashMap<>();
    ExpectedSubtaskResult expectedSubtaskResult =
        new ExpectedSubtaskResult(
            Arrays.asList("githubEvents__0__0__20241213T2002Z", "githubEvents__0__0__20241213T2003Z"),
            Arrays.asList("githubEventsOffline__0__0__20241213T2002Z", "githubEventsOffline__0__0__20241213T2003Z"),
            "1");
    ExpectedSubtaskResult expectedSubtaskResult1 =
        new ExpectedSubtaskResult(
            Arrays.asList("githubEvents__0__0__20241213T2102Z", "githubEvents__0__0__20241213T2203Z"),
            Arrays.asList("githubEventsOffline__0__0__20241213T2032Z", "githubEventsOffline__0__0__20241213T2403Z"),
            "2");
    idVsExpectedRealtimeToOfflineTaskResult.put(expectedSubtaskResult.getId(),
        expectedSubtaskResult);
    idVsExpectedRealtimeToOfflineTaskResult.put(expectedSubtaskResult1.getId(),
        expectedSubtaskResult1);

    ImmutableMap<String, String> segmentNameVsId = ImmutableMap.of(
        "githubEvents__0__0__20241213T2002Z", expectedSubtaskResult.getId(),
        "githubEvents__0__0__20241213T2003Z", expectedSubtaskResult.getId(),
        "githubEvents__0__0__20241213T2102Z", expectedSubtaskResult1.getId(),
        "githubEvents__0__0__20241213T2203Z", expectedSubtaskResult1.getId()
    );

    RealtimeToOfflineSegmentsTaskMetadata originalMetadata =
        new RealtimeToOfflineSegmentsTaskMetadata("testTable_REALTIME", 1000, 2000,
            idVsExpectedRealtimeToOfflineTaskResult, segmentNameVsId);

    ZNRecord znRecord = originalMetadata.toZNRecord();
    assertEquals(znRecord.getId(), "testTable_REALTIME");
    assertEquals(znRecord.getSimpleField("watermarkMs"), "1000");
    assertEquals(znRecord.getSimpleField("windowEndMs"), "2000");
    Map<String, List<String>> listFields = znRecord.getListFields();
    Map<String, Map<String, String>> mapFields = znRecord.getMapFields();

    for (String id : listFields.keySet()) {
      List<String> fields = listFields.get(id);
      assertEquals(fields.size(), 4);
      String taskID = fields.get(2);
      boolean taskFailure = Boolean.parseBoolean(fields.get(3));
      assert !taskFailure;

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

    Map<String, String> map = mapFields.get("segmentToExpectedSubtaskResultId");
    assertEquals(map, segmentNameVsId);

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(znRecord);

    assert isEqual(realtimeToOfflineSegmentsTaskMetadata, originalMetadata);
  }

  private boolean isEqual(RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata,
      RealtimeToOfflineSegmentsTaskMetadata originalMetadata) {
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getWindowEndMs(), originalMetadata.getWindowEndMs());
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getWindowStartMs(), originalMetadata.getWindowStartMs());
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getTableNameWithType(), originalMetadata.getTableNameWithType());

    Map<String, ExpectedSubtaskResult> idVsExpectedRealtimeToOfflineTaskResult =
        realtimeToOfflineSegmentsTaskMetadata.getExpectedSubtaskResultMap();
    Map<String, String> segmentNameVsExpectedRealtimeToOfflineTaskResultId =
        realtimeToOfflineSegmentsTaskMetadata.getSegmentNameToExpectedSubtaskResultID();

    for (String id : idVsExpectedRealtimeToOfflineTaskResult.keySet()) {
      ExpectedSubtaskResult actualExpectedRealtimeToOfflineTaskResult =
          idVsExpectedRealtimeToOfflineTaskResult.get(id);
      ExpectedSubtaskResult expectedSubtaskResult =
          originalMetadata.getExpectedSubtaskResultMap().get(id);
      assert expectedSubtaskResult != null;
      assert isEqual(actualExpectedRealtimeToOfflineTaskResult, expectedSubtaskResult);
    }

    assertEquals(segmentNameVsExpectedRealtimeToOfflineTaskResultId,
        originalMetadata.getSegmentNameToExpectedSubtaskResultID());

    return true;
  }

  private boolean isEqual(ExpectedSubtaskResult expectedSubtaskResult1,
      ExpectedSubtaskResult expectedSubtaskResult2) {
    return Objects.equals(expectedSubtaskResult1.getSegmentsFrom(),
        expectedSubtaskResult2.getSegmentsFrom()) && Objects.equals(
        expectedSubtaskResult1.getSegmentsTo(),
        expectedSubtaskResult2.getSegmentsTo()) && Objects.equals(
        expectedSubtaskResult1.getId(), expectedSubtaskResult2.getId())
        && Objects.equals(
        expectedSubtaskResult1.getTaskID(), expectedSubtaskResult2.getTaskID());
  }
}
