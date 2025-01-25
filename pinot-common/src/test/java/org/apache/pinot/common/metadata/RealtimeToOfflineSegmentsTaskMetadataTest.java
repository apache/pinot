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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.minion.RealtimeToOfflineCheckpointCheckPoint;
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
    List<RealtimeToOfflineCheckpointCheckPoint> checkPoints = new ArrayList<>();
    RealtimeToOfflineCheckpointCheckPoint checkPoint =
        new RealtimeToOfflineCheckpointCheckPoint(
            new HashSet<>(Arrays.asList("githubEvents__0__0__20241213T2002Z", "githubEvents__0__0__20241213T2003Z")),
            new HashSet<>(Arrays.asList("githubEventsOffline__0__0__20241213T2002Z",
                "githubEventsOffline__0__0__20241213T2003Z")),
            "1");
    RealtimeToOfflineCheckpointCheckPoint checkPoint1 =
        new RealtimeToOfflineCheckpointCheckPoint(
            new HashSet<>(Arrays.asList("githubEvents__0__0__20241213T2102Z", "githubEvents__0__0__20241213T2203Z")),
            new HashSet<>(Arrays.asList("githubEventsOffline__0__0__20241213T2032Z",
                "githubEventsOffline__0__0__20241213T2403Z")),
            "2");

    checkPoints.add(checkPoint);
    checkPoints.add(checkPoint1);

    RealtimeToOfflineSegmentsTaskMetadata originalMetadata =
        new RealtimeToOfflineSegmentsTaskMetadata("testTable_REALTIME", 1000, 2000, checkPoints);

    ZNRecord znRecord = originalMetadata.toZNRecord();
    assertEquals(znRecord.getId(), "testTable_REALTIME");
    assertEquals(znRecord.getSimpleField("watermarkMs"), "1000");
    assertEquals(znRecord.getSimpleField("windowEndMs"), "2000");
    Map<String, List<String>> listFields = znRecord.getListFields();

    for (String id : listFields.keySet()) {
      List<String> fields = listFields.get(id);
      assertEquals(fields.size(), 4);
      String taskID = fields.get(2);
      boolean taskFailure = Boolean.parseBoolean(fields.get(3));
      assert !taskFailure;

      switch (taskID) {
        case "1":
          assertEquals(fields.get(0), String.join(",", checkPoint.getSegmentsFrom()));
          assertEquals(fields.get(1), String.join(",", checkPoint.getSegmentsTo()));
          break;
        case "2":
          assertEquals(fields.get(0), String.join(",", checkPoint1.getSegmentsFrom()));
          assertEquals(fields.get(1), String.join(",", checkPoint1.getSegmentsTo()));
          break;
        default:
          throw new RuntimeException("invalid taskID");
      }
    }

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(znRecord);

    assert isEqual(realtimeToOfflineSegmentsTaskMetadata, originalMetadata);
  }

  private boolean isEqual(RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata,
      RealtimeToOfflineSegmentsTaskMetadata originalMetadata) {
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getWindowEndMs(), originalMetadata.getWindowEndMs());
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getWindowStartMs(), originalMetadata.getWindowStartMs());
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getTableNameWithType(), originalMetadata.getTableNameWithType());

    originalMetadata.getCheckPoints().sort(Comparator.comparing(RealtimeToOfflineCheckpointCheckPoint::getId));
    realtimeToOfflineSegmentsTaskMetadata.getCheckPoints()
        .sort(Comparator.comparing(RealtimeToOfflineCheckpointCheckPoint::getId));

    for (int checkpointIndex = 0; checkpointIndex < originalMetadata.getCheckPoints().size(); checkpointIndex++) {
      assert isEqual((originalMetadata.getCheckPoints().get(checkpointIndex)),
          realtimeToOfflineSegmentsTaskMetadata.getCheckPoints().get(checkpointIndex));
    }
    return true;
  }

  private boolean isEqual(RealtimeToOfflineCheckpointCheckPoint checkPoint1,
      RealtimeToOfflineCheckpointCheckPoint checkPoint2) {
    return Objects.equals(checkPoint1.getSegmentsFrom(),
        checkPoint2.getSegmentsFrom()) && Objects.equals(
        checkPoint1.getSegmentsTo(),
        checkPoint2.getSegmentsTo()) && Objects.equals(
        checkPoint1.getId(), checkPoint2.getId())
        && Objects.equals(
        checkPoint1.getTaskID(), checkPoint2.getTaskID());
  }
}
