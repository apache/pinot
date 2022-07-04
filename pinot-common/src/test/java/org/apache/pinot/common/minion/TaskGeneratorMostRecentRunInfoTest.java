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
package org.apache.pinot.common.minion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link TaskGeneratorMostRecentRunInfo}
 */
public class TaskGeneratorMostRecentRunInfoTest {
  private static final String TABLE_NAME_WITH_TYPE = "table_OFFLINE";
  private static final String TASK_TYPE = "taskType";

  @Test
  public void testNewInstance() {
    TaskGeneratorMostRecentRunInfo taskGeneratorMostRecentRunInfo =
        TaskGeneratorMostRecentRunInfo.newInstance(TABLE_NAME_WITH_TYPE, TASK_TYPE);
    assertEquals(taskGeneratorMostRecentRunInfo.getTableNameWithType(), TABLE_NAME_WITH_TYPE);
    assertEquals(taskGeneratorMostRecentRunInfo.getTaskType(), TASK_TYPE);
    assertEquals(taskGeneratorMostRecentRunInfo.getVersion(), -1);
    assertTrue(taskGeneratorMostRecentRunInfo.getMostRecentSuccessRunTS().isEmpty());
    assertTrue(taskGeneratorMostRecentRunInfo.getMostRecentErrorRunMessage().isEmpty());

    ZNRecord znRecord = taskGeneratorMostRecentRunInfo.toZNRecord();
    assertEquals(znRecord.getId(),
        TABLE_NAME_WITH_TYPE + TaskGeneratorMostRecentRunInfo.ZNRECORD_DELIMITER + TASK_TYPE);
    assertEquals(znRecord.getListFields().size(), 1);
    assertTrue(znRecord.getListField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_SUCCESS_RUN_TS).isEmpty());
    assertEquals(znRecord.getMapFields().size(), 1);
    assertTrue(znRecord.getMapField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_ERROR_RUN_MESSAGE).isEmpty());
  }

  @Test
  public void testAddSuccessRunTs() {
    TaskGeneratorMostRecentRunInfo taskGeneratorMostRecentRunInfo =
        TaskGeneratorMostRecentRunInfo.newInstance(TABLE_NAME_WITH_TYPE, TASK_TYPE);
    for (long i = 0; i < TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP; i++) {
      taskGeneratorMostRecentRunInfo.addSuccessRunTs(i);
      List<Long> mostRecentSuccessRunTS = taskGeneratorMostRecentRunInfo.getMostRecentSuccessRunTS();
      assertEquals(mostRecentSuccessRunTS.size(), i + 1);
      for (long j = i; j >= 0; j--) {
        assertTrue(mostRecentSuccessRunTS.contains(j));
      }
    }

    // add another 2 timestamp in order
    taskGeneratorMostRecentRunInfo.addSuccessRunTs(TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 10);
    List<Long> mostRecentSuccessRunTS = taskGeneratorMostRecentRunInfo.getMostRecentSuccessRunTS();
    assertEquals(mostRecentSuccessRunTS.size(), TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);
    taskGeneratorMostRecentRunInfo.addSuccessRunTs(TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 20);
    mostRecentSuccessRunTS = taskGeneratorMostRecentRunInfo.getMostRecentSuccessRunTS();
    assertEquals(mostRecentSuccessRunTS.size(), TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);
    for (long i = 2; i < TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP; i++) {
      assertTrue(mostRecentSuccessRunTS.contains(i));
    }
    assertTrue(mostRecentSuccessRunTS.contains((long) TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 10));
    assertTrue(mostRecentSuccessRunTS.contains((long) TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 20));

    // add 0 again should not change the success ts list
    taskGeneratorMostRecentRunInfo.addSuccessRunTs(0);
    mostRecentSuccessRunTS = taskGeneratorMostRecentRunInfo.getMostRecentSuccessRunTS();
    assertEquals(mostRecentSuccessRunTS.size(), TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);
    assertFalse(mostRecentSuccessRunTS.contains(0L));

    // add the one in middle will remove the smallest one
    taskGeneratorMostRecentRunInfo.addSuccessRunTs(TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 15);
    mostRecentSuccessRunTS = taskGeneratorMostRecentRunInfo.getMostRecentSuccessRunTS();
    assertEquals(mostRecentSuccessRunTS.size(), TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);
    // 2 was removed
    assertFalse(mostRecentSuccessRunTS.contains(2L));
    assertTrue(mostRecentSuccessRunTS.contains((long) TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 15));
  }

  @Test
  public void testErrorRunMessage() {
    String errorMessage = "error";
    TaskGeneratorMostRecentRunInfo taskGeneratorMostRecentRunInfo =
        TaskGeneratorMostRecentRunInfo.newInstance(TABLE_NAME_WITH_TYPE, TASK_TYPE);
    for (long i = 0; i < TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP; i++) {
      taskGeneratorMostRecentRunInfo.addErrorRunMessage(i, errorMessage);
      Map<Long, String> mostRecentErrorRunMessage = taskGeneratorMostRecentRunInfo.getMostRecentErrorRunMessage();
      assertEquals(mostRecentErrorRunMessage.size(), i + 1);
      for (long j = i; j >= 0; j--) {
        assertTrue(mostRecentErrorRunMessage.containsKey(j));
      }
    }

    // add another 2 error messages in order
    taskGeneratorMostRecentRunInfo.addErrorRunMessage(TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 10,
        errorMessage);
    Map<Long, String> mostRecentErrorRunMessage = taskGeneratorMostRecentRunInfo.getMostRecentErrorRunMessage();
    assertEquals(mostRecentErrorRunMessage.size(), TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);
    taskGeneratorMostRecentRunInfo.addErrorRunMessage(TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 20,
        errorMessage);
    mostRecentErrorRunMessage = taskGeneratorMostRecentRunInfo.getMostRecentErrorRunMessage();
    assertEquals(mostRecentErrorRunMessage.size(), TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);
    for (long i = 2; i < TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP; i++) {
      assertTrue(mostRecentErrorRunMessage.containsKey(i));
    }
    assertTrue(
        mostRecentErrorRunMessage.containsKey((long) TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 10));
    assertTrue(
        mostRecentErrorRunMessage.containsKey((long) TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 20));

    // add 0 again should not change the success ts list
    taskGeneratorMostRecentRunInfo.addErrorRunMessage(0, errorMessage);
    mostRecentErrorRunMessage = taskGeneratorMostRecentRunInfo.getMostRecentErrorRunMessage();
    assertEquals(mostRecentErrorRunMessage.size(), TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);
    assertFalse(mostRecentErrorRunMessage.containsKey(0L));

    // add the one in middle will remove the smallest one
    taskGeneratorMostRecentRunInfo.addErrorRunMessage(TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 15,
        errorMessage);
    mostRecentErrorRunMessage = taskGeneratorMostRecentRunInfo.getMostRecentErrorRunMessage();
    assertEquals(mostRecentErrorRunMessage.size(), TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);
    // 2 was removed
    assertFalse(mostRecentErrorRunMessage.containsKey(2L));
    assertTrue(
        mostRecentErrorRunMessage.containsKey((long) TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 15));
  }

  @Test
  public void testFromAndToZNRecord() {
    String errorMessage = "error";
    ZNRecord znRecord =
        new ZNRecord(TABLE_NAME_WITH_TYPE + TaskGeneratorMostRecentRunInfo.ZNRECORD_DELIMITER + TASK_TYPE);
    znRecord.setVersion(10);
    List<String> successTsList = new ArrayList<>();
    for (long i = 0; i < TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 5; i++) {
      successTsList.add(Long.toString(i));
    }
    znRecord.setListField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_SUCCESS_RUN_TS, successTsList);
    Map<String, String> errorMessageMap = new HashMap<>();
    for (long i = 0; i < TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 5; i++) {
      errorMessageMap.put(Long.toString(i), errorMessage);
    }
    znRecord.setMapField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_ERROR_RUN_MESSAGE, errorMessageMap);
    // check the TaskGeneratorMostRecentRunInfo converted from the ZNRecord
    TaskGeneratorMostRecentRunInfo taskGeneratorMostRecentRunInfo =
        TaskGeneratorMostRecentRunInfo.fromZNRecord(znRecord, TABLE_NAME_WITH_TYPE, TASK_TYPE);
    assertEquals(taskGeneratorMostRecentRunInfo.getTableNameWithType(), TABLE_NAME_WITH_TYPE);
    assertEquals(taskGeneratorMostRecentRunInfo.getTaskType(), TASK_TYPE);
    assertEquals(taskGeneratorMostRecentRunInfo.getVersion(), 10);
    assertEquals(taskGeneratorMostRecentRunInfo.getMostRecentSuccessRunTS().size(),
        TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);
    assertEquals(taskGeneratorMostRecentRunInfo.getMostRecentErrorRunMessage().size(),
        TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP);

    // convert it back to ZNREcord
    ZNRecord znRecordConverted = taskGeneratorMostRecentRunInfo.toZNRecord();
    ZNRecord expectedZNRecord = new ZNRecord(TASK_TYPE);
    expectedZNRecord.setVersion(10);
    List<String> expectedSuccessTsList = new ArrayList<>();
    for (long i = 5; i < TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 5; i++) {
      expectedSuccessTsList.add(Long.toString(i));
    }
    expectedZNRecord.setListField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_SUCCESS_RUN_TS, expectedSuccessTsList);
    Map<String, String> expectedErrorMessageMap = new HashMap<>();
    for (long i = 5; i < TaskGeneratorMostRecentRunInfo.MAX_NUM_OF_HISTORY_TO_KEEP + 5; i++) {
      expectedErrorMessageMap.put(Long.toString(i), errorMessage);
    }
    expectedZNRecord.setMapField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_ERROR_RUN_MESSAGE, expectedErrorMessageMap);
    assertEquals(znRecordConverted, expectedZNRecord);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFromZNRecordThrows() {
    ZNRecord znRecord = new ZNRecord("random");
    znRecord.setVersion(10);
    TaskGeneratorMostRecentRunInfo.fromZNRecord(znRecord, TABLE_NAME_WITH_TYPE, TASK_TYPE);
  }
}
