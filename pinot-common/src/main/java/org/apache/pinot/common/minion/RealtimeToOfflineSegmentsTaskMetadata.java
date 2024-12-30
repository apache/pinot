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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Metadata for the minion task of type <code>RealtimeToOfflineSegmentsTask</code>. The <code>_windowStartMs</code>
 * denotes the time (exclusive) until which it's certain that tasks have been completed successfully. The
 * <code>_expectedSubtaskResultMap</code> contains the expected RTO tasks result info. This map can contain both
 * completed and in-completed Tasks expected Results. This map is used by generator to validate whether a potential
 * segment (for RTO task) has already been successfully processed as a RTO task in the past or not. The
 * <code>_windowStartMs</code> and <code>_windowEndMs</code> denote the window bucket time of currently not
 * successfully completed minion task. bucket: [_windowStartMs, _windowEndMs) The window is updated by generator when
 * it's certain that prev minon task run is successful.
 * <p>
 * This gets serialized and stored in zookeeper under the path
 * MINION_TASK_METADATA/${tableNameWithType}/RealtimeToOfflineSegmentsTask
 * <p>
 * PinotTaskGenerator: The <code>_windowStartMs</code>> is used by the
 * <code>RealtimeToOfflineSegmentsTaskGenerator</code>, to determine the window of execution of the prev task based on
 * which it generates new task.
 * <p>
 * PinotTaskExecutor: The same windowStartMs is used by the <code>RealtimeToOfflineSegmentsTaskExecutor</code>, to:
 * - Verify that it's running the latest task scheduled by the task generator.
 * - The _expectedSubtaskResultMap is updated before the offline segments are uploaded to the table.
 */
public class RealtimeToOfflineSegmentsTaskMetadata extends BaseTaskMetadata {

  private static final String WINDOW_START_KEY = "watermarkMs";
  private static final String WINDOW_END_KEY = "windowEndMs";
  private static final String COMMA_SEPARATOR = ",";
  private static final String SEGMENT_NAME_TO_EXPECTED_SUBTASK_RESULT_ID_KEY = "segmentToExpectedSubtaskResultId";

  private final String _tableNameWithType;
  private long _windowStartMs;
  private long _windowEndMs;
  private final Map<String, ExpectedSubtaskResult> _expectedSubtaskResultMap;
  private final Map<String, String> _segmentNameToExpectedSubtaskResultID;

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long windowStartMs) {
    _windowStartMs = windowStartMs;
    _tableNameWithType = tableNameWithType;
    _expectedSubtaskResultMap = new HashMap<>();
    _segmentNameToExpectedSubtaskResultID = new HashMap<>();
  }

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long windowStartMs,
      long windowEndMs,
      Map<String, ExpectedSubtaskResult> expectedSubtaskResultMap,
      Map<String, String> segmentNameToExpectedSubtaskResultID) {
    _tableNameWithType = tableNameWithType;
    _windowStartMs = windowStartMs;
    _expectedSubtaskResultMap = expectedSubtaskResultMap;
    _windowEndMs = windowEndMs;
    _segmentNameToExpectedSubtaskResultID = segmentNameToExpectedSubtaskResultID;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public void setWindowStartMs(long windowStartMs) {
    _windowStartMs = windowStartMs;
  }

  public long getWindowStartMs() {
    return _windowStartMs;
  }

  public long getWindowEndMs() {
    return _windowEndMs;
  }

  public void setWindowEndMs(long windowEndMs) {
    _windowEndMs = windowEndMs;
  }

  public Map<String, ExpectedSubtaskResult> getExpectedSubtaskResultMap() {
    return _expectedSubtaskResultMap;
  }

  public Map<String, String> getSegmentNameToExpectedSubtaskResultID() {
    return _segmentNameToExpectedSubtaskResultID;
  }

  public void addExpectedSubTaskResult(
      ExpectedSubtaskResult newExpectedSubtaskResult) {

    List<String> segmentsFrom = newExpectedSubtaskResult.getSegmentsFrom();

    for (String segmentName : segmentsFrom) {
      if (_segmentNameToExpectedSubtaskResultID.containsKey(segmentName)) {
        String prevExpectedSubtaskResultID =
            _segmentNameToExpectedSubtaskResultID.get(segmentName);

        ExpectedSubtaskResult prevExpectedSubtaskResult =
            _expectedSubtaskResultMap.get(prevExpectedSubtaskResultID);

        // check if prevExpectedRealtimeToOfflineSubtaskResult is not null, since it could
        // have been removed in the same minion run previously.
        if (prevExpectedSubtaskResult != null) {
          Preconditions.checkState(prevExpectedSubtaskResult.isTaskFailure(),
              "ExpectedSubtaskResult can only be replaced if it's of a failed task");
        }
      }

      _segmentNameToExpectedSubtaskResultID.put(segmentName,
          newExpectedSubtaskResult.getId());
      _expectedSubtaskResultMap.put(newExpectedSubtaskResult.getId(),
          newExpectedSubtaskResult);
    }
  }

  public static RealtimeToOfflineSegmentsTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long windowStartMs = znRecord.getLongField(WINDOW_START_KEY, 0);
    long windowEndMs = znRecord.getLongField(WINDOW_END_KEY, 0);
    Map<String, ExpectedSubtaskResult> expectedSubtaskResultMap =
        new HashMap<>();
    Map<String, List<String>> listFields = znRecord.getListFields();

    if (listFields != null) {
      for (Map.Entry<String, List<String>> listField : listFields.entrySet()) {
        String expectedSubtaskResultId = listField.getKey();

        List<String> value = listField.getValue();
        Preconditions.checkState(value.size() == 4);

        List<String> segmentsFrom = Arrays.asList(StringUtils.split(value.get(0), COMMA_SEPARATOR));
        List<String> segmentsTo = Arrays.asList(StringUtils.split(value.get(1), COMMA_SEPARATOR));
        String taskID = value.get(2);
        boolean taskFailure = Boolean.parseBoolean(value.get(3));

        expectedSubtaskResultMap.put(expectedSubtaskResultId,
            new ExpectedSubtaskResult(segmentsFrom, segmentsTo, expectedSubtaskResultId, taskID,
                taskFailure)
        );
      }
    }

    Map<String, Map<String, String>> mapFields = znRecord.getMapFields();
    Map<String, String> segmentNameToExpectedSubtaskResultID = new HashMap<>();
    if (mapFields != null) {
      segmentNameToExpectedSubtaskResultID = mapFields.get(SEGMENT_NAME_TO_EXPECTED_SUBTASK_RESULT_ID_KEY);
    }

    return new RealtimeToOfflineSegmentsTaskMetadata(znRecord.getId(), windowStartMs, windowEndMs,
        expectedSubtaskResultMap, segmentNameToExpectedSubtaskResultID);
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WINDOW_START_KEY, _windowStartMs);
    znRecord.setLongField(WINDOW_END_KEY, _windowEndMs);

    for (String expectedSubtaskResultID : _expectedSubtaskResultMap.keySet()) {
      ExpectedSubtaskResult expectedSubtaskResult =
          _expectedSubtaskResultMap.get(expectedSubtaskResultID);

      String segmentsFrom = String.join(COMMA_SEPARATOR, expectedSubtaskResult.getSegmentsFrom());
      String segmentsTo = String.join(COMMA_SEPARATOR, expectedSubtaskResult.getSegmentsTo());
      String taskId = expectedSubtaskResult.getTaskID();
      boolean taskFailure = expectedSubtaskResult.isTaskFailure();

      List<String> listEntry = Arrays.asList(segmentsFrom, segmentsTo, taskId, Boolean.toString(taskFailure));

      String id = expectedSubtaskResult.getId();
      znRecord.setListField(id, listEntry);
    }

    znRecord.setMapField(SEGMENT_NAME_TO_EXPECTED_SUBTASK_RESULT_ID_KEY,
        _segmentNameToExpectedSubtaskResultID);
    return znRecord;
  }
}
