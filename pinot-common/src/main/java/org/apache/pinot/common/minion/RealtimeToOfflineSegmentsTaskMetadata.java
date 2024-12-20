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
 * Metadata for the minion task of type <code>RealtimeToOfflineSegmentsTask</code>.
 * The <code>_windowStartMs</code> denotes the time (exclusive) until which it's certain that tasks have been
 *   completed successfully.
 * The <code>_expectedRealtimeToOfflineSegmentsTaskResultList</code> denotes the expected RTO tasks result info.
 *   This list can contain both completed and in-completed Tasks expected Results. This list is used by
 *   generator to validate whether a potential segment (for RTO task) has already been successfully
 *   processed as a RTO task in the past or not.
 * The <code>_windowStartMs</code> and <code>_windowEndMs</code> denote the window bucket time
 *  of currently not successfully completed minion task. bucket: [_windowStartMs, _windowEndMs)
 *  The window is updated by generator when it's certain that prev minon task run is successful.
 *
 * This gets serialized and stored in zookeeper under the path
 * MINION_TASK_METADATA/${tableNameWithType}/RealtimeToOfflineSegmentsTask
 *
 * PinotTaskGenerator:
 * The <code>_windowStartMs</code>> is used by the <code>RealtimeToOfflineSegmentsTaskGenerator</code>,
 * to determine the window of execution of the prev task based on which it generates new task.
 *
 * PinotTaskExecutor:
 * The same windowStartMs is used by the <code>RealtimeToOfflineSegmentsTaskExecutor</code>, to:
 * - Verify that it's running the latest task scheduled by the task generator.
 * - The ExpectedRealtimeToOfflineSegmentsTaskResultList is updated before the offline segments
 *   are uploaded to the table.
 */
public class RealtimeToOfflineSegmentsTaskMetadata extends BaseTaskMetadata {

  private static final String WINDOW_START_KEY = "watermarkMs";
  private static final String WINDOW_END_KEY = "windowEndMs";
  private static final String COMMA_SEPARATOR = ",";
  private static final String SEGMENT_NAME_VS_EXPECTED_RTO_RESULT_ID_KEY = "segmentVsExpectedRTOResultId";

  private final String _tableNameWithType;
  private long _windowStartMs;
  private long _windowEndMs;
  private final Map<String, ExpectedRealtimeToOfflineTaskResultInfo> _idVsExpectedRealtimeToOfflineTaskResultInfo;
  private final Map<String, String> _segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId;

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long windowStartMs) {
    _windowStartMs = windowStartMs;
    _tableNameWithType = tableNameWithType;
    _idVsExpectedRealtimeToOfflineTaskResultInfo = new HashMap<>();
    _segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId = new HashMap<>();
  }

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long windowStartMs,
      long windowEndMs,
      Map<String, ExpectedRealtimeToOfflineTaskResultInfo> idVsExpectedRealtimeToOfflineTaskResultInfo,
      Map<String, String> segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId) {
    _tableNameWithType = tableNameWithType;
    _windowStartMs = windowStartMs;
    _idVsExpectedRealtimeToOfflineTaskResultInfo = idVsExpectedRealtimeToOfflineTaskResultInfo;
    _windowEndMs = windowEndMs;
    _segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId = segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId;
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

  public Map<String, ExpectedRealtimeToOfflineTaskResultInfo> getIdVsExpectedRealtimeToOfflineTaskResultInfo() {
    return _idVsExpectedRealtimeToOfflineTaskResultInfo;
  }

  public Map<String, String> getSegmentNameVsExpectedRealtimeToOfflineTaskResultInfoId() {
    return _segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId;
  }

  public void addExpectedRealtimeToOfflineSegmentsTaskResultInfo(
      ExpectedRealtimeToOfflineTaskResultInfo newExpectedRealtimeToOfflineTaskResultInfo) {

    List<String> segmentsFrom = newExpectedRealtimeToOfflineTaskResultInfo.getSegmentsFrom();

    for (String segmentName : segmentsFrom) {
      if (_segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId.containsKey(segmentName)) {
        String prevExpectedRealtimeToOfflineTaskResultInfoId =
            _segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId.get(segmentName);

        ExpectedRealtimeToOfflineTaskResultInfo prevExpectedRealtimeToOfflineTaskResultInfo =
            _idVsExpectedRealtimeToOfflineTaskResultInfo.get(prevExpectedRealtimeToOfflineTaskResultInfoId);

        // check if prevExpectedRealtimeToOfflineTaskResultInfo is not null, since it could
        // have been removed in the same minion run previously.
        if (prevExpectedRealtimeToOfflineTaskResultInfo != null) {
          Preconditions.checkState(prevExpectedRealtimeToOfflineTaskResultInfo.isTaskFailure(),
              "ExpectedRealtimeToOfflineSegmentsTaskResult can only be replaced if it's of a failed task");
        }
      }

      _segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId.put(segmentName,
          newExpectedRealtimeToOfflineTaskResultInfo.getId());
      _idVsExpectedRealtimeToOfflineTaskResultInfo.put(newExpectedRealtimeToOfflineTaskResultInfo.getId(),
          newExpectedRealtimeToOfflineTaskResultInfo);
    }
  }

  public static RealtimeToOfflineSegmentsTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long windowStartMs = znRecord.getLongField(WINDOW_START_KEY, 0);
    long windowEndMs = znRecord.getLongField(WINDOW_END_KEY, 0);
    Map<String, ExpectedRealtimeToOfflineTaskResultInfo> idVExpectedRealtimeToOfflineTaskResultInfoList =
        new HashMap<>();
    Map<String, List<String>> listFields = znRecord.getListFields();

    for (Map.Entry<String, List<String>> listField : listFields.entrySet()) {
      String realtimeToOfflineSegmentsMapId = listField.getKey();

      List<String> value = listField.getValue();
      Preconditions.checkState(value.size() == 4);

      List<String> segmentsFrom = Arrays.asList(StringUtils.split(value.get(0), COMMA_SEPARATOR));
      List<String> segmentsTo = Arrays.asList(StringUtils.split(value.get(1), COMMA_SEPARATOR));
      String taskID = value.get(2);
      boolean taskFailure = Boolean.parseBoolean(value.get(3));

      idVExpectedRealtimeToOfflineTaskResultInfoList.put(realtimeToOfflineSegmentsMapId,
          new ExpectedRealtimeToOfflineTaskResultInfo(segmentsFrom, segmentsTo, realtimeToOfflineSegmentsMapId, taskID,
              taskFailure)
      );
    }

    Map<String, Map<String, String>> mapFields = znRecord.getMapFields();
    Map<String, String> segmentNameVsExpectedRTOIDResult = mapFields.get(SEGMENT_NAME_VS_EXPECTED_RTO_RESULT_ID_KEY);

    return new RealtimeToOfflineSegmentsTaskMetadata(znRecord.getId(), windowStartMs, windowEndMs,
        idVExpectedRealtimeToOfflineTaskResultInfoList, segmentNameVsExpectedRTOIDResult);
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WINDOW_START_KEY, _windowStartMs);
    znRecord.setLongField(WINDOW_END_KEY, _windowEndMs);

    for (String expectedRealtimeToOfflineTaskResultInfoId : _idVsExpectedRealtimeToOfflineTaskResultInfo.keySet()) {
      ExpectedRealtimeToOfflineTaskResultInfo expectedRealtimeToOfflineTaskResultInfo =
          _idVsExpectedRealtimeToOfflineTaskResultInfo.get(expectedRealtimeToOfflineTaskResultInfoId);

      String segmentsFrom = String.join(COMMA_SEPARATOR, expectedRealtimeToOfflineTaskResultInfo.getSegmentsFrom());
      String segmentsTo = String.join(COMMA_SEPARATOR, expectedRealtimeToOfflineTaskResultInfo.getSegmentsTo());
      String taskId = expectedRealtimeToOfflineTaskResultInfo.getTaskID();
      boolean taskFailure = expectedRealtimeToOfflineTaskResultInfo.isTaskFailure();

      List<String> listEntry = Arrays.asList(segmentsFrom, segmentsTo, taskId, Boolean.toString(taskFailure));

      String id = expectedRealtimeToOfflineTaskResultInfo.getId();
      znRecord.setListField(id, listEntry);
    }

    znRecord.setMapField(SEGMENT_NAME_VS_EXPECTED_RTO_RESULT_ID_KEY,
        _segmentNameVsExpectedRealtimeToOfflineTaskResultInfoId);
    return znRecord;
  }
}
