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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Metadata for the minion task of type <code>RealtimeToOfflineSegmentsTask</code>.
 * The <code>_windowStartMs</code> denotes the time (exclusive) until which it's certain that tasks have been
 *   completed successfully.
 * The <code>expectedRealtimeToOfflineSegmentsTaskResultList</code> denotes the expected RTO tasks result info.
 *   This list can contain both completed and in-completed Tasks expected Results. This list is used by
 *   generator to validate whether a potential segment (for RTO task) has already been successfully
 *   processed as a RTO task in the past or not.
 * The <code>_windowStartMs</code> and <code>_windowEndMs</code> denote the window bucket time
 *  of currently not successfully completed minion task.
 *  The window is updated by generator when it's certain that prev minon task run is successful.
 *
 * This gets serialized and stored in zookeeper under the path
 * MINION_TASK_METADATA/${tableNameWithType}/RealtimeToOfflineSegmentsTask
 *
 * PinotTaskGenerator:
 * The <code>watermarkMs</code>> is used by the <code>RealtimeToOfflineSegmentsTaskGenerator</code>,
 * to determine the window of execution of the prev task based on which it generates new task.
 *
 * PinotTaskExecutor:
 * The same watermark is used by the <code>RealtimeToOfflineSegmentsTaskExecutor</code>, to:
 * - Verify that it's running the latest task scheduled by the task generator.
 * - The ExpectedRealtimeToOfflineSegmentsTaskResultList is updated before the offline segments
 *   are uploaded to the table.
 */
public class RealtimeToOfflineSegmentsTaskMetadata extends BaseTaskMetadata {

  private static final String WINDOW_START_KEY = "windowStartMs";
  private static final String WINDOW_END_KEY = "windowEndMs";
  private static final String COMMA_SEPARATOR = ",";

  private final String _tableNameWithType;
  private long _windowStartMs;
  private final List<ExpectedRealtimeToOfflineTaskResultInfo> _expectedRealtimeToOfflineSegmentsTaskResultList;
  private long _windowEndMs;

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long windowStartMs) {
    _windowStartMs = windowStartMs;
    _tableNameWithType = tableNameWithType;
    _expectedRealtimeToOfflineSegmentsTaskResultList = new ArrayList<>();
  }

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long windowStartMs,
      long windowEndMs, List<ExpectedRealtimeToOfflineTaskResultInfo> expectedRealtimeToOfflineSegmentsMapList) {
    _tableNameWithType = tableNameWithType;
    _windowStartMs = windowStartMs;
    _expectedRealtimeToOfflineSegmentsTaskResultList = expectedRealtimeToOfflineSegmentsMapList;
    _windowEndMs = windowEndMs;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public List<ExpectedRealtimeToOfflineTaskResultInfo> getExpectedRealtimeToOfflineSegmentsTaskResultList() {
    return _expectedRealtimeToOfflineSegmentsTaskResultList;
  }

  public void setWindowStartMs(long windowStartMs) {
    _windowStartMs = windowStartMs;
  }

  /**
   * Get the watermark in millis
   */
  public long getWindowStartMs() {
    return _windowStartMs;
  }

  public long getWindowEndMs() {
    return _windowEndMs;
  }

  public void setWindowEndMs(long windowEndMs) {
    _windowEndMs = windowEndMs;
  }

  public static RealtimeToOfflineSegmentsTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long windowStartMs = znRecord.getLongField(WINDOW_START_KEY, 0);
    long windowEndMs = znRecord.getLongField(WINDOW_END_KEY, 0);
    List<ExpectedRealtimeToOfflineTaskResultInfo> expectedRealtimeToOfflineSegmentsMapList = new ArrayList<>();
    Map<String, List<String>> listFields = znRecord.getListFields();
    for (Map.Entry<String, List<String>> listField : listFields.entrySet()) {
      String realtimeToOfflineSegmentsMapId = listField.getKey();
      List<String> value = listField.getValue();
      Preconditions.checkState(value.size() == 3);
      List<String> segmentsFrom = Arrays.asList(StringUtils.split(value.get(0), COMMA_SEPARATOR));
      List<String> segmentsTo = Arrays.asList(StringUtils.split(value.get(1), COMMA_SEPARATOR));
      String taskID = value.get(2);
      expectedRealtimeToOfflineSegmentsMapList.add(
          new ExpectedRealtimeToOfflineTaskResultInfo(segmentsFrom, segmentsTo, realtimeToOfflineSegmentsMapId, taskID)
      );
    }
    return new RealtimeToOfflineSegmentsTaskMetadata(znRecord.getId(), windowStartMs, windowEndMs,
        expectedRealtimeToOfflineSegmentsMapList);
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WINDOW_START_KEY, _windowStartMs);
    znRecord.setLongField(WINDOW_END_KEY, _windowEndMs);
    for (ExpectedRealtimeToOfflineTaskResultInfo realtimeToOfflineSegmentsMap
        : _expectedRealtimeToOfflineSegmentsTaskResultList) {
      String segmentsFrom = String.join(COMMA_SEPARATOR, realtimeToOfflineSegmentsMap.getSegmentsFrom());
      String segmentsTo = String.join(COMMA_SEPARATOR, realtimeToOfflineSegmentsMap.getSegmentsTo());
      String taskId = realtimeToOfflineSegmentsMap.getTaskID();
      String realtimeToOfflineSegmentsMapId = realtimeToOfflineSegmentsMap.getId();
      List<String> listEntry = Arrays.asList(segmentsFrom, segmentsTo, taskId);
      znRecord.setListField(realtimeToOfflineSegmentsMapId, listEntry);
    }
    return znRecord;
  }
}
