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
 * The <code>watermarkMs</code> denotes the time (exclusive) upto which tasks have been executed.
 *
 * This gets serialized and stored in zookeeper under the path
 * MINION_TASK_METADATA/${tableNameWithType}/RealtimeToOfflineSegmentsTask
 *
 * PinotTaskGenerator:
 * The <code>watermarkMs</code>> is used by the <code>RealtimeToOfflineSegmentsTaskGenerator</code>,
 * to determine the window of execution for the task it is generating.
 * The window of execution will be [watermarkMs, watermarkMs + bucketSize)
 *
 * PinotTaskExecutor:
 * The same watermark is used by the <code>RealtimeToOfflineSegmentsTaskExecutor</code>, to:
 * - Verify that is is running the latest task scheduled by the task generator
 * - Update the watermark as the end of the window that it executed for
 */
public class RealtimeToOfflineSegmentsTaskMetadata extends BaseTaskMetadata {

  private static final String WATERMARK_KEY = "watermarkMs";
  private static final String TASK_ID_KEY = "taskID";
  private static final String COMMA_SEPARATOR = ",";

  private final String _tableNameWithType;
  private long _watermarkMs;
  private final List<RealtimeToOfflineSegmentsMap> _expectedRealtimeToOfflineSegmentsMapList;
  private final String _taskId;

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long watermarkMs) {
    _watermarkMs = watermarkMs;
    _tableNameWithType = tableNameWithType;
    _taskId = null;
    _expectedRealtimeToOfflineSegmentsMapList = new ArrayList<>();
  }

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long watermarkMs, String taskId) {
    _tableNameWithType = tableNameWithType;
    _watermarkMs = watermarkMs;
    _expectedRealtimeToOfflineSegmentsMapList = new ArrayList<>();
    _taskId = taskId;
  }

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long watermarkMs,
      String taskId, List<RealtimeToOfflineSegmentsMap> expectedRealtimeToOfflineSegmentsMapList) {
    _tableNameWithType = tableNameWithType;
    _watermarkMs = watermarkMs;
    _expectedRealtimeToOfflineSegmentsMapList = expectedRealtimeToOfflineSegmentsMapList;
    _taskId = taskId;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public List<RealtimeToOfflineSegmentsMap> getExpectedRealtimeToOfflineSegmentsMapList() {
    return _expectedRealtimeToOfflineSegmentsMapList;
  }

  public void setWatermarkMs(long watermarkMs) {
    _watermarkMs = watermarkMs;
  }

  /**
   * Get the watermark in millis
   */
  public long getWatermarkMs() {
    return _watermarkMs;
  }

  public String getTaskId() {
    return _taskId;
  }

  public static RealtimeToOfflineSegmentsTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long watermark = znRecord.getLongField(WATERMARK_KEY, 0);
    String taskID = znRecord.getSimpleField(TASK_ID_KEY);
    List<RealtimeToOfflineSegmentsMap> expectedRealtimeToOfflineSegmentsMapList = new ArrayList<>();
    Map<String, List<String>> listFields = znRecord.getListFields();
    for (Map.Entry<String, List<String>> listField : listFields.entrySet()) {
      String subtaskID = listField.getKey();
      List<String> value = listField.getValue();
      Preconditions.checkState(value.size() == 2);
      List<String> segmentsFrom = Arrays.asList(StringUtils.split(value.get(0), COMMA_SEPARATOR));
      List<String> segmentsTo = Arrays.asList(StringUtils.split(value.get(1), COMMA_SEPARATOR));
      expectedRealtimeToOfflineSegmentsMapList.add(
          new RealtimeToOfflineSegmentsMap(segmentsFrom, segmentsTo, subtaskID));
    }
    return new RealtimeToOfflineSegmentsTaskMetadata(znRecord.getId(), watermark,
        taskID, expectedRealtimeToOfflineSegmentsMapList);
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WATERMARK_KEY, _watermarkMs);
    znRecord.setSimpleField(TASK_ID_KEY, _taskId);
    for (RealtimeToOfflineSegmentsMap realtimeToOfflineSegmentsMap : _expectedRealtimeToOfflineSegmentsMapList) {
      String segmentsFrom = String.join(COMMA_SEPARATOR, realtimeToOfflineSegmentsMap.getSegmentsFrom());
      String segmentsTo = String.join(COMMA_SEPARATOR, realtimeToOfflineSegmentsMap.getSegmentsTo());
      String subtaskID = realtimeToOfflineSegmentsMap.getSubtaskId();
      List<String> listEntry = Arrays.asList(segmentsFrom, segmentsTo);
      znRecord.setListField(subtaskID, listEntry);
    }
    return znRecord;
  }

  public static class RealtimeToOfflineSegmentsMap {
    private final List<String> _segmentsFrom;
    private final List<String> _segmentsTo;
    private final String _subtaskId;

    public RealtimeToOfflineSegmentsMap(List<String> segmentsFrom, List<String> segmentsTo, String subtaskId) {
      _segmentsFrom = segmentsFrom;
      _segmentsTo = segmentsTo;
      _subtaskId = subtaskId;
    }

    public String getSubtaskId() {
      return _subtaskId;
    }

    public List<String> getSegmentsFrom() {
      return _segmentsFrom;
    }

    public List<String> getSegmentsTo() {
      return _segmentsTo;
    }
  }
}
