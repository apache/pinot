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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Metadata for the minion task of type <code>RealtimeToOfflineSegmentsTask</code>. The <code>_windowStartMs</code>
 * denotes the time (exclusive) until which it's certain that tasks have been completed successfully. The
 * <code>_checkPoints</code> contains the expected RTO tasks result info. This map can contain both
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
 * - The _checkPoints is updated before the offline segments are uploaded to the table.
 */
public class RealtimeToOfflineSegmentsTaskMetadata extends BaseTaskMetadata {

  private static final String WINDOW_START_KEY = "watermarkMs";
  private static final String WINDOW_END_KEY = "windowEndMs";
  private static final String COMMA_SEPARATOR = ",";

  private final String _tableNameWithType;
  private long _windowStartMs;
  private long _windowEndMs;
  private final List<RealtimeToOfflineCheckpointCheckPoint> _checkPoints;

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long windowStartMs) {
    _windowStartMs = windowStartMs;
    _tableNameWithType = tableNameWithType;
    _checkPoints = new ArrayList<>();
  }

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long windowStartMs, long windowEndMs,
      List<RealtimeToOfflineCheckpointCheckPoint> checkPoints) {
    _windowStartMs = windowStartMs;
    _windowEndMs = windowEndMs;
    _tableNameWithType = tableNameWithType;
    _checkPoints = checkPoints;
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

  public List<RealtimeToOfflineCheckpointCheckPoint> getCheckPoints() {
    return _checkPoints;
  }

  public void addCheckpoint(RealtimeToOfflineCheckpointCheckPoint newCheckPoint) {
    if (canAddCheckpoint(newCheckPoint)) {
      _checkPoints.add(newCheckPoint);
    }
  }

  private boolean canAddCheckpoint(RealtimeToOfflineCheckpointCheckPoint newCheckPoint) {
    Set<String> segmentsFrom = newCheckPoint.getSegmentsFrom();
    for (String segmentName : segmentsFrom) {
      for (RealtimeToOfflineCheckpointCheckPoint checkPoint : _checkPoints) {
        if (checkPoint.isFailed()) {
          continue;
        }
        Set<String> prevSegmentsFrom = checkPoint.getSegmentsFrom();
        Preconditions.checkState(!prevSegmentsFrom.contains(segmentName),
            "A live Checkpoints already exists for segment: " + segmentName);
      }
    }
    return true;
  }

  public static RealtimeToOfflineSegmentsTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long windowStartMs = znRecord.getLongField(WINDOW_START_KEY, 0);
    long windowEndMs = znRecord.getLongField(WINDOW_END_KEY, 0);

    Map<String, List<String>> listFields = znRecord.getListFields();
    List<RealtimeToOfflineCheckpointCheckPoint> checkPoints = new ArrayList<>();

    if (listFields != null) {
      for (Map.Entry<String, List<String>> listField : listFields.entrySet()) {
        String checkpointID = listField.getKey();

        List<String> value = listField.getValue();
        Preconditions.checkState(value.size() == 4);

        Set<String> segmentsFrom = new HashSet<>(Arrays.asList(StringUtils.split(value.get(0), COMMA_SEPARATOR)));
        Set<String> segmentsTo = new HashSet<>(Arrays.asList(StringUtils.split(value.get(1), COMMA_SEPARATOR)));
        String taskID = value.get(2);
        boolean isFailedCheckpoint = Boolean.parseBoolean(value.get(3));

        checkPoints.add(new RealtimeToOfflineCheckpointCheckPoint(segmentsFrom, segmentsTo, checkpointID, taskID,
            isFailedCheckpoint));
      }
    }

    return new RealtimeToOfflineSegmentsTaskMetadata(znRecord.getId(), windowStartMs, windowEndMs, checkPoints);
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WINDOW_START_KEY, _windowStartMs);
    znRecord.setLongField(WINDOW_END_KEY, _windowEndMs);

    for (RealtimeToOfflineCheckpointCheckPoint checkPoint : _checkPoints) {
      String segmentsFrom = String.join(COMMA_SEPARATOR, checkPoint.getSegmentsFrom());
      String segmentsTo = String.join(COMMA_SEPARATOR, checkPoint.getSegmentsTo());
      String taskId = checkPoint.getTaskID();
      boolean isFailedCheckpoint = checkPoint.isFailed();
      String id = checkPoint.getId();

      List<String> listEntry = Arrays.asList(segmentsFrom, segmentsTo, taskId, Boolean.toString(isFailedCheckpoint));
      znRecord.setListField(id, listEntry);
    }

    return znRecord;
  }
}
