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

import java.util.List;
import java.util.Objects;
import java.util.UUID;


/**
 * ExpectedRealtimeOfflineTaskResultInfo is created in
 * {@link org.apache.pinot.plugin.minion.tasks.realtimetoofflinesegments.RealtimeToOfflineSegmentsTaskExecutor}
 * before uploading offline segment(s) to the offline table.
 *
 *  The <code>_segmentsFrom</code> denotes the input RealtimeSegments.
 *  The <code>_segmentsTo</code> denotes the expected offline segemnts.
 *  The <code>_id</code> denotes the unique identifier of object.
 *  The <code>_taskID</code> denotes the minion taskId.
 *  The <code>_taskFailure</code> denotes the failure status of minion task handling the
 *    current ExpectedResult. This is modified in
 *    {@link org.apache.pinot.plugin.minion.tasks.realtimetoofflinesegments.RealtimeToOfflineSegmentsTaskGenerator}
 *    when a prev minion task is failed.
 *
 */
public class ExpectedSubtaskResult {
  private final List<String> _segmentsFrom;
  private final List<String> _segmentsTo;
  private final String _id;
  private final String _taskID;
  private boolean _taskFailure = false;

  public ExpectedSubtaskResult(List<String> segmentsFrom, List<String> segmentsTo, String taskID) {
    _segmentsFrom = segmentsFrom;
    _segmentsTo = segmentsTo;
    _taskID = taskID;
    _id = UUID.randomUUID().toString();
  }

  public ExpectedSubtaskResult(List<String> segmentsFrom, List<String> segmentsTo,
      String realtimeToOfflineSegmentsMapId, String taskID, boolean taskFailure) {
    _segmentsFrom = segmentsFrom;
    _segmentsTo = segmentsTo;
    _id = realtimeToOfflineSegmentsMapId;
    _taskID = taskID;
    _taskFailure = taskFailure;
  }

  public String getTaskID() {
    return _taskID;
  }

  public String getId() {
    return _id;
  }

  public List<String> getSegmentsFrom() {
    return _segmentsFrom;
  }

  public List<String> getSegmentsTo() {
    return _segmentsTo;
  }

  public boolean isTaskFailure() {
    return _taskFailure;
  }

  public void setTaskFailure() {
    _taskFailure = true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExpectedSubtaskResult)) {
      return false;
    }
    ExpectedSubtaskResult that = (ExpectedSubtaskResult) o;
    return Objects.equals(_id, that._id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(_id);
  }
}
