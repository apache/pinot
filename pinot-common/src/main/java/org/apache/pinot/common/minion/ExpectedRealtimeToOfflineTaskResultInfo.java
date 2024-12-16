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
 */
public class ExpectedRealtimeToOfflineTaskResultInfo {
  private final List<String> _segmentsFrom;
  private final List<String> _segmentsTo;
  private final String _id;
  private final String _taskID;

  public ExpectedRealtimeToOfflineTaskResultInfo(List<String> segmentsFrom, List<String> segmentsTo, String taskID) {
    _segmentsFrom = segmentsFrom;
    _segmentsTo = segmentsTo;
    _taskID = taskID;
    _id = UUID.randomUUID().toString();
  }

  public ExpectedRealtimeToOfflineTaskResultInfo(List<String> segmentsFrom, List<String> segmentsTo,
      String realtimeToOfflineSegmentsMapId, String taskID) {
    _segmentsFrom = segmentsFrom;
    _segmentsTo = segmentsTo;
    _id = realtimeToOfflineSegmentsMapId;
    _taskID = taskID;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExpectedRealtimeToOfflineTaskResultInfo)) {
      return false;
    }
    ExpectedRealtimeToOfflineTaskResultInfo that = (ExpectedRealtimeToOfflineTaskResultInfo) o;
    return Objects.equals(_segmentsFrom, that._segmentsFrom) && Objects.equals(_segmentsTo,
        that._segmentsTo) && Objects.equals(_id, that._id) && Objects.equals(_taskID, that._taskID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentsFrom, _segmentsTo, _id, _taskID);
  }
}
