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

import java.util.Set;
import java.util.UUID;


/**
 * RealtimeToOfflineCheckpointCheckPoint is created in
 * {@link org.apache.pinot.plugin.minion.tasks.realtimetoofflinesegments.RealtimeToOfflineSegmentsTaskExecutor}
 * before uploading offline segment(s) to the offline table.
 *
 * RealtimeToOfflineCheckpointCheckPoint is ExpectedSubtaskResult.
 *
 *  The <code>_segmentsFrom</code> denotes the input RealtimeSegments.
 *  The <code>_segmentsTo</code> denotes the expected offline segemnts.
 *  The <code>_id</code> denotes the unique identifier of object.
 *  The <code>_taskID</code> denotes the minion taskId.
 *  The <code>_failed</code> denotes the failure status of minion subtask handling the
 *    checkpoint. This is modified in
 *    {@link org.apache.pinot.plugin.minion.tasks.realtimetoofflinesegments.RealtimeToOfflineSegmentsTaskGenerator}
 *    when a prev minion task is failed.
 *
 */
public class RealtimeToOfflineCheckpointCheckPoint {
  private final Set<String> _segmentsFrom;
  private final Set<String> _segmentsTo;
  private final String _id;
  private final String _taskID;
  private boolean _failed = false;

  public RealtimeToOfflineCheckpointCheckPoint(Set<String> segmentsFrom, Set<String> segmentsTo, String taskID) {
    _segmentsFrom = segmentsFrom;
    _segmentsTo = segmentsTo;
    _taskID = taskID;
    _id = UUID.randomUUID().toString();
  }

  public RealtimeToOfflineCheckpointCheckPoint(Set<String> segmentsFrom, Set<String> segmentsTo,
      String id, String taskID, boolean failed) {
    _segmentsFrom = segmentsFrom;
    _segmentsTo = segmentsTo;
    _id = id;
    _taskID = taskID;
    _failed = failed;
  }

  public String getTaskID() {
    return _taskID;
  }

  public String getId() {
    return _id;
  }

  public Set<String> getSegmentsFrom() {
    return _segmentsFrom;
  }

  public Set<String> getSegmentsTo() {
    return _segmentsTo;
  }

  public boolean isFailed() {
    return _failed;
  }

  public void setFailed() {
    _failed = true;
  }
}
