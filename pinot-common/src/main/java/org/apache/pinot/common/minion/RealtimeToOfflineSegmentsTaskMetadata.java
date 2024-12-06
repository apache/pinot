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

  private final String _tableNameWithType;
  private long _watermarkMs;
  private int _numSubtasks;

  public RealtimeToOfflineSegmentsTaskMetadata(String tableNameWithType, long watermarkMs) {
    _tableNameWithType = tableNameWithType;
    _watermarkMs = watermarkMs;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public int getNumSubtasks() {
    return _numSubtasks;
  }

  public void setNumSubtasks(int numSubtasks) {
    _numSubtasks = numSubtasks;
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

  public static RealtimeToOfflineSegmentsTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long watermark = znRecord.getLongField(WATERMARK_KEY, 0);
    return new RealtimeToOfflineSegmentsTaskMetadata(znRecord.getId(), watermark);
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WATERMARK_KEY, _watermarkMs);
    return znRecord;
  }
}
