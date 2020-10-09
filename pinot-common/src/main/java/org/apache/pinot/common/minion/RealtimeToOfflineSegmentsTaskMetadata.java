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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.helix.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Metadata for the minion task of type <code>realtimeToOfflineSegmentsTask</code>.
 * The <code>watermarkMillis</code> denotes the time (exclusive) upto which tasks have been executed.
 *
 * This gets serialized and stored in zookeeper under the path MINION_TASK_METADATA/realtimeToOfflineSegmentsTask/tableNameWithType
 *
 * PinotTaskGenerator:
 * The <code>watermarkMillis</code>> is used by the <code>RealtimeToOfflineSegmentsTaskGenerator</code>,
 * to determine the window of execution for the task it is generating.
 * The window of execution will be [watermarkMillis, watermarkMillis + bucketSize)
 *
 * PinotTaskExecutor:
 * The same watermark is used by the <code>RealtimeToOfflineSegmentsTaskExecutor</code>, to:
 * - Verify that is is running the latest task scheduled by the task generator
 * - Update the watermark as the end of the window that it executed for
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RealtimeToOfflineSegmentsTaskMetadata {

  private static final String WATERMARK_KEY = "watermarkMillis";

  private final String _tableNameWithType;
  private long _watermarkMillis;

  @JsonCreator
  public RealtimeToOfflineSegmentsTaskMetadata(@JsonProperty(value = "tableNameWithType", required = true) String tableNameWithType,
      @JsonProperty(value = "watermarkMillis", required = true) long watermarkMillis) {
    _tableNameWithType = tableNameWithType;
    _watermarkMillis = watermarkMillis;
  }

  @JsonProperty
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  /**
   * Get the watermark in millis
   */
  @JsonProperty
  public long getWatermarkMillis() {
    return _watermarkMillis;
  }

  public void setWatermarkMillis(long watermarkMillis) {
    _watermarkMillis = watermarkMillis;
  }

  public static RealtimeToOfflineSegmentsTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long watermark = znRecord.getLongField(WATERMARK_KEY, 0);
    return new RealtimeToOfflineSegmentsTaskMetadata(znRecord.getId(), watermark);
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WATERMARK_KEY, _watermarkMillis);
    return znRecord;
  }


  public String toJsonString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String toString() {
    return toJsonString();
  }
}
