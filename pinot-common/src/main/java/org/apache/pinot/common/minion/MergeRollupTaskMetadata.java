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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Metadata for the minion task of type <code>MergeRollupTask</code>.
 * The <code>watermarkMap</code> denotes the time (exclusive) upto which tasks have been executed for the bucket granularity.
 *
 * This gets serialized and stored in zookeeper under the path MINION_TASK_METADATA/MergeRollupTask/tableNameWithType
 */
public class MergeRollupTaskMetadata {

  private static final String WATERMARK_KEY_PREFIX = "watermarkMs_";
  private static final int WATERMARK_KEY_PREFIX_LENGTH = WATERMARK_KEY_PREFIX.length();

  private final String _tableNameWithType;
  // Map from merge level to its watermark
  private final Map<String, Long> _watermarkMap;

  public MergeRollupTaskMetadata(String tableNameWithType, Map<String, Long> watermarkMap) {
    _tableNameWithType = tableNameWithType;
    _watermarkMap = watermarkMap;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  /**
   * Get the watermarkMap in millis
   */
  public Map<String, Long> getWatermarkMap() {
    return _watermarkMap;
  }

  public static MergeRollupTaskMetadata fromZNRecord(ZNRecord znRecord) {
    Map<String, Long> watermarkMap = new HashMap<>();
    Map<String, String> fields = znRecord.getSimpleFields();
    for (Map.Entry<String, String> entry : fields.entrySet()) {
      if (entry.getKey().startsWith(WATERMARK_KEY_PREFIX)) {
        watermarkMap.put(entry.getKey().substring(WATERMARK_KEY_PREFIX_LENGTH), Long.parseLong(entry.getValue()));
      }
    }
    return new MergeRollupTaskMetadata(znRecord.getId(), watermarkMap);
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    for (Map.Entry<String, Long> entry : _watermarkMap.entrySet()) {
      znRecord.setLongField(WATERMARK_KEY_PREFIX + entry.getKey(), entry.getValue());
    }
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
