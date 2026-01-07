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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Metadata for the minion task of type <code>MaterializedViewTask</code>.
 * The <code>watermarkMs</code> denotes the time (exclusive) upto which tasks have been executed.
 *
 * This gets serialized and stored in zookeeper under the path
 * MINION_TASK_METADATA/MaterializedViewTask/tableNameWithType
 *
 * PinotTaskGenerator:
 * The <code>watermarkMs</code>> is used by the <code>MaterializedViewTaskGenerator</code>,
 * to determine the window of execution for the task it is generating.
 * The window of execution will be [watermarkMs, watermarkMs + bucketSize)
 *
 * PinotTaskExecutor:
 * The same watermark is used by the <code>MaterializedViewTaskExecutor</code>, to:
 * - Verify that is is running the latest task scheduled by the task generator
 * - Update the watermark as the end of the window that it executed for
 */
public class MaterializedViewTaskMetadata extends BaseTaskMetadata {

  private static final String WATERMARK_KEY_PREFIX = "watermarkMs_";
  private static final int WATERMARK_KEY_PREFIX_LENGTH = WATERMARK_KEY_PREFIX.length();

  private final String _tableNameWithType;
  private final Map<String, Long> _watermarkMap;

  public MaterializedViewTaskMetadata(String tableNameWithType, Map<String, Long> watermarkMap) {
    _tableNameWithType = tableNameWithType;
    _watermarkMap = watermarkMap;
  }

  @Override
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  /**
   * Get the watermark in millis
   */
  public Map<String, Long> getWatermarkMap() {
    return _watermarkMap;
  }

  public static MaterializedViewTaskMetadata fromZNRecord(ZNRecord znRecord) {
    Map<String, Long> watermarkMap = new HashMap<>();
    Map<String, String> fields = znRecord.getSimpleFields();
    for (Map.Entry<String, String> entry : fields.entrySet()) {
      if (entry.getKey().startsWith(WATERMARK_KEY_PREFIX)) {
        watermarkMap.put(entry.getKey().substring(WATERMARK_KEY_PREFIX_LENGTH),
            Long.parseLong(entry.getValue()));
      }
    }
    return new MaterializedViewTaskMetadata(znRecord.getId(), watermarkMap);
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    for (Map.Entry<String, Long> entry : _watermarkMap.entrySet()) {
      znRecord.setLongField(WATERMARK_KEY_PREFIX + entry.getKey(), entry.getValue());
    }
    return znRecord;
  }
}
