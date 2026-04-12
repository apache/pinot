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
 * Metadata for the minion task of type {@code MaterializedViewTask}.
 * The {@code watermarkMs} denotes the time (exclusive) up to which tasks have been executed.
 *
 * <p>This gets serialized and stored in ZooKeeper under the path
 * MINION_TASK_METADATA/${tableNameWithType}/MaterializedViewTask
 *
 * <p>PinotTaskGenerator:
 * The {@code watermarkMs} is used by the {@code MaterializedViewTaskGenerator}
 * to determine the execution window: [watermarkMs, watermarkMs + bucketSize).
 *
 * <p>PinotTaskExecutor:
 * The same watermark is used by the {@code MaterializedViewTaskExecutor} to:
 * <ul>
 *   <li>Verify that it is running the latest task scheduled by the generator</li>
 *   <li>Update the watermark to the end of the window upon successful execution</li>
 * </ul>
 */
public class MaterializedViewTaskMetadata extends BaseTaskMetadata {

  private static final String WATERMARK_KEY = "watermarkMs";

  private final String _tableNameWithType;
  private final long _watermarkMs;

  public MaterializedViewTaskMetadata(String tableNameWithType, long watermarkMs) {
    _tableNameWithType = tableNameWithType;
    _watermarkMs = watermarkMs;
  }

  @Override
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public long getWatermarkMs() {
    return _watermarkMs;
  }

  public static MaterializedViewTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long watermark = znRecord.getLongField(WATERMARK_KEY, 0);
    return new MaterializedViewTaskMetadata(znRecord.getId(), watermark);
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WATERMARK_KEY, _watermarkMs);
    return znRecord;
  }
}
