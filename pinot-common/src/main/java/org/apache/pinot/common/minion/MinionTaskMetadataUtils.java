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

import javax.annotation.Nullable;
import org.I0Itec.zkclient.exception.ZkException;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;


/**
 * Helper methods to fetch/persist ZNRecord for minion task metadata
 */
public final class MinionTaskMetadataUtils {

  private MinionTaskMetadataUtils() {

  }

  /**
   * Fetches the ZNRecord for the given minion task and tableName, from MINION_TASK_METADATA/taskName/tableNameWthType
   */
  @Nullable
  public static ZNRecord fetchTaskMetadata(HelixPropertyStore<ZNRecord> propertyStore, String taskType,
      String tableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadata(taskType, tableNameWithType);
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord != null) {
      znRecord.setVersion(stat.getVersion());
    }
    return znRecord;
  }

  /**
   * Deletes the ZNRecord for the given minion task and tableName, from MINION_TASK_METADATA/taskName/tableNameWthType
   */
  public static void deleteTaskMetadata(HelixPropertyStore<ZNRecord> propertyStore, String taskType,
      String tableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadata(taskType, tableNameWithType);
    if (!propertyStore.remove(path, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to delete task metadata: " + taskType + ", " + tableNameWithType);
    }
  }

  /**
   * Generic method for persisting {@link BaseTaskMetadata} to MINION_TASK_METADATA. The metadata will be saved in the
   * ZNode under the path: /MINION_TASK_METADATA/${taskType}/${tableNameWithType}
   *
   * Will fail if expectedVersion does not match.
   * Set expectedVersion -1 to override version check.
   */
  public static void persistTaskMetadata(HelixPropertyStore<ZNRecord> propertyStore, String taskType,
      BaseTaskMetadata taskMetadata, int expectedVersion) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadata(taskType,
        taskMetadata.getTableNameWithType());
    if (!propertyStore
        .set(path, taskMetadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT)) {
      throw new ZkException(
          "Failed to persist minion metadata for task: " + taskType + " and metadata: " + taskMetadata);
    }
  }
}
