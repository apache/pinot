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
 * Helper methods to fetch/persist ZNRecord for task generator info
 */
public final class TaskGeneratorInfoUtils {
  private TaskGeneratorInfoUtils() {
  }
  /**
   * Fetches the {@link ZNRecord} for the given tableName and taskType, from
   * MINION_TASK_GENERATOR_INFO/tableNameWthType/taskName
   *
   * @param propertyStore the {@link HelixPropertyStore} to fetch {@link ZNRecord} from
   * @param tableNameWithType the table name with type
   * @param taskType the task type
   * @return the {@link ZNRecord}
   */
  @Nullable
  public static ZNRecord fetchTaskGeneratorInfo(HelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      String taskType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForTaskGeneratorInfo(tableNameWithType, taskType);
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord != null) {
      znRecord.setVersion(stat.getVersion());
    }
    return znRecord;
  }

  /**
   * Persists the {@link BaseTaskGeneratorInfo} for the given tableName and taskType, to
   * MINION_TASK_GENERATOR_INFO/tableNameWthType/taskName
   *
   * @param propertyStore the {@link HelixPropertyStore} to save {@link BaseTaskGeneratorInfo} into
   * @param tableNameWithType the table name with type
   * @param taskGeneratorInfo the {@link BaseTaskGeneratorInfo}
   * @param expectedVersion the expected {@link ZNRecord} version
   */
  public static void persistTaskGeneratorInfo(HelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      BaseTaskGeneratorInfo taskGeneratorInfo, int expectedVersion) {
    String path = ZKMetadataProvider.constructPropertyStorePathForTaskGeneratorInfo(tableNameWithType,
        taskGeneratorInfo.getTaskType());
    if (!propertyStore.set(path, taskGeneratorInfo.toZNRecord(), expectedVersion, AccessOption.PERSISTENT)) {
      throw new ZkException(
          "Failed to persist task generator info for task: " + taskGeneratorInfo.getClass() + " and info: "
              + taskGeneratorInfo);
    }
  }

  /**
   * Delete the {@link ZNRecord} for the given tableName and taskTyp, from
   * MINION_TASK_GENERATOR_INFO/tableNameWthType/taskName
   *
   * @param propertyStore the {@link HelixPropertyStore} to delete
   * @param tableNameWithType the table name with type
   * @param taskType the task type
   */
  public static void deleteTaskGeneratorInfo(HelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      String taskType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForTaskGeneratorInfo(tableNameWithType, taskType);
    if (!propertyStore.remove(path, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to delete task generator info: " + tableNameWithType + ", " + taskType);
    }
  }
}
