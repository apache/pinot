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
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;


/**
 * Helper methods to fetch/persist ZNRecord for minion task metadata
 */
public final class MinionTaskMetadataUtils {

  private MinionTaskMetadataUtils() {
  }

  /**
   * Fetches the minion task metadata ZNRecord for the given minion task and tableName. Fetch from the new path
   * MINION_TASK_METADATA/${tableNameWthType}/{taskType} if it exists; otherwise, fetch from the old path
   * MINION_TASK_METADATA/${taskType}/${tableNameWthType}.
   */
  @Nullable
  public static ZNRecord fetchTaskMetadata(HelixPropertyStore<ZNRecord> propertyStore, String taskType,
      String tableNameWithType) {
    String newPath = ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadata(tableNameWithType, taskType);
    if (propertyStore.exists(newPath, AccessOption.PERSISTENT)) {
      return fetchTaskMetadata(propertyStore, newPath);
    } else {
      return fetchTaskMetadata(propertyStore,
          ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadataDeprecated(taskType, tableNameWithType));
    }
  }

  @Nullable
  private static ZNRecord fetchTaskMetadata(HelixPropertyStore<ZNRecord> propertyStore, String path) {
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord != null) {
      znRecord.setVersion(stat.getVersion());
    }
    return znRecord;
  }

  /**
   * Deletes the minion task metadata ZNRecord for the given minion task and tableName, from both the new path
   * MINION_TASK_METADATA/${tableNameWthType}/${taskType} and the old path
   * MINION_TASK_METADATA/${taskType}/${tableNameWthType}.
   */
  public static void deleteTaskMetadata(HelixPropertyStore<ZNRecord> propertyStore, String taskType,
      String tableNameWithType) {
    String newPath = ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadata(tableNameWithType, taskType);
    String oldPath =
        ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadataDeprecated(taskType, tableNameWithType);
    boolean newPathDeleted = propertyStore.remove(newPath, AccessOption.PERSISTENT);
    boolean oldPathDeleted = propertyStore.remove(oldPath, AccessOption.PERSISTENT);
    if (!newPathDeleted || !oldPathDeleted) {
      throw new ZkException("Failed to delete task metadata: " + taskType + ", " + tableNameWithType);
    }
  }

  /**
   * Deletes the minion task metadata ZNRecord for the given tableName, from both the new path
   * MINION_TASK_METADATA/${tableNameWthType} and the old path
   * MINION_TASK_METADATA/<any task type>/${tableNameWthType}
   */
  public static void deleteTaskMetadata(HelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType) {
    // delete the minion task metadata ZNRecord MINION_TASK_METADATA/${tableNameWthType}
    String path = ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadata(tableNameWithType);
    if (!propertyStore.remove(path, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to delete task metadata for table: " + tableNameWithType);
    }
    // delete the minion task metadata ZNRecord MINION_TASK_METADATA/<any task type>/${tableNameWthType}
    // TODO: another way of finding old minion task metadata path is: (1) use reflection to find all task types,
    //   similar to what TaskGeneratorRegistry.java does (2) construct possible old minion task metadata path
    //   using those types.
    //   The tradeoff is: (1) the current approach uses ZK as the source of truth, so we will not miss any ZNode
    //   (2) the other approach will reduce ZK load if there are thousands of tables, because we need to talk to
    //   the ZK to find all its direct children in the current approach.
    List<String> childNames =
        propertyStore.getChildNames(ZKMetadataProvider.getPropertyStorePathForMinionTaskMetadataPrefix(),
            AccessOption.PERSISTENT);
    if (childNames != null && !childNames.isEmpty()) {
      for (String child : childNames) {
        // Even though some child names are not task types (e.g., in the new metadata path, the child name
        // is a table name), it does not harm to try to delete the non-existent constructed path.
        String oldPath =
            ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadataDeprecated(child, tableNameWithType);
        if (!propertyStore.remove(oldPath, AccessOption.PERSISTENT)) {
          throw new ZkException("Failed to delete task metadata: " + child + ", " + tableNameWithType);
        }
      }
    }
  }

  /**
   * Generic method for persisting {@link BaseTaskMetadata} to MINION_TASK_METADATA. The metadata will
   * be saved in the ZNode under the new path /MINION_TASK_METADATA/${tableNameWithType}/${taskType} if
   * it exists or the old path does not exist; otherwise, it will be saved in the ZNode under the old path
   * /MINION_TASK_METADATA/${taskType}/${tableNameWithType}.
   *
   * Will fail if expectedVersion does not match.
   * Set expectedVersion -1 to override version check.
   */
  public static void persistTaskMetadata(HelixPropertyStore<ZNRecord> propertyStore, String taskType,
      BaseTaskMetadata taskMetadata, int expectedVersion) {
    String newPath =
        ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadata(taskMetadata.getTableNameWithType(),
            taskType);
    String oldPath = ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadataDeprecated(taskType,
        taskMetadata.getTableNameWithType());
    if (propertyStore.exists(newPath, AccessOption.PERSISTENT) || !propertyStore.exists(oldPath,
        AccessOption.PERSISTENT)) {
      persistTaskMetadata(newPath, propertyStore, taskType, taskMetadata, expectedVersion);
    } else {
      persistTaskMetadata(oldPath, propertyStore, taskType, taskMetadata, expectedVersion);
    }
  }

  private static void persistTaskMetadata(String path, HelixPropertyStore<ZNRecord> propertyStore, String taskType,
      BaseTaskMetadata taskMetadata, int expectedVersion) {
    if (!propertyStore.set(path, taskMetadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT)) {
      throw new ZkException(
          "Failed to persist minion metadata for task: " + taskType + " and metadata: " + taskMetadata);
    }
  }
}
