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
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;


/**
 * Utility methods to fetch/persist {@link MaterializedViewMetadata} from/to ZooKeeper
 * under the path {@code /CONFIGS/MATERIALIZED_VIEW/<mvTableNameWithType>}.
 */
public final class MaterializedViewMetadataUtils {

  private MaterializedViewMetadataUtils() {
  }

  @Nullable
  public static MaterializedViewMetadata fetchMaterializedViewMetadata(
      HelixPropertyStore<ZNRecord> propertyStore, String mvTableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewMetadata(mvTableNameWithType);
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    znRecord.setVersion(stat.getVersion());
    return MaterializedViewMetadata.fromZNRecord(znRecord);
  }

  /**
   * Persists the given metadata to ZK. Uses {@code expectedVersion} for optimistic locking;
   * pass {@code -1} to skip version checking.
   */
  public static void persistMaterializedViewMetadata(HelixPropertyStore<ZNRecord> propertyStore,
      MaterializedViewMetadata metadata, int expectedVersion) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewMetadata(
        metadata.getMvTableNameWithType());
    if (!propertyStore.set(path, metadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to persist MaterializedViewMetadata for: " + metadata.getMvTableNameWithType());
    }
  }

  public static void deleteMaterializedViewMetadata(HelixPropertyStore<ZNRecord> propertyStore,
      String mvTableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewMetadata(mvTableNameWithType);
    if (!propertyStore.remove(path, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to delete MaterializedViewMetadata for: " + mvTableNameWithType);
    }
  }
}
