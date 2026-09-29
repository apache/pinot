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
package org.apache.pinot.materializedview.metadata;

import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;


/// Utility methods to fetch/persist [MaterializedViewDefinitionMetadata] from/to ZooKeeper
/// under the path `/CONFIGS/MATERIALIZED_VIEW/DEFINITION/<viewTableNameWithType>`.
public final class MaterializedViewDefinitionMetadataUtils {

  private MaterializedViewDefinitionMetadataUtils() {
  }

  @Nullable
  public static MaterializedViewDefinitionMetadata fetch(HelixPropertyStore<ZNRecord> propertyStore,
      String viewTableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewDefinition(viewTableNameWithType);
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    znRecord.setVersion(stat.getVersion());
    return MaterializedViewDefinitionMetadata.fromZNRecord(znRecord);
  }

  public static void persist(HelixPropertyStore<ZNRecord> propertyStore,
      MaterializedViewDefinitionMetadata metadata, int expectedVersion) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewDefinition(
        metadata.getMaterializedViewTableNameWithType());
    if (!propertyStore.set(path, metadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to persist MaterializedViewDefinitionMetadata for: "
          + metadata.getMaterializedViewTableNameWithType());
    }
  }

  /// Creates the definition metadata znode only if it does not already exist.  Returns true on
  /// success; returns false if a concurrent writer already created the znode.  Used on
  /// cold-start so two scheduler runs do not clobber each other's `partitionExprMaps` /
  /// `splitSpec` (which can diverge under a racing schema update).
  public static boolean createIfAbsent(HelixPropertyStore<ZNRecord> propertyStore,
      MaterializedViewDefinitionMetadata metadata) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewDefinition(
        metadata.getMaterializedViewTableNameWithType());
    return propertyStore.create(path, metadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  public static void delete(HelixPropertyStore<ZNRecord> propertyStore,
      String viewTableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewDefinition(viewTableNameWithType);
    if (!propertyStore.remove(path, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to delete MaterializedViewDefinitionMetadata for: " + viewTableNameWithType);
    }
  }
}
