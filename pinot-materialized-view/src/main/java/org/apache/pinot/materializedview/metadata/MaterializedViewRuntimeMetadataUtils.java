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


/// Utility methods to fetch/persist [MaterializedViewRuntimeMetadata] from/to ZooKeeper
/// under the path `/CONFIGS/MATERIALIZED_VIEW/RUNTIME/<viewTableNameWithType>`.
public final class MaterializedViewRuntimeMetadataUtils {

  private MaterializedViewRuntimeMetadataUtils() {
  }

  @Nullable
  public static MaterializedViewRuntimeMetadata fetch(HelixPropertyStore<ZNRecord> propertyStore,
      String viewTableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(viewTableNameWithType);
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    znRecord.setVersion(stat.getVersion());
    return MaterializedViewRuntimeMetadata.fromZNRecord(znRecord);
  }

  /// Fetches runtime metadata along with its ZK stat version.
  /// The version is set on the returned ZNRecord and can be used for CAS writes.
  ///
  /// @return the runtime metadata, or `null` if not found
  @Nullable
  public static MaterializedViewRuntimeMetadata fetchWithVersion(HelixPropertyStore<ZNRecord> propertyStore,
      String viewTableNameWithType, Stat outStat) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(viewTableNameWithType);
    ZNRecord znRecord = propertyStore.get(path, outStat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return null;
    }
    return MaterializedViewRuntimeMetadata.fromZNRecord(znRecord);
  }

  /// Persists the runtime metadata via version-checked CAS write.
  ///
  /// `expectedVersion` MUST match the ZK version of the value the caller fetched (use
  /// `fetchWithVersion`).  A version mismatch is reported via a typed exception so callers
  /// can distinguish "another writer beat us — retry" from "the metadata itself is invalid":
  ///
  ///   - [CasConflictException] — ZK rejected the write because the version changed.  Callers
  ///     SHOULD re-fetch and retry.
  ///   - [IllegalStateException] / [IllegalArgumentException] — `validateForPersist` rejected
  ///     the instance.  Callers MUST NOT retry; the data is structurally invalid.
  ///   - [ZkException] — underlying ZK transport / session failure.  Callers SHOULD retry
  ///     with backoff but should NOT treat as a routine CAS conflict.
  public static void persist(HelixPropertyStore<ZNRecord> propertyStore,
      MaterializedViewRuntimeMetadata metadata, int expectedVersion) {
    // Strict writer-side check: refuse to persist an instance that violates any
    // forward-compatibility invariants enforced by validateForPersist().  Reads
    // (constructor) tolerate slightly inconsistent historical data; writes never
    // propagate inconsistency forward.
    metadata.validateForPersist();
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(
        metadata.getMaterializedViewTableNameWithType());
    if (!propertyStore.set(path, metadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT)) {
      throw new CasConflictException("CAS conflict persisting MaterializedViewRuntimeMetadata for: "
          + metadata.getMaterializedViewTableNameWithType()
          + " (expectedVersion=" + expectedVersion + " did not match)");
    }
  }

  /// Thrown by [#persist] when the version-checked CAS write is rejected because another
  /// writer mutated the znode first.  Carries the message so the retry loop can surface
  /// the conflict at ERROR level when retries exhaust.
  public static final class CasConflictException extends ZkException {
    public CasConflictException(String message) {
      super(message);
    }
  }

  /// Creates the runtime metadata znode only if it does not already exist.  Returns true on
  /// success; returns false if a concurrent writer already created the znode (the caller can
  /// then re-fetch and proceed with that value).  Used on cold-start to avoid two scheduler
  /// runs blind-clobbering each other.
  public static boolean createIfAbsent(HelixPropertyStore<ZNRecord> propertyStore,
      MaterializedViewRuntimeMetadata metadata) {
    metadata.validateForPersist();
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(
        metadata.getMaterializedViewTableNameWithType());
    return propertyStore.create(path, metadata.toZNRecord(), AccessOption.PERSISTENT);
  }

  public static void delete(HelixPropertyStore<ZNRecord> propertyStore,
      String viewTableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMaterializedViewRuntime(viewTableNameWithType);
    if (!propertyStore.remove(path, AccessOption.PERSISTENT)) {
      throw new ZkException("Failed to delete MaterializedViewRuntimeMetadata for: " + viewTableNameWithType);
    }
  }
}
