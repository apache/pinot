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
package org.apache.pinot.common.metadata.columndeletion;

import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// CAS read/write helper for {@link ColumnDeletionMetadata}.
///
/// Thread-safe: the helper is stateless. Concurrent writers must pass the expected znode version
/// from the last read. A version conflict returns {@code false} and does not clobber the znode.
/// A record whose format version is newer than this process can parse is never overwritten.
public final class ColumnDeletionMetadataAccessHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnDeletionMetadataAccessHelper.class);

  private ColumnDeletionMetadataAccessHelper() {
  }

  /// Read the ledger znode, or {@code null} when it does not exist.
  ///
  /// The returned {@link ZNRecord} has {@link ZNRecord#getVersion()} set from the Stat so callers
  /// can CAS the next write.
  @Nullable
  public static ZNRecord getColumnDeletionMetadataZNRecord(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String tableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForColumnDeletionMetadata(tableNameWithType);
    Stat stat = new Stat();
    ZNRecord znRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord != null) {
      znRecord.setVersion(stat.getVersion());
    }
    return znRecord;
  }

  /// Decode the ledger, or {@code null} when the znode does not exist.
  ///
  /// @throws ColumnDeletionUnsupportedFormatException if the stored format is newer than this process
  @Nullable
  public static ColumnDeletionMetadata getColumnDeletionMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String tableNameWithType) {
    ZNRecord znRecord = getColumnDeletionMetadataZNRecord(propertyStore, tableNameWithType);
    if (znRecord == null) {
      return null;
    }
    return ColumnDeletionMetadata.fromZNRecord(znRecord);
  }

  /// Write the ledger with an expected znode version.
  ///
  /// First write uses {@code create}. {@code expectedVersion} of {@code -1} is create-only: if the
  /// znode already exists the write returns {@code false} and does not overwrite. Updates must pass
  /// the version from {@link #getColumnDeletionMetadataZNRecord}. A stored format newer than this
  /// process can parse is never overwritten.
  ///
  /// @return true if the write succeeded
  public static boolean writeColumnDeletionMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      ColumnDeletionMetadata metadata, int expectedVersion) {
    if (metadata.getFormatVersion() > ColumnDeletionMetadata.CURRENT_FORMAT_VERSION) {
      throw new ColumnDeletionUnsupportedFormatException(metadata.getFormatVersion(),
          ColumnDeletionMetadata.CURRENT_FORMAT_VERSION);
    }
    String tableNameWithType = metadata.getTableNameWithType();
    String path = ZKMetadataProvider.constructPropertyStorePathForColumnDeletionMetadata(tableNameWithType);
    Stat stat = new Stat();
    ZNRecord existing = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (existing == null) {
      if (expectedVersion != -1) {
        LOGGER.warn("Failed to write column deletion metadata for table: {} at expected version: {} (znode missing)",
            tableNameWithType, expectedVersion);
        return false;
      }
      boolean created = propertyStore.create(path, metadata.toZNRecord(), AccessOption.PERSISTENT);
      if (created) {
        LOGGER.info("Created column deletion metadata for table: {}", tableNameWithType);
      } else {
        LOGGER.warn("Failed to create column deletion metadata for table: {}", tableNameWithType);
      }
      return created;
    }
    int storedFormatVersion = ColumnDeletionMetadata.peekFormatVersion(existing);
    if (storedFormatVersion > ColumnDeletionMetadata.CURRENT_FORMAT_VERSION) {
      throw new ColumnDeletionUnsupportedFormatException(storedFormatVersion,
          ColumnDeletionMetadata.CURRENT_FORMAT_VERSION);
    }
    if (expectedVersion == -1) {
      LOGGER.warn("Refusing create-only write of column deletion metadata for table: {} (znode exists at version: {})",
          tableNameWithType, stat.getVersion());
      return false;
    }
    try {
      boolean result = propertyStore.set(path, metadata.toZNRecord(), expectedVersion, AccessOption.PERSISTENT);
      if (result) {
        LOGGER.info("Wrote column deletion metadata for table: {} at expected version: {}", tableNameWithType,
            expectedVersion);
      } else {
        LOGGER.warn("Failed to write column deletion metadata for table: {} at expected version: {}",
            tableNameWithType, expectedVersion);
      }
      return result;
    } catch (ZkBadVersionException e) {
      LOGGER.warn("CAS conflict writing column deletion metadata for table: {} at expected version: {}",
          tableNameWithType, expectedVersion);
      return false;
    }
  }
}
