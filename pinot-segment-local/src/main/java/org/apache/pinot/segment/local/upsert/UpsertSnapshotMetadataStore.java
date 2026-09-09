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
package org.apache.pinot.segment.local.upsert;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Synchronously persists compact partition context at the end of an existing startup snapshot attempt.
/// Atomic file replacement allows concurrent readers. Diagnostic write failures leave recovery bitmaps untouched.
public final class UpsertSnapshotMetadataStore {
  public static final String ENABLE_SNAPSHOT_METADATA = "enableSnapshotMetadata";
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertSnapshotMetadataStore.class);

  private UpsertSnapshotMetadataStore() {
  }

  static void persist(File tableIndexDir, UpsertSnapshotMetadata metadata) {
    try {
      File target = getMetadataFile(tableIndexDir, metadata.partitionId());
      // Do not create a table directory that may have been removed.
      File temporary = File.createTempFile(target.getName(), ".tmp", tableIndexDir);
      try {
        Files.write(temporary.toPath(), JsonUtils.objectToBytes(metadata));
        Files.move(temporary.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
      } finally {
        Files.deleteIfExists(temporary.toPath());
      }
    } catch (Exception e) {
      LOGGER.warn("Could not persist upsert snapshot metadata for table directory: {}, partition: {}",
          tableIndexDir, metadata.partitionId(), e);
    }
  }

  /// Returns the last successfully persisted context, which can predate newer bitmap snapshots or a restart.
  /// Missing, malformed and unsupported metadata is unavailable, never evidence of agreement or divergence.
  @Nullable
  public static UpsertSnapshotMetadata read(File tableIndexDir, int partitionId) {
    File file = getMetadataFile(tableIndexDir, partitionId);
    if (!file.isFile()) {
      return null;
    }
    try {
      UpsertSnapshotMetadata metadata = JsonUtils.fileToObject(file, UpsertSnapshotMetadata.class);
      return metadata.formatVersion() == UpsertSnapshotMetadata.FORMAT_VERSION && metadata.partitionId() == partitionId
          ? metadata : null;
    } catch (Exception e) {
      LOGGER.warn("Could not read upsert snapshot metadata: {}", file, e);
      return null;
    }
  }

  private static File getMetadataFile(File tableIndexDir, int partitionId) {
    return new File(tableIndexDir, "upsert.snapshot.metadata.partition." + partitionId + ".json");
  }
}
