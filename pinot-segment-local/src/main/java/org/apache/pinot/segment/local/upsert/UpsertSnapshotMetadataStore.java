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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Publishes self-contained count summaries independently of recovery bitmaps. One daemon writes at most eight
/// queued summaries, each capped at [#MAX_SEGMENTS] entries. No bitmap bytes or segment objects are retained.
/// Submission never waits for disk I/O or queue capacity; overload loses diagnostic coverage, not ingestion progress.
public final class UpsertSnapshotMetadataStore {
  public static final String ENABLE_SNAPSHOT_METADATA = "enableSnapshotMetadata";
  static final int MAX_SEGMENTS = 10_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertSnapshotMetadataStore.class);
  private static final ThreadPoolExecutor WRITER = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
      new ArrayBlockingQueue<>(8),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("upsert-snapshot-metadata-%d").build());

  private UpsertSnapshotMetadataStore() {
  }

  static void submit(File tableIndexDir, UpsertSnapshotMetadata metadata) {
    submit(tableIndexDir, metadata, WRITER);
  }

  @VisibleForTesting
  static boolean submit(File tableIndexDir, UpsertSnapshotMetadata metadata, Executor executor) {
    try {
      executor.execute(() -> {
        try {
          persist(tableIndexDir, metadata);
        } catch (Exception e) {
          LOGGER.warn("Could not persist upsert snapshot metadata for table directory: {}, partition: {}",
              tableIndexDir, metadata.partitionId(), e);
        }
      });
      return true;
    } catch (RejectedExecutionException e) {
      LOGGER.warn("Skipping upsert snapshot metadata for table directory: {}, partition: {}: writer queue is full",
          tableIndexDir, metadata.partitionId());
      return false;
    }
  }

  @VisibleForTesting
  static void persist(File tableIndexDir, UpsertSnapshotMetadata metadata)
      throws IOException {
    File target = getMetadataFile(tableIndexDir, metadata.partitionId());
    // Do not create the table directory: the table may have been removed since capture.
    File temporary = File.createTempFile(target.getName(), ".tmp", tableIndexDir);
    try {
      Files.write(temporary.toPath(), JsonUtils.objectToBytes(metadata));
      Files.move(temporary.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
    } finally {
      Files.deleteIfExists(temporary.toPath());
    }
  }

  /// Returns the last successfully published summary, which can predate newer bitmap snapshots or a restart.
  /// Missing, malformed and unsupported summaries are unavailable, never evidence of agreement or divergence.
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
