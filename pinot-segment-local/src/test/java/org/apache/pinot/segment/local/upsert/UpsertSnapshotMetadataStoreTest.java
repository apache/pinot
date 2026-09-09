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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.upsert.UpsertSnapshotMetadata.SegmentSnapshot;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class UpsertSnapshotMetadataStoreTest {
  @Test
  public void testSelfContainedSummaryAndCompatibility()
      throws Exception {
    File directory = Files.createTempDirectory("upsert-snapshot-metadata").toFile();
    try {
      assertNull(UpsertSnapshotMetadataStore.read(directory, 3));
      Map<String, SegmentSnapshot> counts = new HashMap<>();
      counts.put("segment", new SegmentSnapshot("123", 10, null));
      UpsertSnapshotMetadata metadata = new UpsertSnapshotMetadata(1, 3, "table__3__2__100", "5000", 1000,
          2, 1, 0, false, counts);
      counts.clear();
      UpsertSnapshotMetadataStore.persist(directory, metadata);
      assertEquals(UpsertSnapshotMetadataStore.read(directory, 3), metadata);
      assertNull(UpsertSnapshotMetadataStore.read(directory, 3).segments().get("segment").queryableDocCount());
      assertEquals(JsonUtils.objectToJsonNode(metadata).get("boundaryStatus").asText(), "UNVERIFIED");

      // A future status must not turn a version-1 observation into a verified boundary.
      String json = JsonUtils.objectToString(metadata).replace("UNVERIFIED", "VERIFIED");
      assertEquals(JsonUtils.stringToObject(json, UpsertSnapshotMetadata.class).getBoundaryStatus(), "UNVERIFIED");
      UpsertSnapshotMetadataStore.persist(directory,
          new UpsertSnapshotMetadata(2, 3, "table__3__2__100", "5000", 1000, 2, 1, 0, false, Map.of()));
      assertNull(UpsertSnapshotMetadataStore.read(directory, 3));
      File file = new File(directory, "upsert.snapshot.metadata.partition.3.json");
      Files.writeString(file.toPath(), "{truncated");
      assertNull(UpsertSnapshotMetadataStore.read(directory, 3));
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  @Test(timeOut = 10_000)
  public void testFullQueueDropsDiagnosticWithoutWaitingForWriter()
      throws Exception {
    File directory = Files.createTempDirectory("upsert-snapshot-queue").toFile();
    CountDownLatch writerStarted = new CountDownLatch(1);
    CountDownLatch releaseWriter = new CountDownLatch(1);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
    try {
      executor.execute(() -> {
        writerStarted.countDown();
        try {
          releaseWriter.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      assertTrue(writerStarted.await(5, TimeUnit.SECONDS));
      UpsertSnapshotMetadata metadata = new UpsertSnapshotMetadata(1, 0, "segment", "10", 1000,
          0, 0, 0, false, Map.of());
      assertTrue(UpsertSnapshotMetadataStore.submit(directory, metadata, executor));
      assertFalse(UpsertSnapshotMetadataStore.submit(directory, metadata, executor));
      assertNull(UpsertSnapshotMetadataStore.read(directory, 0));
      releaseWriter.countDown();
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertEquals(UpsertSnapshotMetadataStore.read(directory, 0), metadata);
    } finally {
      releaseWriter.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
      FileUtils.deleteDirectory(directory);
    }
  }
}
