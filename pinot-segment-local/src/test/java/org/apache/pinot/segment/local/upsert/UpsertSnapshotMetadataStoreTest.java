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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class UpsertSnapshotMetadataStoreTest {
  @Test
  public void testCompactContextIsImmediatelyAvailableAndCompatible()
      throws Exception {
    File directory = Files.createTempDirectory("upsert-snapshot-metadata").toFile();
    try {
      assertNull(UpsertSnapshotMetadataStore.read(directory, 3));
      UpsertSnapshotMetadata metadata = metadata(3);
      assertTrue(UpsertSnapshotMetadataStore.persist(directory, metadata));
      assertEquals(UpsertSnapshotMetadataStore.read(directory, 3), metadata);
      assertFalse(JsonUtils.objectToJsonNode(metadata).has("segments"));
      assertFalse(JsonUtils.objectToJsonNode(metadata).has("lifecycleBefore"));
      assertFalse(JsonUtils.objectToJsonNode(metadata).has("configurationFingerprint"));
      assertFalse(JsonUtils.objectToJsonNode(metadata).has("comparisonIssues"));
      assertTrue(JsonUtils.objectToJsonNode(metadata).has("concurrentSnapshots"));
      assertEquals(JsonUtils.objectToJsonNode(metadata).get("boundaryStatus").asText(), "UNVERIFIED");

      // A forged status must not turn an observed capture into a verified boundary.
      String json = JsonUtils.objectToString(metadata).replace("UNVERIFIED", "VERIFIED");
      assertEquals(JsonUtils.stringToObject(json, UpsertSnapshotMetadata.class).getBoundaryStatus(), "UNVERIFIED");
      File file = new File(directory, "upsert.snapshot.metadata.partition.3.json");
      for (int unsupportedVersion : new int[]{2, 99}) {
        Files.writeString(file.toPath(), JsonUtils.objectToString(metadata).replace(
            "\"formatVersion\":" + UpsertSnapshotMetadata.FORMAT_VERSION, "\"formatVersion\":" + unsupportedVersion));
        assertNull(UpsertSnapshotMetadataStore.read(directory, 3));
      }
      Files.writeString(file.toPath(), "{truncated");
      assertNull(UpsertSnapshotMetadataStore.read(directory, 3));
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  @Test
  public void testWriteFailureIsBestEffortAndCleansTemporaryFile()
      throws Exception {
    File directory = Files.createTempDirectory("upsert-snapshot-write-failure").toFile();
    try {
      UpsertSnapshotMetadata metadata = metadata(0);
      File target = new File(directory, "upsert.snapshot.metadata.partition.0.json");
      assertTrue(target.mkdir());
      Files.writeString(new File(target, "block-replacement").toPath(), "keep");
      assertFalse(UpsertSnapshotMetadataStore.persist(directory, metadata));
      assertTrue(target.isDirectory());
      assertEquals(directory.list().length, 1);
      assertNull(UpsertSnapshotMetadataStore.read(directory, 0));
      File missingDirectory = new File(directory, "removed-table");
      UpsertSnapshotMetadataStore.persist(missingDirectory, metadata);
      assertFalse(missingDirectory.exists());
    } finally {
      FileUtils.deleteDirectory(directory);
    }
  }

  private static UpsertSnapshotMetadata metadata(int partitionId) {
    return new UpsertSnapshotMetadata(UpsertSnapshotMetadata.FORMAT_VERSION, partitionId, "table__3__2__100", "5000",
        1000, 1001, "runtime", 1, new UpsertSnapshotMetadata.Attempt(0, 0, 0, 0, 0, false),
        new UpsertSnapshotMetadata.Counters(1, 0, 0), UpsertSnapshotMetadata.Content.unavailable(0),
        UpsertSnapshotMetadata.CleanupProgress.disabled(), UpsertSnapshotMetadata.CleanupProgress.disabled(),
        false);
  }
}
