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
      UpsertSnapshotMetadata metadata = new UpsertSnapshotMetadata(1, 3, "table__3__2__100", "5000", 1000);
      UpsertSnapshotMetadataStore.persist(directory, metadata);
      assertEquals(UpsertSnapshotMetadataStore.read(directory, 3), metadata);
      assertEquals(JsonUtils.objectToJsonNode(metadata).size(), 6);
      assertFalse(JsonUtils.objectToJsonNode(metadata).has("segments"));
      assertEquals(JsonUtils.objectToJsonNode(metadata).get("boundaryStatus").asText(), "UNVERIFIED");

      // A future status must not turn a version-1 observation into a verified boundary.
      String json = JsonUtils.objectToString(metadata).replace("UNVERIFIED", "VERIFIED");
      assertEquals(JsonUtils.stringToObject(json, UpsertSnapshotMetadata.class).getBoundaryStatus(), "UNVERIFIED");
      UpsertSnapshotMetadataStore.persist(directory,
          new UpsertSnapshotMetadata(2, 3, "table__3__2__100", "5000", 1000));
      assertNull(UpsertSnapshotMetadataStore.read(directory, 3));
      File file = new File(directory, "upsert.snapshot.metadata.partition.3.json");
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
      UpsertSnapshotMetadata metadata = new UpsertSnapshotMetadata(1, 0, "segment", "10", 1000);
      File target = new File(directory, "upsert.snapshot.metadata.partition.0.json");
      assertTrue(target.mkdir());
      Files.writeString(new File(target, "block-replacement").toPath(), "keep");
      UpsertSnapshotMetadataStore.persist(directory, metadata);
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
}
