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
package org.apache.pinot.minion.coordination;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.coordination.NativeTaskOutputIdentity;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Real generation verifies immutable artifact identity, captured source integrity and local filesystem boundaries.
public class NativeSegmentGenerationTaskTest {
  @Test
  public void testRealSegmentArtifactAndBounds() throws Exception {
    Path directory = Files.createTempDirectory("native-minion-generation-");
    try {
      Path sources = Files.createDirectory(directory.resolve("sources"));
      Path outputs = Files.createDirectory(directory.resolve("outputs"));
      Path source = sources.resolve("orders.json");
      Files.writeString(source, "{\"id\":1,\"label\":\"one\"}\n{\"id\":2,\"label\":\"two\"}\n");
      String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(source)));
      MetadataRpc.ParticipantToken participant = MetadataRpc.ParticipantToken.newBuilder()
          .setHistoryId(UUID.randomUUID().toString()).setParticipantId("Minion_test")
          .setIncarnation(UUID.randomUUID().toString()).setRegistrationEpoch(1)
          .setRole(MetadataRpc.ParticipantRole.MINION).build();
      MetadataRpc.TaskExecutionState task = task(participant, source, outputs, hash);
      TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").build();
      Schema schema = new Schema.SchemaBuilder().setSchemaName("orders").addSingleValueDimension("id", DataType.LONG)
          .addSingleValueDimension("label", DataType.STRING).build();
      NativeSegmentGenerationTask generator = new NativeSegmentGenerationTask(sources, outputs, 1024, 2);
      MetadataRpc.OfflineSegmentContent result = generator.generate(participant, task, table, schema,
          directory.resolve("work"), () -> { });
      Path archive = Path.of(java.net.URI.create(result.getDownloadUri()));
      assertTrue(Files.isRegularFile(archive));
      assertEquals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(archive))),
          result.getContentSha256());
      assertEquals(result.getSegmentName(),
          NativeTaskOutputIdentity.outputSegmentName(UUID.fromString(task.getTaskId()), 1, 0));
      Path extracted = Files.createDirectory(directory.resolve("extracted"));
      TarCompressionUtils.untar(archive.toFile(), extracted.toFile());
      SegmentMetadataImpl metadata = new SegmentMetadataImpl(extracted.resolve(result.getSegmentName()).toFile());
      assertEquals(metadata.getTotalDocs(), 2);
      assertEquals(metadata.getCrc(), result.getLoader().getCrc());
      assertFalse(Files.exists(directory.resolve("work").resolve(result.getSegmentName())));
      byte[] original = Files.readAllBytes(archive);
      MetadataRpc.TaskExecutionState wrongDigest = task.toBuilder().setInput(task.getInput().toBuilder()
          .putConfigs("sourceSha256", "0".repeat(64))).build();
      assertThrows(IllegalArgumentException.class,
          () -> generator.generate(participant, wrongDigest, table, schema, directory.resolve("work"), () -> { }));
      assertThrows(IllegalArgumentException.class, () -> new NativeSegmentGenerationTask(sources, outputs, 1, 2)
          .generate(participant, task, table, schema, directory.resolve("work"), () -> { }));
      assertThrows(Exception.class, () -> new NativeSegmentGenerationTask(sources, outputs, 1024, 1)
          .generate(participant, task, table, schema, directory.resolve("work"), () -> { }));
      assertEquals(Files.readAllBytes(archive), original, "Failed attempts cannot replace a staged artifact");
    } finally {
      FileUtils.deleteDirectory(directory.toFile());
    }
  }

  @Test
  public void testSourceAndOutputSymlinksCannotEscapeApprovedRoots() throws Exception {
    Path directory = Files.createTempDirectory("native-minion-boundaries-");
    try {
      Path sources = Files.createDirectory(directory.resolve("sources"));
      Path outputs = Files.createDirectory(directory.resolve("outputs"));
      Path outside = Files.createDirectory(directory.resolve("outside"));
      Path source = outside.resolve("input.json");
      Files.writeString(source, "{\"id\":1}\n");
      String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(source)));
      MetadataRpc.ParticipantToken participant = MetadataRpc.ParticipantToken.newBuilder()
          .setHistoryId(UUID.randomUUID().toString()).setParticipantId("Minion_test")
          .setIncarnation(UUID.randomUUID().toString()).setRegistrationEpoch(1)
          .setRole(MetadataRpc.ParticipantRole.MINION).build();
      Path linked = Files.createSymbolicLink(sources.resolve("input.json"), source);
      TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").build();
      Schema schema = new Schema.SchemaBuilder().setSchemaName("orders").addSingleValueDimension("id", DataType.LONG)
          .build();
      NativeSegmentGenerationTask generator = new NativeSegmentGenerationTask(sources, outputs, 1024, 10);
      assertThrows(IllegalArgumentException.class, () -> generator.generate(participant,
          task(participant, linked, outputs, hash), table, schema, directory.resolve("work"), () -> { }));
      Files.delete(linked);
      Files.copy(source, linked);
      Files.createSymbolicLink(outputs.resolve(participant.getHistoryId()), outside);
      assertThrows(IllegalArgumentException.class, () -> generator.generate(participant,
          task(participant, linked, outputs, hash), table, schema, directory.resolve("work"), () -> { }));
      try (var files = Files.list(outside)) {
        assertEquals(files.count(), 1L, "No attempt artifacts may be created outside the approved root");
      }
    } finally {
      FileUtils.deleteDirectory(directory.toFile());
    }
  }

  private static MetadataRpc.TaskExecutionState task(MetadataRpc.ParticipantToken participant, Path source,
      Path outputs, String digest) {
    return MetadataRpc.TaskExecutionState.newBuilder().setHistoryId(participant.getHistoryId())
        .setTaskId(UUID.randomUUID().toString()).setAttempt(1).setState(MetadataRpc.TaskExecutionState.State.RUNNING)
        .setInput(MetadataRpc.TaskInput.newBuilder().setMinionId(participant.getParticipantId()).setMaxAttempts(2)
            .setQueue(MetadataRpc.TaskQueueKey.newBuilder().setTableName("orders_OFFLINE")
                .setTableObjectId(UUID.randomUUID().toString()).setTaskType(NativeTaskOutputIdentity.TASK_TYPE))
            .putAllConfigs(Map.of("sourceUri", source.toUri().toString(), "sourceSha256", digest))
            .setOutputPolicy(MetadataRpc.TaskOutputPolicy.newBuilder().setRootUri(outputs.toUri().toString())
                .setMaxOutputs(1))).build();
  }
}
