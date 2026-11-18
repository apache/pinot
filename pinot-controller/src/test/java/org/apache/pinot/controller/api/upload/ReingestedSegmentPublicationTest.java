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
package org.apache.pinot.controller.api.upload;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests that fixed-range recovery publishes one immutable artifact without corrupting concurrent winners.
public class ReingestedSegmentPublicationTest {
  private static final String TABLE = "replay_REALTIME";
  private static final String SEGMENT = new LLCSegmentName("replay", 0, 0, 0L).getSegmentName();

  @Test
  public void testRecoveryPublishesUniqueArtifactAndPreservesCanonicalFile()
      throws Exception {
    File dir = Files.createTempDirectory("reingestion-publication").toFile();
    try {
      File source = new File(dir, "source");
      File canonical = new File(dir, SEGMENT);
      Files.writeString(source.toPath(), "recovered");
      Files.writeString(canonical.toPath(), "original");
      PinotHelixResourceManager manager = manager();
      ZNRecord record = pending().toZNRecord();
      record.setVersion(3);
      when(manager.getSegmentMetadataZnRecord(TABLE, SEGMENT)).thenReturn(record);
      AtomicReference<SegmentZKMetadata> published = new AtomicReference<>();
      when(manager.updateZkMetadata(eq(TABLE), any(SegmentZKMetadata.class), eq(3))).thenAnswer(invocation -> {
        published.set(invocation.getArgument(1));
        return true;
      });
      operator(manager).completeReingestedSegmentOperations(TABLE, artifact(), canonical.toURI(),
          source.toURI().toString(), canonical.toURI().toString(), source.length());
      SegmentZKMetadata metadata = published.get();
      assertEquals(metadata.getStatus(), Status.DONE);
      assertNotEquals(metadata.getDownloadUrl(), canonical.toURI().toString());
      assertTrue(metadata.getDownloadUrl().startsWith(canonical.toURI() + ".recovery."));
      assertEquals(Files.readString(new File(URI.create(metadata.getDownloadUrl())).toPath()), "recovered");
      assertEquals(Files.readString(canonical.toPath()), "original");
      assertEquals(metadata.getStartOffset(), "10");
      assertEquals(metadata.getEndOffset(), "20");
      verify(manager).resetSegment(TABLE, SEGMENT, null);
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }

  @Test
  public void testConcurrentWinnerNeverGetsOverwrittenByLosingReplay()
      throws Exception {
    File dir = Files.createTempDirectory("reingestion-conflict").toFile();
    try {
      File source = new File(dir, "source");
      File canonical = new File(dir, SEGMENT);
      File winner = new File(dir, "winning-attempt");
      Files.writeString(source.toPath(), "loser");
      Files.writeString(winner.toPath(), "winner");
      PinotHelixResourceManager manager = manager();
      ZNRecord before = pending().toZNRecord();
      before.setVersion(3);
      SegmentZKMetadata completed = pending();
      completed.setStatus(Status.DONE);
      completed.setDownloadUrl(winner.toURI().toString());
      ZNRecord after = completed.toZNRecord();
      after.setVersion(4);
      when(manager.getSegmentMetadataZnRecord(TABLE, SEGMENT)).thenReturn(before, after);
      expectThrows(ControllerApplicationException.class,
          () -> operator(manager).completeReingestedSegmentOperations(TABLE, artifact(), canonical.toURI(),
              source.toURI().toString(), "http://controller/segments/replay/" + SEGMENT, source.length()));
      verify(manager, never()).updateZkMetadata(eq(TABLE), any(), anyInt());
      assertEquals(Files.readString(winner.toPath()), "winner");
      assertFalse(canonical.exists());
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }

  @Test
  public void testWrongRangeIsRejectedBeforeArtifactCopy()
      throws Exception {
    PinotHelixResourceManager manager = manager();
    when(manager.getSegmentMetadataZnRecord(TABLE, SEGMENT)).thenReturn(pending().toZNRecord());
    SegmentMetadata artifact = artifact();
    when(artifact.getEndOffset()).thenReturn("21");
    expectThrows(ControllerApplicationException.class,
        () -> operator(manager).completeReingestedSegmentOperations(TABLE, artifact, URI.create("file:/absent/target"),
            "file:/absent/source", "http://controller/segments/replay/" + SEGMENT, 10));
    verify(manager, never()).updateZkMetadata(eq(TABLE), any(), anyInt());
  }

  @Test
  public void testFailedMetadataCasLeavesWinningArtifactUntouched()
      throws Exception {
    File dir = Files.createTempDirectory("reingestion-cas").toFile();
    try {
      File source = new File(dir, "source");
      File canonical = new File(dir, SEGMENT);
      Files.writeString(source.toPath(), "replayed");
      PinotHelixResourceManager manager = manager();
      ZNRecord record = pending().toZNRecord();
      record.setVersion(3);
      when(manager.getSegmentMetadataZnRecord(TABLE, SEGMENT)).thenReturn(record);
      AtomicReference<String> attemptedUrl = new AtomicReference<>();
      when(manager.updateZkMetadata(eq(TABLE), any(SegmentZKMetadata.class), eq(3))).thenAnswer(invocation -> {
        attemptedUrl.set(((SegmentZKMetadata) invocation.getArgument(1)).getDownloadUrl());
        // A lost CAS response can mean this exact URL already won. It must never be deleted on failure.
        return false;
      });
      expectThrows(RuntimeException.class,
          () -> operator(manager).completeReingestedSegmentOperations(TABLE, artifact(), canonical.toURI(),
              source.toURI().toString(), canonical.toURI().toString(), source.length()));
      assertEquals(Files.readString(new File(URI.create(attemptedUrl.get())).toPath()), "replayed");
      verify(manager, never()).resetSegment(TABLE, SEGMENT, null);
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }

  private static PinotHelixResourceManager manager() {
    PinotHelixResourceManager manager = mock(PinotHelixResourceManager.class);
    when(manager.getTableConfig(TABLE))
        .thenReturn(new TableConfigBuilder(TableType.REALTIME).setTableName("replay").build());
    return manager;
  }

  private static ZKOperator operator(PinotHelixResourceManager manager) {
    return new ZKOperator(manager, new ControllerConf(), mock(ControllerMetrics.class));
  }

  private static SegmentZKMetadata pending() {
    SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT);
    metadata.setStatus(Status.COMMITTING);
    metadata.setReplicateMode(true);
    metadata.setStartOffset("10");
    metadata.setEndOffset("20");
    return metadata;
  }

  private static SegmentMetadata artifact() {
    SegmentMetadata artifact = mock(SegmentMetadata.class);
    when(artifact.getName()).thenReturn(SEGMENT);
    when(artifact.getStartOffset()).thenReturn("10");
    when(artifact.getEndOffset()).thenReturn("20");
    when(artifact.getCustomMap()).thenReturn(Map.of());
    when(artifact.getColumnMetadataMap()).thenReturn(new TreeMap<>());
    return artifact;
  }
}
