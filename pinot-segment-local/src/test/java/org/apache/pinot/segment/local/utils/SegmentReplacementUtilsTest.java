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
package org.apache.pinot.segment.local.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/// Exercises root discovery and collection using actual local files and supplied metadata snapshots.
public class SegmentReplacementUtilsTest {
  private static final String TABLE = "test_OFFLINE";
  private static final URI ROOT = URI.create("s3://bucket/table/segment-replacements-v1/test_OFFLINE/");
  private static final long NOW = SegmentReplacementUtils.RETENTION_MS * 2;

  @DataProvider
  public Object[][] cleanupCases() {
    return new Object[][]{
        {200L, false, true}, // Expired, obsolete output from a changed segment.
        {200L, true, false}, // Successful push: preserve the registered URL.
        {100L, false, true}, // Expired attempts are collectible even when the segment CRC never changes.
        {-1L, false, true} // Deleted segment cannot be re-added by a REFRESH_ONLY push.
    };
  }

  @Test(dataProvider = "cleanupCases")
  public void testCleanupUsesCurrentMetadata(long crc, boolean registered, boolean delete) throws Exception {
    URI output = ROOT.resolve("100/" + UUID.randomUUID() + "/segment.tar.gz");
    PinotFS fs = mock(PinotFS.class);
    when(fs.exists(any())).thenReturn(true);
    when(fs.listFilesWithMetadata(ROOT, true)).thenReturn(List.of(file(output, 1),
        file(URI.create(output + SegmentReplacementUtils.OBSOLETE_SUFFIX), 1)));
    SegmentZKMetadata current = crc == -1 ? null : segment(crc, registered ? output.toString() : "s3://other/segment");
    SegmentReplacementUtils.cleanup(ROOT, fs, current == null ? Map.of() : Map.of("segment", current), NOW);
    verify(fs, times(delete ? 1 : 0)).delete(output, false);
  }

  @Test
  public void testCleanupKeepsUnexpiredAttemptsAndActiveUploads() throws Exception {
    URI unexpired = ROOT.resolve((NOW + 1) + "/" + UUID.randomUUID() + "/segment.tar.gz");
    URI expired = ROOT.resolve("100/" + UUID.randomUUID() + "/segment.tar.gz");
    PinotFS fs = mock(PinotFS.class);
    when(fs.exists(any())).thenReturn(true);
    when(fs.listFilesWithMetadata(ROOT, true)).thenReturn(List.of(file(unexpired, 1), file(expired, 1),
        file(URI.create(expired + SegmentReplacementUtils.OBSOLETE_SUFFIX), 1)));
    SegmentZKMetadata current = segment(200, "s3://other/segment");
    current.setSegmentUploadStartTime(1);
    SegmentReplacementUtils.cleanup(ROOT, fs, Map.of("segment", current), NOW);
    verify(fs, never()).delete(expired, false);
    verify(fs, never()).delete(unexpired, false);
    current.setSegmentUploadStartTime(-1);
    SegmentReplacementUtils.cleanup(ROOT, fs, Map.of("segment", current), NOW);
    verify(fs).delete(expired, false);
    verify(fs, never()).delete(unexpired, false);
  }

  @Test
  public void testCleanupWaitsFromFirstObsoleteObservation() throws Exception {
    URI output = ROOT.resolve("100/" + UUID.randomUUID() + "/segment.tar.gz");
    URI marker = URI.create(output + SegmentReplacementUtils.OBSOLETE_SUFFIX);
    PinotFS fs = mock(PinotFS.class);
    when(fs.exists(any())).thenReturn(true);
    when(fs.listFilesWithMetadata(ROOT, true)).thenReturn(List.of(file(output, 1)));
    Map<String, SegmentZKMetadata> segments = Map.of("segment", segment(200, "s3://other/segment"));
    SegmentReplacementUtils.cleanup(ROOT, fs, segments, NOW);
    verify(fs).touch(marker);
    verify(fs, never()).delete(output, false);
    when(fs.listFilesWithMetadata(ROOT, true)).thenReturn(List.of(file(output, 1), file(marker, NOW)));
    SegmentReplacementUtils.cleanup(ROOT, fs, segments, NOW + 1);
    verify(fs, never()).delete(output, false);
    SegmentReplacementUtils.cleanup(ROOT, fs, segments, NOW + SegmentReplacementUtils.RETENTION_MS);
    verify(fs).delete(output, false);
  }

  @Test
  public void testCleanupKeepsYoungUnknownAndReferencedFiles() throws Exception {
    URI young = ROOT.resolve("100/" + UUID.randomUUID() + "/young.tar.gz");
    URI unknownTime = ROOT.resolve("100/" + UUID.randomUUID() + "/unknown.tar.gz");
    URI malformed = ROOT.resolve("100/not-an-attempt/segment.tar.gz");
    URI referenced = ROOT.resolve("100/" + UUID.randomUUID() + "/referenced.tar.gz");
    PinotFS fs = mock(PinotFS.class);
    when(fs.exists(any())).thenReturn(true);
    when(fs.listFilesWithMetadata(ROOT, true)).thenReturn(List.of(file(young, NOW), file(unknownTime, 0),
        file(malformed, 1), file(referenced, 1)));
    SegmentReplacementUtils.cleanup(ROOT, fs, Map.of("segment", segment(200, referenced.toString())), NOW);
    verify(fs, never()).delete(any(), anyBoolean());
  }

  @Test
  public void testRootsSurviveConfigChangesAndUseSuppliedSnapshot() throws Exception {
    File temp = Files.createTempDirectory("segment-replacement-roots").toFile();
    URI first = SegmentReplacementUtils.outputRoot(new File(temp, "first").toURI(), TABLE);
    URI second = SegmentReplacementUtils.outputRoot(new File(temp, "second").toURI(), TABLE);
    URI references = SegmentReplacementUtils.referencesDir(new File(temp, "controller").toURI().toString(), TABLE);
    URI output = first.resolve("100/" + UUID.randomUUID() + "/segment.tar.gz");
    try (MockedStatic<PinotFSFactory> factory = mockStatic(PinotFSFactory.class)) {
      factory.when(() -> PinotFSFactory.create("file")).thenAnswer(i -> new LocalPinotFS());
      SegmentReplacementUtils.rememberRoot(references, first);
      SegmentReplacementUtils.rememberRoot(references, second);
      FileUtils.writeStringToFile(new File(output), "purged", StandardCharsets.UTF_8);
      String dataDir = new File(temp, "controller").toURI().toString();
      SegmentReplacementUtils.cleanup(dataDir, TABLE, List.of(segment(200, output.toString())));
      File marker = new File(URI.create(output + SegmentReplacementUtils.OBSOLETE_SUFFIX));
      assertFalse(marker.exists());
      SegmentReplacementUtils.cleanup(dataDir, TABLE, List.of(segment(300, "s3://other/segment")));
      assertTrue(marker.isFile());
      assertTrue(marker.setLastModified(System.currentTimeMillis() - SegmentReplacementUtils.RETENTION_MS - 1000));
      SegmentReplacementUtils.cleanup(dataDir, TABLE, List.of(segment(300, "s3://other/segment")));
      assertFalse(new File(output).exists());
      assertFalse(marker.exists());
    } finally {
      FileUtils.deleteDirectory(temp);
    }
  }

  @Test
  public void testRootPersistenceUsesOneFileAndRepairsIncompleteWrite() throws Exception {
    File temp = Files.createTempDirectory("purge-root-reference").toFile();
    try (PinotFS fs = new LocalPinotFS()) {
      URI references = temp.toURI();
      SegmentReplacementUtils.rememberRoot(fs, references, ROOT);
      SegmentReplacementUtils.rememberRoot(fs, references, ROOT);
      String[] files = fs.listFiles(references, false);
      assertEquals(files.length, 1);
      File reference = new File(files[0]);
      assertEquals(Files.readString(reference.toPath()), ROOT.toString());
      Files.writeString(reference.toPath(), "partial write");
      SegmentReplacementUtils.rememberRoot(fs, references, ROOT);
      assertEquals(Files.readString(reference.toPath()), ROOT.toString());
      assertEquals(fs.listFiles(references, false).length, 1);
    } finally {
      FileUtils.deleteDirectory(temp);
    }
  }

  @Test
  public void testCorruptReferenceSkipsCleanup() throws Exception {
    File temp = Files.createTempDirectory("segment-replacement-corrupt-reference").toFile();
    String dataDir = temp.toURI().toString();
    try (MockedStatic<PinotFSFactory> factory = mockStatic(PinotFSFactory.class)) {
      factory.when(() -> PinotFSFactory.create("file")).thenAnswer(i -> new LocalPinotFS());
      PinotFS outputFS = mock(PinotFS.class);
      factory.when(() -> PinotFSFactory.create("s3")).thenReturn(outputFS);
      SegmentReplacementUtils.rememberRoot(SegmentReplacementUtils.referencesDir(dataDir, TABLE), ROOT);
      File reference = FileUtils.listFiles(temp, new String[]{"root"}, true).iterator().next();
      Files.writeString(reference.toPath(), "s3://wrong-bucket/");
      SegmentReplacementUtils.cleanup(dataDir, TABLE, List.of());
      verifyNoInteractions(outputFS);
    } finally {
      FileUtils.deleteDirectory(temp);
    }
  }

  @Test
  public void testReferenceToAnotherTableCannotAuthorizeCleanup() throws Exception {
    File temp = Files.createTempDirectory("segment-replacement-wrong-table").toFile();
    String dataDir = temp.toURI().toString();
    try (MockedStatic<PinotFSFactory> factory = mockStatic(PinotFSFactory.class)) {
      factory.when(() -> PinotFSFactory.create("file")).thenAnswer(i -> new LocalPinotFS());
      PinotFS outputFS = mock(PinotFS.class);
      factory.when(() -> PinotFSFactory.create("s3")).thenReturn(outputFS);
      URI other = SegmentReplacementUtils.outputRoot(URI.create("s3://bucket/table"), "other_OFFLINE");
      SegmentReplacementUtils.rememberRoot(SegmentReplacementUtils.referencesDir(dataDir, TABLE), other);
      SegmentReplacementUtils.cleanup(dataDir, TABLE, List.of());
      verifyNoInteractions(outputFS);
    } finally {
      FileUtils.deleteDirectory(temp);
    }
  }

  @Test
  public void testRootPersistenceReadbackFailureStopsGeneration() throws Exception {
    PinotFS fs = mock(PinotFS.class);
    when(fs.open(any())).thenAnswer(i -> new ByteArrayInputStream("truncated".getBytes(StandardCharsets.UTF_8)));
    expectThrows(IllegalStateException.class,
        () -> SegmentReplacementUtils.rememberRoot(fs, URI.create("s3://bucket/references/"), ROOT));
  }

  private static SegmentZKMetadata segment(long crc, String downloadUrl) {
    SegmentZKMetadata segment = new SegmentZKMetadata("segment");
    segment.setCrc(crc);
    segment.setDownloadUrl(downloadUrl);
    return segment;
  }

  private static FileMetadata file(URI uri, long modified) {
    return new FileMetadata.Builder().setFilePath(uri.toString()).setLastModifiedTime(modified).build();
  }
}
