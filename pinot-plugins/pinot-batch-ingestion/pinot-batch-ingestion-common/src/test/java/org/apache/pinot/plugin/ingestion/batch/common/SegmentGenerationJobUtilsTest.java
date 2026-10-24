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

package org.apache.pinot.plugin.ingestion.batch.common;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SegmentGenerationJobUtilsTest {
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "SegmentGenerationJobUtilsTest-" + UUID.randomUUID());
    FileUtils.forceMkdir(_tempDir);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testUseGlobalDirectorySequenceId() {
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(null));
    SegmentNameGeneratorSpec spec = new SegmentNameGeneratorSpec();
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(new HashMap<>());
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("use.global.directory.sequence.id", "false"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("use.global.directory.sequence.id", "FALSE"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("use.global.directory.sequence.id", "True"));
    Assert.assertTrue(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("local.directory.sequence.id", "true"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("local.directory.sequence.id", "TRUE"));
    Assert.assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("local.directory.sequence.id", "False"));
    Assert.assertTrue(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
  }

  @Test
  public void testGetStagingCopyParallelism() {
    Assert.assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(null),
        SegmentGenerationJobUtils.DEFAULT_STAGING_COPY_PARALLELISM);
    Assert.assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(Map.of()),
        SegmentGenerationJobUtils.DEFAULT_STAGING_COPY_PARALLELISM);
    Assert.assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
            Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "4")), 4);
    Assert.assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
            Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "0")),
        SegmentGenerationJobUtils.DEFAULT_STAGING_COPY_PARALLELISM);
    Assert.assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
            Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "-1")),
        SegmentGenerationJobUtils.DEFAULT_STAGING_COPY_PARALLELISM);
    Assert.assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
            Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "not-a-number")),
        SegmentGenerationJobUtils.DEFAULT_STAGING_COPY_PARALLELISM);
    Assert.assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
            Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "999")),
        SegmentGenerationJobUtils.MAX_STAGING_COPY_PARALLELISM);
  }

  @Test
  public void testMoveFilesParallelismOnePreservesRelativeLayout()
      throws Exception {
    runMoveAndAssertLayout(1);
  }

  @Test
  public void testMoveFilesParallelismFourPreservesRelativeLayout()
      throws Exception {
    runMoveAndAssertLayout(4);
  }

  @Test
  public void testMoveFilesOverwriteFalseSkipsExisting()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    FileUtils.writeStringToFile(new File(sourceDir, "seg.tar.gz"), "new", StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(new File(destDir, "seg.tar.gz"), "old", StandardCharsets.UTF_8);

    LocalPinotFS fs = new LocalPinotFS();
    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), false, 2);

    Assert.assertEquals(FileUtils.readFileToString(new File(destDir, "seg.tar.gz"), StandardCharsets.UTF_8), "old");
    // Source left in place when destination was not overwritten
    Assert.assertTrue(new File(sourceDir, "seg.tar.gz").exists());
  }

  @Test
  public void testMoveFilesOverwriteTrueReplacesExisting()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    FileUtils.writeStringToFile(new File(sourceDir, "seg.tar.gz"), "new", StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(new File(destDir, "seg.tar.gz"), "old", StandardCharsets.UTF_8);

    LocalPinotFS fs = new LocalPinotFS();
    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, 2);

    Assert.assertEquals(FileUtils.readFileToString(new File(destDir, "seg.tar.gz"), StandardCharsets.UTF_8), "new");
    Assert.assertFalse(new File(sourceDir, "seg.tar.gz").exists());
  }

  @Test
  public void testMoveFilesFailsWhenOneMoveFails()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    FileUtils.writeStringToFile(new File(sourceDir, "ok.tar.gz"), "ok", StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(new File(sourceDir, "bad.tar.gz"), "bad", StandardCharsets.UTF_8);

    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean move(URI srcUri, URI dstUri, boolean overwrite)
          throws IOException {
        if (srcUri.getPath().endsWith("bad.tar.gz")) {
          throw new IOException("simulated move failure for " + srcUri);
        }
        return super.move(srcUri, dstUri, overwrite);
      }
    };

    try {
      SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, 4);
      Assert.fail("Expected IOException when one move fails");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("bad.tar.gz") || (e.getCause() != null && e.getCause().getMessage()
          .contains("bad.tar.gz")) || e.getMessage().contains("simulated move failure"), e.getMessage());
    }
  }

  @Test
  public void testMoveFilesClampsExtremeParallelismValues()
      throws Exception {
    runMoveAndAssertLayout(0);
    runMoveAndAssertLayout(-3);
    runMoveAndAssertLayout(Integer.MAX_VALUE);
  }

  @Test
  public void testMoveFilesNeverExceedsMaxParallelism()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    int fileCount = SegmentGenerationJobUtils.MAX_STAGING_COPY_PARALLELISM + 16;
    for (int i = 0; i < fileCount; i++) {
      FileUtils.writeStringToFile(new File(sourceDir, "seg-" + i + ".tar.gz"), "c" + i, StandardCharsets.UTF_8);
    }
    Set<Thread> workerThreads = ConcurrentHashMap.newKeySet();
    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean move(URI srcUri, URI dstUri, boolean overwrite)
          throws IOException {
        workerThreads.add(Thread.currentThread());
        return super.move(srcUri, dstUri, overwrite);
      }
    };

    // A direct caller of the overload must not be able to bypass the MAX_STAGING_COPY_PARALLELISM cap.
    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, Integer.MAX_VALUE);

    Assert.assertEquals(destDir.list().length, fileCount);
    Assert.assertTrue(workerThreads.size() <= SegmentGenerationJobUtils.MAX_STAGING_COPY_PARALLELISM,
        "Used " + workerThreads.size() + " threads for " + fileCount + " files");
  }

  @Test
  public void testMoveFilesPassesOverwriteFlagToFileSystem()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.writeStringToFile(new File(sourceDir, "seg.tar.gz"), "new", StandardCharsets.UTF_8);
    List<Boolean> observedOverwriteFlags = new CopyOnWriteArrayList<>();
    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean move(URI srcUri, URI dstUri, boolean overwrite)
          throws IOException {
        observedOverwriteFlags.add(overwrite);
        return super.move(srcUri, dstUri, overwrite);
      }
    };

    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), false, 2);

    // The caller's flag has to reach PinotFS.move, which re-checks the destination itself.
    Assert.assertEquals(observedOverwriteFlags.size(), 1);
    Assert.assertFalse(observedOverwriteFlags.get(0));
    Assert.assertEquals(FileUtils.readFileToString(new File(destDir, "seg.tar.gz"), StandardCharsets.UTF_8), "new");
  }

  @Test
  public void testMoveFilesTreatsFalseMoveResultAsSkip()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    FileUtils.writeStringToFile(new File(sourceDir, "seg.tar.gz"), "new", StandardCharsets.UTF_8);
    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean move(URI srcUri, URI dstUri, boolean overwrite) {
        // What PinotFS.move does when the destination appeared after the exists() pre-check.
        return false;
      }
    };

    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), false, 2);

    Assert.assertEquals(destDir.list().length, 0);
    Assert.assertTrue(new File(sourceDir, "seg.tar.gz").exists());
  }

  @Test
  public void testMoveFilesFallsBackWhenMetadataListingUnsupported()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(new File(sourceDir, "part-a"));
    FileUtils.writeStringToFile(new File(sourceDir, "part-a/seg.tar.gz"), "content", StandardCharsets.UTF_8);
    PinotFS fs = new LocalPinotFS() {
      @Override
      public List<FileMetadata> listFilesWithMetadata(URI fileUri, boolean recursive) {
        throw new UnsupportedOperationException();
      }
    };

    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, 2);

    Assert.assertEquals(FileUtils.readFileToString(new File(destDir, "part-a/seg.tar.gz"), StandardCharsets.UTF_8),
        "content");
  }

  @Test
  public void testMoveFilesInterruptedCancelsOutstandingMoves()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    for (int i = 0; i < 8; i++) {
      FileUtils.writeStringToFile(new File(sourceDir, "seg-" + i + ".tar.gz"), "c" + i, StandardCharsets.UTF_8);
    }
    CountDownLatch neverReleased = new CountDownLatch(1);
    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean move(URI srcUri, URI dstUri, boolean overwrite)
          throws IOException {
        try {
          neverReleased.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
        return super.move(srcUri, dstUri, overwrite);
      }
    };

    Thread.currentThread().interrupt();
    try {
      SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, 4);
      Assert.fail("Expected IOException when the caller is interrupted");
    } catch (IOException e) {
      // Thread.interrupted() also clears the flag so it cannot leak into the next test.
      Assert.assertTrue(Thread.interrupted(), "Interrupt status must be restored");
      Assert.assertTrue(e.getMessage().contains("Interrupted"), e.getMessage());
    }
    // Outstanding moves are cancelled rather than awaited, so nothing reaches the destination.
    Assert.assertEquals(destDir.list().length, 0);
  }

  private void runMoveAndAssertLayout(int parallelism)
      throws Exception {
    File sourceDir = new File(_tempDir, "src-" + parallelism);
    File destDir = new File(_tempDir, "dest-" + parallelism);
    FileUtils.forceMkdir(new File(sourceDir, "part-a"));
    FileUtils.forceMkdir(new File(sourceDir, "part-b/nested"));
    Set<String> expectedRelativePaths = new HashSet<>();
    for (int i = 0; i < 8; i++) {
      String relative;
      if (i < 4) {
        relative = "part-a/seg-" + i + ".tar.gz";
      } else {
        relative = "part-b/nested/seg-" + i + ".tar.gz";
      }
      FileUtils.writeStringToFile(new File(sourceDir, relative), "content-" + i, StandardCharsets.UTF_8);
      expectedRelativePaths.add(relative);
    }

    LocalPinotFS fs = new LocalPinotFS();
    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, parallelism);

    for (String relative : expectedRelativePaths) {
      File destFile = new File(destDir, relative);
      Assert.assertTrue(destFile.exists(), "Missing dest file: " + relative);
      int start = relative.lastIndexOf("seg-") + 4;
      int end = relative.indexOf('.', start);
      String expectedContent = "content-" + relative.substring(start, end);
      Assert.assertEquals(FileUtils.readFileToString(destFile, StandardCharsets.UTF_8), expectedContent);
      Assert.assertFalse(new File(sourceDir, relative).exists(), "Source should be moved: " + relative);
    }
  }
}
