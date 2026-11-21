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

import com.google.common.util.concurrent.Uninterruptibles;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


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
    assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(null));
    SegmentNameGeneratorSpec spec = new SegmentNameGeneratorSpec();
    assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(new HashMap<>());
    assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("use.global.directory.sequence.id", "false"));
    assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("use.global.directory.sequence.id", "FALSE"));
    assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("use.global.directory.sequence.id", "True"));
    assertTrue(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("local.directory.sequence.id", "true"));
    assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("local.directory.sequence.id", "TRUE"));
    assertFalse(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
    spec.setConfigs(Map.of("local.directory.sequence.id", "False"));
    assertTrue(SegmentGenerationJobUtils.useGlobalDirectorySequenceId(spec));
  }

  @Test
  public void testGetStagingCopyParallelism() {
    assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(null), 10);
    assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(Map.of()),
        SegmentGenerationJobUtils.DEFAULT_STAGING_COPY_PARALLELISM);
    assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
        Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "1")), 1);
    assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
            Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "0")),
        SegmentGenerationJobUtils.DEFAULT_STAGING_COPY_PARALLELISM);
    assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
            Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "-1")),
        SegmentGenerationJobUtils.DEFAULT_STAGING_COPY_PARALLELISM);
    assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
            Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "not-a-number")),
        SegmentGenerationJobUtils.DEFAULT_STAGING_COPY_PARALLELISM);
    assertEquals(SegmentGenerationJobUtils.getStagingCopyParallelism(
        Map.of(SegmentGenerationJobUtils.STAGING_COPY_PARALLELISM, "999")), 64);
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

    assertEquals(FileUtils.readFileToString(new File(destDir, "seg.tar.gz"), StandardCharsets.UTF_8), "old");
    // Source left in place when destination was not overwritten
    assertTrue(new File(sourceDir, "seg.tar.gz").exists());
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

    assertEquals(FileUtils.readFileToString(new File(destDir, "seg.tar.gz"), StandardCharsets.UTF_8), "new");
    assertFalse(new File(sourceDir, "seg.tar.gz").exists());
  }

  @Test
  public void testMoveFilesAggregatesIndependentFailures()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    FileUtils.writeStringToFile(new File(sourceDir, "ok.tar.gz"), "ok", StandardCharsets.UTF_8);
    for (String fileName : List.of("bad-one.tar.gz", "bad-two.tar.gz")) {
      FileUtils.writeStringToFile(new File(sourceDir, fileName), "bad", StandardCharsets.UTF_8);
    }

    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean move(URI srcUri, URI dstUri, boolean overwrite)
          throws IOException {
        String fileName = new File(srcUri).getName();
        if (fileName.startsWith("bad-")) {
          throw new IOException(fileName);
        }
        return super.move(srcUri, dstUri, overwrite);
      }
    };

    IOException failure = expectThrows(IOException.class,
        () -> SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, 2));
    assertEquals(failure.getSuppressed().length, 1);
    assertEquals(Set.of(failure.getMessage(), failure.getSuppressed()[0].getMessage()),
        Set.of("bad-one.tar.gz", "bad-two.tar.gz"));
    assertEquals(FileUtils.readFileToString(new File(destDir, "ok.tar.gz"), StandardCharsets.UTF_8), "ok");
  }

  @Test
  public void testMoveFilesClampsExtremeParallelismValues()
      throws Exception {
    runMoveAndAssertLayout(0);
    runMoveAndAssertLayout(-3);
    runMoveAndAssertLayout(Integer.MAX_VALUE);
  }

  @Test
  public void testMoveFilesOverlapsMovesWithinParallelismCap()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    int fileCount = SegmentGenerationJobUtils.MAX_STAGING_COPY_PARALLELISM + 16;
    for (int i = 0; i < fileCount; i++) {
      FileUtils.writeStringToFile(new File(sourceDir, "seg-" + i + ".tar.gz"), "c" + i, StandardCharsets.UTF_8);
    }
    AtomicInteger activeMoves = new AtomicInteger();
    AtomicInteger maxActiveMoves = new AtomicInteger();
    Set<Thread> workerThreads = ConcurrentHashMap.newKeySet();
    CountDownLatch firstTwoMoves = new CountDownLatch(2);
    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean move(URI srcUri, URI dstUri, boolean overwrite)
          throws IOException {
        workerThreads.add(Thread.currentThread());
        int active = activeMoves.incrementAndGet();
        maxActiveMoves.accumulateAndGet(active, Math::max);
        firstTwoMoves.countDown();
        try {
          if (!Uninterruptibles.awaitUninterruptibly(firstTwoMoves, 10, TimeUnit.SECONDS)) {
            throw new IOException("Two moves did not overlap");
          }
          return super.move(srcUri, dstUri, overwrite);
        } finally {
          activeMoves.decrementAndGet();
        }
      }
    };

    // A direct caller of the overload must not be able to bypass the MAX_STAGING_COPY_PARALLELISM cap.
    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, Integer.MAX_VALUE);

    assertEquals(destDir.list().length, fileCount);
    assertTrue(maxActiveMoves.get() >= 2, "Two moves must overlap");
    assertTrue(maxActiveMoves.get() <= SegmentGenerationJobUtils.MAX_STAGING_COPY_PARALLELISM,
        "Observed " + maxActiveMoves.get() + " simultaneous moves");
    assertTrue(workerThreads.size() <= SegmentGenerationJobUtils.MAX_STAGING_COPY_PARALLELISM,
        "Used " + workerThreads.size() + " workers");
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
    assertEquals(observedOverwriteFlags.size(), 1);
    assertFalse(observedOverwriteFlags.get(0));
    assertEquals(FileUtils.readFileToString(new File(destDir, "seg.tar.gz"), StandardCharsets.UTF_8), "new");
  }

  @Test
  public void testMoveFilesFalseMoveResultSkipsOnlyWhenDestinationExistsAndNoOverwrite()
      throws Exception {
    // A concurrent writer created the destination between the exists() pre-check and move().
    moveWithFalseResult(true);

    assertEquals(FileUtils.readFileToString(new File(_tempDir, "dest/seg.tar.gz"), StandardCharsets.UTF_8), "other");
    assertTrue(new File(_tempDir, "src/seg.tar.gz").exists());
  }

  @Test
  public void testMoveFilesFalseMoveResultFailsWithoutDestination()
      throws Exception {
    expectThrows(IOException.class, () -> moveWithFalseResult(false));
    assertTrue(new File(_tempDir, "src/seg.tar.gz").exists());
    assertFalse(new File(_tempDir, "dest/seg.tar.gz").exists());
  }

  @Test
  public void testMoveFilesFalseMoveResultFailsWhenOverwritePartiallyCompletes()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    File sourceFile = new File(sourceDir, "seg.tar.gz");
    File destFile = new File(destDir, "seg.tar.gz");
    FileUtils.writeStringToFile(sourceFile, "replacement", StandardCharsets.UTF_8);
    FileUtils.writeStringToFile(destFile, "old", StandardCharsets.UTF_8);
    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean doMove(URI srcUri, URI dstUri)
          throws IOException {
        assertFalse(new File(dstUri).exists(), "Overwrite removed the old file before attempting the move");
        FileUtils.writeStringToFile(new File(dstUri), "partial", StandardCharsets.UTF_8);
        return false;
      }
    };

    IOException failure = expectThrows(IOException.class,
        () -> SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, 1));
    assertTrue(failure.getMessage().contains("seg.tar.gz"));
    assertEquals(FileUtils.readFileToString(sourceFile, StandardCharsets.UTF_8), "replacement");
    assertEquals(FileUtils.readFileToString(destFile, StandardCharsets.UTF_8), "partial");
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

    assertEquals(FileUtils.readFileToString(new File(destDir, "part-a/seg.tar.gz"), StandardCharsets.UTF_8),
        "content");
  }

  @Test
  public void testMoveFilesDoesNotFallbackOnMetadataListingIoFailure() {
    IOException listingFailure = new IOException("Cannot list staging directory");
    PinotFS fs = new LocalPinotFS() {
      @Override
      public List<FileMetadata> listFilesWithMetadata(URI fileUri, boolean recursive)
          throws IOException {
        throw listingFailure;
      }

      @Override
      public String[] listFiles(URI fileUri, boolean recursive) {
        throw new AssertionError("An I/O failure must propagate to the caller");
      }
    };

    assertSame(expectThrows(IOException.class,
        () -> SegmentGenerationJobUtils.moveFiles(fs, new File(_tempDir, "src").toURI(),
            new File(_tempDir, "dest").toURI(), true, 2)), listingFailure);
  }

  @Test
  public void testMoveFilesReturnsNormallyForEmptyStaging()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(new File(sourceDir, "empty-directory"));

    SegmentGenerationJobUtils.moveFiles(new LocalPinotFS(), sourceDir.toURI(), destDir.toURI(), true, 2);

    assertFalse(destDir.exists());
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
      fail("Expected IOException when the caller is interrupted");
    } catch (IOException e) {
      // Thread.interrupted() also clears the flag so it cannot leak into the next test.
      assertTrue(Thread.interrupted(), "Interrupt status must be restored");
      assertTrue(e.getMessage().contains("Interrupted"), e.getMessage());
    }
    // Outstanding moves are cancelled, so nothing reaches the destination.
    assertEquals(destDir.list().length, 0);
  }

  @Test
  public void testMoveFilesInterruptedWaitsForInFlightMoves()
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    int fileCount = 4;
    int parallelism = 2;
    for (int i = 0; i < fileCount; i++) {
      String name = "seg-" + i + ".tar.gz";
      FileUtils.writeStringToFile(new File(sourceDir, name), "new", StandardCharsets.UTF_8);
      FileUtils.writeStringToFile(new File(destDir, name), "old", StandardCharsets.UTF_8);
    }
    CountDownLatch movesStarted = new CountDownLatch(parallelism);
    CountDownLatch workersInterrupted = new CountDownLatch(parallelism);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch cleanupStarted = new CountDownLatch(1);
    AtomicInteger movesEntered = new AtomicInteger();
    AtomicInteger movesFinished = new AtomicInteger();
    AtomicBoolean movedAfterCleanup = new AtomicBoolean();
    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean doMove(URI srcUri, URI dstUri)
          throws IOException {
        // BasePinotFS has already checked the source and removed the old destination at this point.
        movesEntered.incrementAndGet();
        movesStarted.countDown();
        try {
          if (!release.await(10, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting for the test to release the move");
          }
        } catch (InterruptedException e) {
          // A move that ignores the interrupt, like a remote copy that is already under way.
          workersInterrupted.countDown();
          if (!Uninterruptibles.awaitUninterruptibly(release, 10, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting for the test to release the interrupted move", e);
          }
        }
        if (cleanupStarted.getCount() == 0) {
          movedAfterCleanup.set(true);
        }
        boolean moved = super.doMove(srcUri, dstUri);
        movesFinished.incrementAndGet();
        return moved;
      }
    };
    AtomicInteger movesFinishedAtCleanup = new AtomicInteger(-1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicReference<IOException> cleanupFailure = new AtomicReference<>();
    AtomicBoolean interruptRestored = new AtomicBoolean();
    Thread driver = new Thread(() -> {
      try {
        SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, parallelism);
      } catch (Throwable t) {
        failure.set(t);
      } finally {
        // Both staging callers run cleanup immediately, even when moveFiles throws.
        interruptRestored.set(Thread.currentThread().isInterrupted());
        movesFinishedAtCleanup.set(movesFinished.get());
        cleanupStarted.countDown();
        try {
          fs.delete(sourceDir.toURI(), true);
        } catch (IOException e) {
          cleanupFailure.set(e);
        }
      }
    }, "staging-copy-test-driver");
    driver.setDaemon(true);
    driver.start();

    try {
      assertTrue(movesStarted.await(5, TimeUnit.SECONDS), "Expected two in-flight overwrites");
      driver.interrupt();
      assertTrue(workersInterrupted.await(5, TimeUnit.SECONDS), "Both workers should observe cancellation");
      assertFalse(cleanupStarted.await(1, TimeUnit.SECONDS), "Cleanup must wait for in-flight overwrites");
      assertEquals(sourceDir.list().length, fileCount, "Staging must still contain the replacement inputs");
    } finally {
      release.countDown();
      driver.join(TimeUnit.SECONDS.toMillis(5));
    }

    assertFalse(driver.isAlive(), "The driver should finish once the workers have returned");
    assertEquals(movesEntered.get(), parallelism, "Queued moves should be cancelled");
    assertEquals(movesFinishedAtCleanup.get(), parallelism);
    assertFalse(movedAfterCleanup.get());
    assertTrue(failure.get() instanceof IOException, String.valueOf(failure.get()));
    assertTrue(failure.get().getCause() instanceof InterruptedException, String.valueOf(failure.get().getCause()));
    assertTrue(interruptRestored.get(), "Interrupt status must be restored");
    assertNull(cleanupFailure.get());
    assertFalse(sourceDir.exists());
    int replacements = 0;
    for (File file : destDir.listFiles()) {
      if (FileUtils.readFileToString(file, StandardCharsets.UTF_8).equals("new")) {
        replacements++;
      }
    }
    assertEquals(replacements, parallelism);
  }

  private void moveWithFalseResult(boolean destinationAppears)
      throws Exception {
    File sourceDir = new File(_tempDir, "src");
    File destDir = new File(_tempDir, "dest");
    FileUtils.forceMkdir(sourceDir);
    FileUtils.forceMkdir(destDir);
    FileUtils.writeStringToFile(new File(sourceDir, "seg.tar.gz"), "new", StandardCharsets.UTF_8);
    PinotFS fs = new LocalPinotFS() {
      @Override
      public boolean move(URI srcUri, URI dstUri, boolean overwrite)
          throws IOException {
        if (destinationAppears) {
          FileUtils.writeStringToFile(new File(dstUri), "other", StandardCharsets.UTF_8);
        }
        return false;
      }
    };

    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), false, 1);
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
      if (i < 4) {
        FileUtils.writeStringToFile(new File(destDir, relative), "old", StandardCharsets.UTF_8);
      }
      expectedRelativePaths.add(relative);
    }

    LocalPinotFS fs = new LocalPinotFS();
    SegmentGenerationJobUtils.moveFiles(fs, sourceDir.toURI(), destDir.toURI(), true, parallelism);

    for (String relative : expectedRelativePaths) {
      File destFile = new File(destDir, relative);
      assertTrue(destFile.exists(), "Missing dest file: " + relative);
      int start = relative.lastIndexOf("seg-") + 4;
      int end = relative.indexOf('.', start);
      String expectedContent = "content-" + relative.substring(start, end);
      assertEquals(FileUtils.readFileToString(destFile, StandardCharsets.UTF_8), expectedContent);
      assertFalse(new File(sourceDir, relative).exists(), "Source should be moved: " + relative);
    }
  }
}
