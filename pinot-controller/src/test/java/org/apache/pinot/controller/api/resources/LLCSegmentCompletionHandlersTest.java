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
package org.apache.pinot.controller.api.resources;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class LLCSegmentCompletionHandlersTest {
  private static final String TEST_SCHEME = "controlleruploadlock";
  private static final String FAIL_FIRST_TEST_SCHEME = "controlleruploadfailfirst";
  private static final String CONTROLLER_NAMESPACE = "controller-one";
  private static final String INSTANCE_ID = "Server_1";
  private static final String SEGMENT_NAME = new LLCSegmentName("test_REALTIME", 1, 0, 1234L).getSegmentName();
  private static final String STREAM_PARTITION_MSG_OFFSET = "42";

  @BeforeClass
  public void setUp() {
    PinotFSFactory.register(TEST_SCHEME, BlockingPinotFS.class.getName(), null);
    PinotFSFactory.register(FAIL_FIRST_TEST_SCHEME, FailFirstPinotFS.class.getName(), null);
  }

  @Test
  public void testUploadCoordinationIsStableWithinOneControllerProcess() {
    URI dataDirUri = URI.create(TEST_SCHEME + "://root");
    String otherSegmentName = new LLCSegmentName("test_REALTIME", 1, 1, 1234L).getSegmentName();

    UUID firstId = LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE, INSTANCE_ID,
        SEGMENT_NAME, STREAM_PARTITION_MSG_OFFSET);
    UUID retryId = LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE, INSTANCE_ID,
        SEGMENT_NAME, STREAM_PARTITION_MSG_OFFSET);

    Assert.assertEquals(retryId, firstId);
    Assert.assertNotEquals(
        LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId("controller-two", INSTANCE_ID, SEGMENT_NAME,
            STREAM_PARTITION_MSG_OFFSET), firstId);
    Assert.assertNotEquals(
        LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE, "Server_2", SEGMENT_NAME,
            STREAM_PARTITION_MSG_OFFSET), firstId);
    Assert.assertNotEquals(
        LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE, INSTANCE_ID,
            otherSegmentName, STREAM_PARTITION_MSG_OFFSET), firstId);
    Assert.assertNotEquals(
        LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE, INSTANCE_ID, SEGMENT_NAME,
            "43"), firstId);
    Assert.assertNotEquals(LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, SEGMENT_NAME,
            UUID.fromString("00000000-0000-0000-0000-000000000001")),
        LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, SEGMENT_NAME,
            UUID.fromString("00000000-0000-0000-0000-000000000002")));
  }

  @Test
  public void testUploadsForSameSegmentDoNotOverlap()
      throws Exception {
    BlockingPinotFS.reset();
    File segmentFile = File.createTempFile("segment", ".tar");
    UUID uploadCoordinationId = LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE,
        INSTANCE_ID, SEGMENT_NAME, STREAM_PARTITION_MSG_OFFSET);
    URI dataDirUri = URI.create(TEST_SCHEME + "://root");
    URI firstSegmentUri = LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, SEGMENT_NAME,
        UUID.fromString("00000000-0000-0000-0000-000000000011"));
    URI retrySegmentUri = LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, SEGMENT_NAME,
        UUID.fromString("00000000-0000-0000-0000-000000000012"));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch secondCallStarted = new CountDownLatch(1);
    try {
      Future<Boolean> firstUpload = executor.submit(
          () -> LLCSegmentCompletionHandlers.tryCopySegmentToDeepStore(segmentFile, uploadCoordinationId,
              firstSegmentUri));
      Assert.assertTrue(BlockingPinotFS.awaitFirstCopy());

      Future<Boolean> secondUpload = executor.submit(() -> {
        secondCallStarted.countDown();
        return LLCSegmentCompletionHandlers.tryCopySegmentToDeepStore(segmentFile, uploadCoordinationId,
            retrySegmentUri);
      });
      Assert.assertTrue(secondCallStarted.await(5, TimeUnit.SECONDS));
      Assert.assertFalse(secondUpload.get(5, TimeUnit.SECONDS));
      Assert.assertEquals(BlockingPinotFS.getCopyCount(), 1);

      BlockingPinotFS.releaseFirstCopy();
      Assert.assertTrue(firstUpload.get(5, TimeUnit.SECONDS));
      Assert.assertEquals(BlockingPinotFS.getCopyCount(), 1);
      Assert.assertEquals(BlockingPinotFS.getMaxActiveCopies(), 1);
      Assert.assertEquals(BlockingPinotFS.getCopiedUris(), List.of(firstSegmentUri));
    } finally {
      BlockingPinotFS.releaseFirstCopy();
      executor.shutdownNow();
      FileUtils.deleteQuietly(segmentFile);
    }
  }

  @Test
  public void testDifferentCoordinationIdsCanUploadConcurrently()
      throws Exception {
    BlockingPinotFS.reset();
    File segmentFile = File.createTempFile("segment", ".tar");
    String otherSegmentName = new LLCSegmentName("test_REALTIME", 1, 1, 1234L).getSegmentName();
    URI dataDirUri = URI.create(TEST_SCHEME + "://root");
    URI firstSegmentUri = LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, SEGMENT_NAME,
        UUID.fromString("00000000-0000-0000-0000-000000000001"));
    URI secondSegmentUri = LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, otherSegmentName,
        UUID.fromString("00000000-0000-0000-0000-000000000002"));
    UUID firstCoordinationId = LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE,
        INSTANCE_ID, SEGMENT_NAME, STREAM_PARTITION_MSG_OFFSET);
    UUID secondCoordinationId = LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE,
        INSTANCE_ID, otherSegmentName, STREAM_PARTITION_MSG_OFFSET);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> firstUpload = executor.submit(
          () -> LLCSegmentCompletionHandlers.tryCopySegmentToDeepStore(segmentFile, firstCoordinationId,
              firstSegmentUri));
      Assert.assertTrue(BlockingPinotFS.awaitFirstCopy());

      Future<Boolean> secondUpload = executor.submit(
          () -> LLCSegmentCompletionHandlers.tryCopySegmentToDeepStore(segmentFile, secondCoordinationId,
              secondSegmentUri));
      Assert.assertTrue(secondUpload.get(5, TimeUnit.SECONDS));
      Assert.assertTrue(BlockingPinotFS.awaitBothCopies(5, TimeUnit.SECONDS));
      Assert.assertEquals(BlockingPinotFS.getMaxActiveCopies(), 2);

      BlockingPinotFS.releaseFirstCopy();
      Assert.assertTrue(firstUpload.get(5, TimeUnit.SECONDS));
      Assert.assertEquals(BlockingPinotFS.getCopyCount(), 2);
    } finally {
      BlockingPinotFS.releaseFirstCopy();
      executor.shutdownNow();
      FileUtils.deleteQuietly(segmentFile);
    }
  }

  @Test
  public void testLaterRetryUsesFreshDestinationWithoutTouchingPreviousUpload()
      throws Exception {
    BlockingPinotFS.reset();
    File segmentFile = File.createTempFile("segment", ".tar");
    UUID uploadCoordinationId = LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE,
        INSTANCE_ID, SEGMENT_NAME, STREAM_PARTITION_MSG_OFFSET);
    URI dataDirUri = URI.create(TEST_SCHEME + "://root");
    URI firstSegmentUri = LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, SEGMENT_NAME,
        UUID.fromString("00000000-0000-0000-0000-000000000021"));
    URI retrySegmentUri = LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, SEGMENT_NAME,
        UUID.fromString("00000000-0000-0000-0000-000000000022"));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> firstUpload = executor.submit(
          () -> LLCSegmentCompletionHandlers.tryCopySegmentToDeepStore(segmentFile, uploadCoordinationId,
              firstSegmentUri));
      Assert.assertTrue(BlockingPinotFS.awaitFirstCopy());
      BlockingPinotFS.releaseFirstCopy();
      Assert.assertTrue(firstUpload.get(5, TimeUnit.SECONDS));

      Assert.assertTrue(
          LLCSegmentCompletionHandlers.tryCopySegmentToDeepStore(segmentFile, uploadCoordinationId, retrySegmentUri));
      Assert.assertEquals(BlockingPinotFS.getCopiedUris(), List.of(firstSegmentUri, retrySegmentUri));
      Assert.assertEquals(BlockingPinotFS.getDeleteCount(), 0);
    } finally {
      BlockingPinotFS.releaseFirstCopy();
      executor.shutdownNow();
      FileUtils.deleteQuietly(segmentFile);
    }
  }

  @Test
  public void testFailedUploadReleasesCoordinationLock()
      throws Exception {
    FailFirstPinotFS.reset();
    File segmentFile = File.createTempFile("segment", ".tar");
    UUID uploadCoordinationId = LLCSegmentCompletionHandlers.getSegmentUploadCoordinationId(CONTROLLER_NAMESPACE,
        INSTANCE_ID, SEGMENT_NAME, STREAM_PARTITION_MSG_OFFSET);
    URI dataDirUri = URI.create(FAIL_FIRST_TEST_SCHEME + "://root");
    URI firstSegmentUri = LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, SEGMENT_NAME,
        UUID.fromString("00000000-0000-0000-0000-000000000031"));
    URI retrySegmentUri = LLCSegmentCompletionHandlers.getSegmentUploadURI(dataDirUri, SEGMENT_NAME,
        UUID.fromString("00000000-0000-0000-0000-000000000032"));
    try {
      IOException exception = Assert.expectThrows(IOException.class,
          () -> LLCSegmentCompletionHandlers.tryCopySegmentToDeepStore(segmentFile, uploadCoordinationId,
              firstSegmentUri));
      Assert.assertEquals(exception.getMessage(), "Injected first-copy failure");

      Assert.assertTrue(
          LLCSegmentCompletionHandlers.tryCopySegmentToDeepStore(segmentFile, uploadCoordinationId, retrySegmentUri));
      Assert.assertEquals(FailFirstPinotFS.getCopyAttempts(), 2);
    } finally {
      FileUtils.deleteQuietly(segmentFile);
    }
  }

  /// PinotFS fixture that fails its first copy attempt after reset.
  /// Copy attempt counting is thread-safe; reset must run when no copies are active.
  public static class FailFirstPinotFS extends LocalPinotFS {
    private static final AtomicInteger COPY_ATTEMPTS = new AtomicInteger();

    private static void reset() {
      COPY_ATTEMPTS.set(0);
    }

    private static int getCopyAttempts() {
      return COPY_ATTEMPTS.get();
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws IOException {
      if (COPY_ATTEMPTS.incrementAndGet() == 1) {
        throw new IOException("Injected first-copy failure");
      }
    }
  }

  public static class BlockingPinotFS extends LocalPinotFS {
    private static final AtomicInteger COPY_COUNT = new AtomicInteger();
    private static final AtomicInteger DELETE_COUNT = new AtomicInteger();
    private static final AtomicInteger ACTIVE_COPIES = new AtomicInteger();
    private static final AtomicInteger MAX_ACTIVE_COPIES = new AtomicInteger();
    private static final Queue<URI> COPIED_URIS = new ConcurrentLinkedQueue<>();
    private static volatile CountDownLatch _firstCopyStarted = new CountDownLatch(1);
    private static volatile CountDownLatch _bothCopiesStarted = new CountDownLatch(2);
    private static volatile CountDownLatch _releaseFirstCopy = new CountDownLatch(1);

    private static void reset() {
      COPY_COUNT.set(0);
      DELETE_COUNT.set(0);
      ACTIVE_COPIES.set(0);
      MAX_ACTIVE_COPIES.set(0);
      COPIED_URIS.clear();
      _firstCopyStarted = new CountDownLatch(1);
      _bothCopiesStarted = new CountDownLatch(2);
      _releaseFirstCopy = new CountDownLatch(1);
    }

    private static boolean awaitFirstCopy()
        throws InterruptedException {
      return _firstCopyStarted.await(5, TimeUnit.SECONDS);
    }

    private static boolean awaitBothCopies(long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      return _bothCopiesStarted.await(timeout, timeUnit);
    }

    private static void releaseFirstCopy() {
      _releaseFirstCopy.countDown();
    }

    private static int getCopyCount() {
      return COPY_COUNT.get();
    }

    private static int getMaxActiveCopies() {
      return MAX_ACTIVE_COPIES.get();
    }

    private static int getDeleteCount() {
      return DELETE_COUNT.get();
    }

    private static List<URI> getCopiedUris() {
      return List.copyOf(COPIED_URIS);
    }

    @Override
    public boolean exists(URI fileUri) {
      return false;
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete) {
      DELETE_COUNT.incrementAndGet();
      return true;
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      int copyNumber = COPY_COUNT.incrementAndGet();
      COPIED_URIS.add(dstUri);
      int activeCopies = ACTIVE_COPIES.incrementAndGet();
      MAX_ACTIVE_COPIES.accumulateAndGet(activeCopies, Math::max);
      _firstCopyStarted.countDown();
      _bothCopiesStarted.countDown();
      try {
        if (copyNumber == 1 && !_releaseFirstCopy.await(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Timed out waiting to release the first copy");
        }
      } finally {
        ACTIVE_COPIES.decrementAndGet();
      }
    }
  }
}
