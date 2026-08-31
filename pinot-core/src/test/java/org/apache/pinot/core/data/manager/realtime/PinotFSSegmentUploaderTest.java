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
package org.apache.pinot.core.data.manager.realtime;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.BasePinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.StringUtil;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotFSSegmentUploaderTest {
  private static final int TIMEOUT_IN_MS = 1000;
  private File _file;
  private LLCSegmentName _llcSegmentName;
  private ServerMetrics _serverMetrics = Mockito.mock(ServerMetrics.class);

  @BeforeClass
  public void setUp()
      throws URISyntaxException, IOException, HttpErrorStatusException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("class.hdfs",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$AlwaysSucceedPinotFS");
    properties.put("class.timeout",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$AlwaysTimeoutPinotFS");
    properties.put("class.existing",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$AlwaysExistPinotFS");
    properties.put("class.overlapping",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$OverlappingPinotFS");
    properties.put("class.singleflight",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$SingleFlightPinotFS");
    properties.put("class.failthen",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$FailThenSucceedPinotFS");
    properties.put("class.disappearing",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$DisappearingPinotFS");
    properties.put("class.interruptible",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$InterruptiblePinotFS");
    properties.put("class.generational",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$GenerationalPinotFS");
    PinotFSFactory.init(new PinotConfiguration(properties));
    _file = FileUtils.getFile(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
    _file.deleteOnExit();
    _llcSegmentName = new LLCSegmentName("test_REALTIME", 1, 0, System.currentTimeMillis());
  }

  @Test
  public void testSuccessfulUpload() {
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("hdfs://root", TIMEOUT_IN_MS, _serverMetrics)) {
      URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
      Assert.assertTrue(segmentURI.toString().startsWith(StringUtil
          .join(File.separator, "hdfs://root", _llcSegmentName.getTableName(), _llcSegmentName.getSegmentName())));
    }
  }

  @Test
  public void testSegmentAlreadyExist() {
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("existing://root", TIMEOUT_IN_MS, _serverMetrics)) {
      URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
      Assert.assertTrue(segmentURI.toString().startsWith(StringUtil
          .join(File.separator, "existing://root", _llcSegmentName.getTableName(), _llcSegmentName.getSegmentName())));
    }
  }

  @Test
  public void testRetryUsesSameDestUri() {
    AlwaysExistPinotFS.resetOperations();
    UUID segmentBuildId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("existing://root", TIMEOUT_IN_MS, _serverMetrics, segmentBuildId)) {
      URI firstSegmentUri = segmentUploader.uploadSegment(_file, _llcSegmentName);
      URI secondSegmentUri = segmentUploader.uploadSegment(_file, _llcSegmentName);

      Assert.assertEquals(secondSegmentUri, firstSegmentUri);
      Assert.assertTrue(firstSegmentUri.toString()
          .endsWith(_llcSegmentName.getSegmentName() + ".tmp." + segmentBuildId));
      Assert.assertEquals(AlwaysExistPinotFS.getOperations(),
          List.of("delete:" + firstSegmentUri, "copy:" + firstSegmentUri));
    }
  }

  @Test
  public void testOverlappingBuildsUseDifferentDestUris()
      throws Exception {
    OverlappingPinotFS.resetUploads();
    PinotFSSegmentUploader firstSegmentUploader =
        new PinotFSSegmentUploader("overlapping://root", TIMEOUT_IN_MS * 10, _serverMetrics,
            UUID.fromString("00000000-0000-0000-0000-000000000001"));
    PinotFSSegmentUploader secondSegmentUploader =
        new PinotFSSegmentUploader("overlapping://root", TIMEOUT_IN_MS * 10, _serverMetrics,
            UUID.fromString("00000000-0000-0000-0000-000000000002"));
    File firstFile = File.createTempFile("first-segment", ".tar");
    File secondFile = File.createTempFile("second-segment", ".tar");
    try {
      FileUtils.write(firstFile, "first payload", StandardCharsets.UTF_8);
      FileUtils.write(secondFile, "second payload", StandardCharsets.UTF_8);

      CompletableFuture<URI> firstUpload = CompletableFuture.supplyAsync(
          () -> firstSegmentUploader.uploadSegment(firstFile, _llcSegmentName));
      CompletableFuture<URI> secondUpload = CompletableFuture.supplyAsync(
          () -> secondSegmentUploader.uploadSegment(secondFile, _llcSegmentName));
      URI firstSegmentUri = firstUpload.get(10, TimeUnit.SECONDS);
      URI secondSegmentUri = secondUpload.get(10, TimeUnit.SECONDS);

      Assert.assertNotEquals(secondSegmentUri, firstSegmentUri);
      Assert.assertTrue(firstSegmentUri.toString().endsWith(".tmp.00000000-0000-0000-0000-000000000001"));
      Assert.assertTrue(secondSegmentUri.toString().endsWith(".tmp.00000000-0000-0000-0000-000000000002"));
      Assert.assertEquals(OverlappingPinotFS.getPayload(firstSegmentUri), "first payload");
      Assert.assertEquals(OverlappingPinotFS.getPayload(secondSegmentUri), "second payload");
    } finally {
      firstSegmentUploader.close();
      secondSegmentUploader.close();
      FileUtils.deleteQuietly(firstFile);
      FileUtils.deleteQuietly(secondFile);
    }
  }

  @Test
  public void testTimedOutRetryJoinsInFlightUpload()
      throws Exception {
    SingleFlightPinotFS.reset();
    UUID segmentBuildId = UUID.fromString("00000000-0000-0000-0000-000000000003");
    CountDownLatch retryJoined = new CountDownLatch(1);
    ExecutorService retryExecutor = Executors.newSingleThreadExecutor();
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("singleflight://root", 25, _serverMetrics, segmentBuildId, Duration.ofHours(1),
            ignored -> retryJoined.countDown())) {
      Assert.assertNull(segmentUploader.uploadSegment(_file, _llcSegmentName));
      Assert.assertTrue(SingleFlightPinotFS.awaitUploadStarted());

      CompletableFuture<URI> retry = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName, TIMEOUT_IN_MS * 10), retryExecutor);
      Assert.assertTrue(retryJoined.await(5, TimeUnit.SECONDS));
      Assert.assertFalse(retry.isDone());
      Assert.assertEquals(SingleFlightPinotFS.getCopyCount(), 1);

      SingleFlightPinotFS.finishUpload();
      URI segmentUri = retry.get(5, TimeUnit.SECONDS);
      Assert.assertTrue(segmentUri.toString().endsWith(".tmp." + segmentBuildId));
      Assert.assertEquals(SingleFlightPinotFS.getCopyCount(), 1);
    } finally {
      SingleFlightPinotFS.finishUpload();
      retryExecutor.shutdownNow();
    }
  }

  @Test
  public void testKeyedRetryJoinsInFlightUploadOnSharedUploader()
      throws Exception {
    SingleFlightPinotFS.reset();
    UUID uploadId = UUID.fromString("00000000-0000-0000-0000-000000000008");
    CountDownLatch retryJoined = new CountDownLatch(1);
    ExecutorService retryExecutor = Executors.newSingleThreadExecutor();
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("singleflight://root", 25, _serverMetrics, null, Duration.ofHours(1),
            ignored -> retryJoined.countDown())) {
      Assert.assertNull(segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId));
      Assert.assertTrue(SingleFlightPinotFS.awaitUploadStarted());

      CompletableFuture<URI> retry = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName, TIMEOUT_IN_MS * 10, uploadId), retryExecutor);
      Assert.assertTrue(retryJoined.await(5, TimeUnit.SECONDS));
      Assert.assertFalse(retry.isDone());
      Assert.assertEquals(SingleFlightPinotFS.getCopyCount(), 1);

      SingleFlightPinotFS.finishUpload();
      URI segmentUri = retry.get(5, TimeUnit.SECONDS);
      Assert.assertTrue(SegmentCompletionUtils.isTmpFile(segmentUri.toString()));
      Assert.assertFalse(segmentUri.toString().endsWith(".tmp." + uploadId));
      Assert.assertEquals(SingleFlightPinotFS.getCopyCount(), 1);
    } finally {
      SingleFlightPinotFS.finishUpload();
      retryExecutor.shutdownNow();
    }
  }

  @Test
  public void testActiveKeyedUploadDoesNotExpireWithCompletedUploadCache()
      throws Exception {
    SingleFlightPinotFS.reset();
    UUID uploadId = UUID.fromString("00000000-0000-0000-0000-000000000013");
    CountDownLatch retryJoined = new CountDownLatch(1);
    ExecutorService retryExecutor = Executors.newSingleThreadExecutor();
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("singleflight://root", 25, _serverMetrics, null, Duration.ofNanos(1),
            ignored -> retryJoined.countDown())) {
      Assert.assertNull(segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId));
      Assert.assertTrue(SingleFlightPinotFS.awaitUploadStarted());

      CompletableFuture<URI> retry = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName, TIMEOUT_IN_MS * 10, uploadId), retryExecutor);
      Assert.assertTrue(retryJoined.await(5, TimeUnit.SECONDS));
      Assert.assertFalse(retry.isDone());
      Assert.assertEquals(SingleFlightPinotFS.getCopyCount(), 1);

      SingleFlightPinotFS.finishUpload();
      Assert.assertNotNull(retry.get(5, TimeUnit.SECONDS));
      Assert.assertEquals(SingleFlightPinotFS.getCopyCount(), 1);
    } finally {
      SingleFlightPinotFS.finishUpload();
      retryExecutor.shutdownNow();
    }
  }

  @Test
  public void testExpiredSharedUploadIdUsesNewDestination()
      throws Exception {
    DisappearingPinotFS.reset();
    UUID uploadId = UUID.fromString("00000000-0000-0000-0000-000000000016");
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("disappearing://root", TIMEOUT_IN_MS, _serverMetrics, null,
            Duration.ofNanos(1))) {
      URI firstUri = segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId);
      Thread.sleep(1);
      URI secondUri = segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId);

      Assert.assertNotEquals(secondUri, firstUri);
      Assert.assertEquals(DisappearingPinotFS.getCopyCount(), 2);
    }
  }

  @Test
  public void testKeyedRetryReusesCompletedUploadUntilControllerMovesIt()
      throws Exception {
    SingleFlightPinotFS.reset();
    UUID uploadId = UUID.fromString("00000000-0000-0000-0000-000000000012");
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("singleflight://root", TIMEOUT_IN_MS, _serverMetrics)) {
      CompletableFuture<URI> firstUpload = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId));
      Assert.assertTrue(SingleFlightPinotFS.awaitUploadStarted());
      SingleFlightPinotFS.finishUpload();
      URI firstUri = firstUpload.get(5, TimeUnit.SECONDS);

      URI retryUri = segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId);
      Assert.assertEquals(retryUri, firstUri);
      Assert.assertEquals(SingleFlightPinotFS.getCopyCount(), 1);

      SingleFlightPinotFS.remove(firstUri);
      URI postMoveRetryUri = segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId);
      Assert.assertNotEquals(postMoveRetryUri, firstUri);
      Assert.assertEquals(SingleFlightPinotFS.getCopyCount(), 2);
    } finally {
      SingleFlightPinotFS.finishUpload();
    }
  }

  @Test
  public void testConcurrentSharedRetriesJoinNewGenerationAfterMove()
      throws Exception {
    GenerationalPinotFS.reset();
    UUID uploadId = UUID.fromString("00000000-0000-0000-0000-000000000014");
    CountDownLatch retryJoined = new CountDownLatch(1);
    ExecutorService retryExecutor = Executors.newFixedThreadPool(2);
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("generational://root", TIMEOUT_IN_MS * 10, _serverMetrics, null,
            Duration.ofHours(1), ignored -> retryJoined.countDown())) {
      URI firstUri = segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId);
      GenerationalPinotFS.remove(firstUri);

      CompletableFuture<URI> firstRetry = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId), retryExecutor);
      Assert.assertTrue(GenerationalPinotFS.awaitSecondCopy());
      CompletableFuture<URI> secondRetry = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId), retryExecutor);
      Assert.assertTrue(retryJoined.await(5, TimeUnit.SECONDS));

      Assert.assertEquals(GenerationalPinotFS.getCopyCount(), 2);
      GenerationalPinotFS.finishSecondCopy();
      URI secondUri = firstRetry.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(secondRetry.get(5, TimeUnit.SECONDS), secondUri);
      Assert.assertNotEquals(secondUri, firstUri);
      Assert.assertEquals(GenerationalPinotFS.getDestinations(), List.of(firstUri, secondUri));
      Assert.assertEquals(GenerationalPinotFS.getDeleteCount(), 0);
    } finally {
      GenerationalPinotFS.finishSecondCopy();
      retryExecutor.shutdownNow();
    }
  }

  @Test
  public void testConcurrentSegmentBuildRetriesJoinNewGenerationAfterMove()
      throws Exception {
    GenerationalPinotFS.reset();
    UUID segmentBuildId = UUID.fromString("00000000-0000-0000-0000-000000000015");
    CountDownLatch retryJoined = new CountDownLatch(1);
    ExecutorService retryExecutor = Executors.newFixedThreadPool(2);
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("generational://root", TIMEOUT_IN_MS * 10, _serverMetrics, segmentBuildId,
            Duration.ofHours(1), ignored -> retryJoined.countDown())) {
      URI firstUri = segmentUploader.uploadSegment(_file, _llcSegmentName);
      GenerationalPinotFS.remove(firstUri);

      CompletableFuture<URI> firstRetry = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName), retryExecutor);
      Assert.assertTrue(GenerationalPinotFS.awaitSecondCopy());
      CompletableFuture<URI> secondRetry = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName), retryExecutor);
      Assert.assertTrue(retryJoined.await(5, TimeUnit.SECONDS));

      Assert.assertEquals(GenerationalPinotFS.getCopyCount(), 2);
      GenerationalPinotFS.finishSecondCopy();
      URI secondUri = firstRetry.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(secondRetry.get(5, TimeUnit.SECONDS), secondUri);
      Assert.assertNotEquals(secondUri, firstUri);
      Assert.assertEquals(GenerationalPinotFS.getDestinations(), List.of(firstUri, secondUri));
      Assert.assertEquals(GenerationalPinotFS.getDeleteCount(), 0);
    } finally {
      GenerationalPinotFS.finishSecondCopy();
      retryExecutor.shutdownNow();
    }
  }

  @Test
  public void testHangingGenerationCheckIsTimeoutBoundedAndSingleFlight()
      throws Exception {
    GenerationalPinotFS.reset();
    UUID segmentBuildId = UUID.fromString("00000000-0000-0000-0000-000000000017");
    CountDownLatch joinedRetries = new CountDownLatch(3);
    ExecutorService retryExecutor = Executors.newFixedThreadPool(2);
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("generational://root", 25, _serverMetrics, segmentBuildId, Duration.ofHours(1),
            ignored -> joinedRetries.countDown())) {
      URI firstUri = segmentUploader.uploadSegment(_file, _llcSegmentName, TIMEOUT_IN_MS);
      GenerationalPinotFS.remove(firstUri);
      GenerationalPinotFS.blockNextExists();

      CompletableFuture<URI> firstRetry = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName));
      Assert.assertTrue(GenerationalPinotFS.awaitBlockedExists());
      URI secondRetry = segmentUploader.uploadSegment(_file, _llcSegmentName);

      Assert.assertNull(firstRetry.get(5, TimeUnit.SECONDS));
      Assert.assertNull(secondRetry);
      Assert.assertEquals(GenerationalPinotFS.getExistsCount(), 2);
      Assert.assertEquals(GenerationalPinotFS.getCopyCount(), 1);
      Assert.assertFalse(GenerationalPinotFS.hasSecondCopyStarted());

      GenerationalPinotFS.releaseBlockedExists();
      Assert.assertTrue(GenerationalPinotFS.awaitSecondCopy());
      CompletableFuture<URI> firstJoin = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName, TIMEOUT_IN_MS * 10), retryExecutor);
      CompletableFuture<URI> secondJoin = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(_file, _llcSegmentName, TIMEOUT_IN_MS * 10), retryExecutor);
      Assert.assertTrue(joinedRetries.await(5, TimeUnit.SECONDS));
      GenerationalPinotFS.finishSecondCopy();

      URI secondUri = firstJoin.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(secondJoin.get(5, TimeUnit.SECONDS), secondUri);
      Assert.assertNotEquals(secondUri, firstUri);
      Assert.assertEquals(GenerationalPinotFS.getCopyCount(), 2);
    } finally {
      GenerationalPinotFS.releaseBlockedExists();
      GenerationalPinotFS.finishSecondCopy();
      retryExecutor.shutdownNow();
    }
  }

  @Test
  public void testKeyedOverlappingBuildsUseDifferentDestUrisOnSharedUploader()
      throws Exception {
    OverlappingPinotFS.resetUploads();
    UUID firstUploadId = UUID.fromString("00000000-0000-0000-0000-000000000009");
    UUID secondUploadId = UUID.fromString("00000000-0000-0000-0000-000000000010");
    File firstFile = File.createTempFile("first-shared-segment", ".tar");
    File secondFile = File.createTempFile("second-shared-segment", ".tar");
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("overlapping://root", TIMEOUT_IN_MS * 10, _serverMetrics)) {
      FileUtils.write(firstFile, "first shared payload", StandardCharsets.UTF_8);
      FileUtils.write(secondFile, "second shared payload", StandardCharsets.UTF_8);

      CompletableFuture<URI> firstUpload = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(firstFile, _llcSegmentName, firstUploadId));
      CompletableFuture<URI> secondUpload = CompletableFuture.supplyAsync(
          () -> segmentUploader.uploadSegment(secondFile, _llcSegmentName, secondUploadId));
      URI firstSegmentUri = firstUpload.get(10, TimeUnit.SECONDS);
      URI secondSegmentUri = secondUpload.get(10, TimeUnit.SECONDS);

      Assert.assertNotEquals(secondSegmentUri, firstSegmentUri);
      Assert.assertTrue(SegmentCompletionUtils.isTmpFile(firstSegmentUri.toString()));
      Assert.assertTrue(SegmentCompletionUtils.isTmpFile(secondSegmentUri.toString()));
      Assert.assertFalse(firstSegmentUri.toString().endsWith(".tmp." + firstUploadId));
      Assert.assertFalse(secondSegmentUri.toString().endsWith(".tmp." + secondUploadId));
      Assert.assertEquals(OverlappingPinotFS.getPayload(firstSegmentUri), "first shared payload");
      Assert.assertEquals(OverlappingPinotFS.getPayload(secondSegmentUri), "second shared payload");
    } finally {
      FileUtils.deleteQuietly(firstFile);
      FileUtils.deleteQuietly(secondFile);
    }
  }

  @Test
  public void testActiveUploadIdCannotBeReusedForDifferentSegment()
      throws Exception {
    SingleFlightPinotFS.reset();
    UUID uploadId = UUID.fromString("00000000-0000-0000-0000-000000000011");
    LLCSegmentName otherSegmentName = new LLCSegmentName("test_REALTIME", 2, 0, System.currentTimeMillis());
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("singleflight://root", 25, _serverMetrics)) {
      Assert.assertNull(segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId));
      Assert.assertTrue(SingleFlightPinotFS.awaitUploadStarted());
      Assert.expectThrows(IllegalArgumentException.class,
          () -> segmentUploader.uploadSegment(_file, otherSegmentName, uploadId));
    } finally {
      SingleFlightPinotFS.finishUpload();
    }
  }

  @Test
  public void testFailedUploadIsReplacedAfterItTerminates() {
    FailThenSucceedPinotFS.reset();
    UUID segmentBuildId = UUID.fromString("00000000-0000-0000-0000-000000000004");
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("failthen://root", TIMEOUT_IN_MS, _serverMetrics, segmentBuildId)) {
      Assert.assertNull(segmentUploader.uploadSegment(_file, _llcSegmentName));
      URI segmentUri = segmentUploader.uploadSegment(_file, _llcSegmentName);
      Assert.assertNotNull(segmentUri);
      Assert.assertEquals(FailThenSucceedPinotFS.getCopyCount(), 2);
    }
  }

  @Test
  public void testCompletedSegmentBuildStartsNewGenerationAfterTempObjectMoved()
      throws Exception {
    DisappearingPinotFS.reset();
    UUID segmentBuildId = UUID.fromString("00000000-0000-0000-0000-000000000005");
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("disappearing://root", TIMEOUT_IN_MS, _serverMetrics, segmentBuildId,
            Duration.ofNanos(1))) {
      URI firstUri = segmentUploader.uploadSegment(_file, _llcSegmentName);
      DisappearingPinotFS.remove(firstUri);
      Thread.sleep(1);
      URI secondUri = segmentUploader.uploadSegment(_file, _llcSegmentName);

      Assert.assertNotEquals(secondUri, firstUri);
      Assert.assertEquals(DisappearingPinotFS.getCopyCount(), 2);
    }
  }

  @Test
  public void testSegmentBuildIdCannotBeReusedForDifferentInput()
      throws IOException {
    UUID segmentBuildId = UUID.fromString("00000000-0000-0000-0000-000000000006");
    File otherFile = File.createTempFile("other-segment", ".tar");
    LLCSegmentName otherSegmentName = new LLCSegmentName("test_REALTIME", 2, 0, System.currentTimeMillis());
    try (PinotFSSegmentUploader fileBoundUploader =
        new PinotFSSegmentUploader("hdfs://root", TIMEOUT_IN_MS, _serverMetrics, segmentBuildId);
        PinotFSSegmentUploader nameBoundUploader =
            new PinotFSSegmentUploader("hdfs://root", TIMEOUT_IN_MS, _serverMetrics, segmentBuildId)) {
      Assert.assertNotNull(fileBoundUploader.uploadSegment(_file, _llcSegmentName));
      Assert.expectThrows(IllegalArgumentException.class,
          () -> fileBoundUploader.uploadSegment(otherFile, _llcSegmentName));

      Assert.assertNotNull(nameBoundUploader.uploadSegment(_file, _llcSegmentName));
      Assert.expectThrows(IllegalArgumentException.class,
          () -> nameBoundUploader.uploadSegment(_file, otherSegmentName));
    } finally {
      FileUtils.deleteQuietly(otherFile);
    }
  }

  @Test
  public void testRetiringUploaderInterruptsAbandonedBuild()
      throws Exception {
    InterruptiblePinotFS.reset();
    PinotFSSegmentUploader segmentUploader = new PinotFSSegmentUploader("interruptible://root", TIMEOUT_IN_MS * 10,
        _serverMetrics, UUID.fromString("00000000-0000-0000-0000-000000000007"));
    CompletableFuture<URI> upload = CompletableFuture.supplyAsync(
        () -> segmentUploader.uploadSegment(_file, _llcSegmentName));
    try {
      Assert.assertTrue(InterruptiblePinotFS.awaitUploadStarted());
      segmentUploader.retire();
      Assert.assertTrue(InterruptiblePinotFS.awaitInterrupted());
      Assert.assertNull(upload.get(5, TimeUnit.SECONDS));
    } finally {
      segmentUploader.close();
    }
  }

  @Test
  public void testUploadTimeOut() {
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("timeout://root", TIMEOUT_IN_MS, _serverMetrics)) {
      URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
      Assert.assertNull(segmentURI);
    }
  }

  @Test
  public void testNoSegmentStoreConfigured() {
    try (PinotFSSegmentUploader segmentUploader =
        new PinotFSSegmentUploader("", TIMEOUT_IN_MS, _serverMetrics)) {
      URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
      Assert.assertNull(segmentURI);
    }
  }

  @Test
  public void testUploadIdMethodsPreserveLegacySubclassOverrides() {
    UUID uploadId = UUID.fromString("00000000-0000-0000-0000-000000000018");
    try (LegacyPinotFSSegmentUploader segmentUploader = new LegacyPinotFSSegmentUploader(_serverMetrics)) {
      Assert.assertEquals(segmentUploader.uploadSegment(_file, _llcSegmentName, uploadId),
          URI.create("legacy://default"));
      Assert.assertEquals(segmentUploader.uploadSegment(_file, _llcSegmentName, TIMEOUT_IN_MS, uploadId),
          URI.create("legacy://timeout"));
      Assert.assertEquals(segmentUploader.getDefaultCallCount(), 1);
      Assert.assertEquals(segmentUploader.getTimeoutCallCount(), 1);
    }
  }

  private static class LegacyPinotFSSegmentUploader extends PinotFSSegmentUploader {
    private final AtomicInteger _defaultCallCount = new AtomicInteger();
    private final AtomicInteger _timeoutCallCount = new AtomicInteger();

    private LegacyPinotFSSegmentUploader(ServerMetrics serverMetrics) {
      super("hdfs://root", TIMEOUT_IN_MS, serverMetrics);
    }

    @Override
    public URI uploadSegment(File segmentFile, LLCSegmentName segmentName) {
      _defaultCallCount.incrementAndGet();
      return URI.create("legacy://default");
    }

    @Override
    public URI uploadSegment(File segmentFile, LLCSegmentName segmentName, int timeoutInMillis) {
      _timeoutCallCount.incrementAndGet();
      return URI.create("legacy://timeout");
    }

    private int getDefaultCallCount() {
      return _defaultCallCount.get();
    }

    private int getTimeoutCallCount() {
      return _timeoutCallCount.get();
    }
  }

  public static class AlwaysSucceedPinotFS extends BasePinotFS {

    @Override
    public void init(PinotConfiguration config) {
    }

    @Override
    public boolean mkdir(URI uri)
        throws IOException {
      return false;
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete)
        throws IOException {
      return false;
    }

    @Override
    public boolean doMove(URI srcUri, URI dstUri)
        throws IOException {
      return false;
    }

    @Override
    public boolean copyDir(URI srcUri, URI dstUri)
        throws IOException {
      return false;
    }

    @Override
    public boolean exists(URI fileUri)
        throws IOException {
      return false;
    }

    @Override
    public long length(URI fileUri)
        throws IOException {
      return 0;
    }

    @Override
    public String[] listFiles(URI fileUri, boolean recursive)
        throws IOException {
      return new String[0];
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
    }

    @Override
    public boolean isDirectory(URI uri)
        throws IOException {
      return false;
    }

    @Override
    public long lastModified(URI uri)
        throws IOException {
      return 0;
    }

    @Override
    public boolean touch(URI uri)
        throws IOException {
      return false;
    }

    @Override
    public InputStream open(URI uri)
        throws IOException {
      return null;
    }
  }

  public static class AlwaysTimeoutPinotFS extends AlwaysSucceedPinotFS {
    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      // Make sure the sleep time > the timeout threshold of uploader.
      Thread.sleep(TIMEOUT_IN_MS * 1000);
    }
  }

  public static class AlwaysExistPinotFS extends AlwaysSucceedPinotFS {
    private static final List<String> OPERATIONS = new CopyOnWriteArrayList<>();

    private static void resetOperations() {
      OPERATIONS.clear();
    }

    private static List<String> getOperations() {
      return List.copyOf(OPERATIONS);
    }

    @Override
    public boolean exists(URI fileUri)
        throws IOException {
      return true;
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete)
        throws IOException {
      OPERATIONS.add("delete:" + segmentUri);
      return true;
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      OPERATIONS.add("copy:" + dstUri);
    }
  }

  public static class OverlappingPinotFS extends AlwaysSucceedPinotFS {
    private static final Map<URI, String> UPLOADS = new ConcurrentHashMap<>();
    private static volatile CountDownLatch _uploadsStarted = new CountDownLatch(2);

    private static void resetUploads() {
      UPLOADS.clear();
      _uploadsStarted = new CountDownLatch(2);
    }

    private static String getPayload(URI uri) {
      return UPLOADS.get(uri);
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      CountDownLatch uploadsStarted = _uploadsStarted;
      uploadsStarted.countDown();
      if (!uploadsStarted.await(5, TimeUnit.SECONDS)) {
        throw new IOException("Timed out waiting for overlapping segment uploads");
      }
      UPLOADS.put(dstUri, FileUtils.readFileToString(srcFile, StandardCharsets.UTF_8));
    }
  }

  public static class SingleFlightPinotFS extends AlwaysSucceedPinotFS {
    private static final AtomicInteger COPY_COUNT = new AtomicInteger();
    private static final Map<URI, Boolean> UPLOADS = new ConcurrentHashMap<>();
    private static volatile CountDownLatch _uploadStarted = new CountDownLatch(1);
    private static volatile CountDownLatch _finishUpload = new CountDownLatch(1);

    private static void reset() {
      COPY_COUNT.set(0);
      UPLOADS.clear();
      _uploadStarted = new CountDownLatch(1);
      _finishUpload = new CountDownLatch(1);
    }

    private static boolean awaitUploadStarted()
        throws InterruptedException {
      return _uploadStarted.await(5, TimeUnit.SECONDS);
    }

    private static void finishUpload() {
      _finishUpload.countDown();
    }

    private static int getCopyCount() {
      return COPY_COUNT.get();
    }

    private static void remove(URI uri) {
      UPLOADS.remove(uri);
    }

    @Override
    public boolean exists(URI fileUri) {
      return UPLOADS.containsKey(fileUri);
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      COPY_COUNT.incrementAndGet();
      _uploadStarted.countDown();
      if (!_finishUpload.await(5, TimeUnit.SECONDS)) {
        throw new IOException("Timed out waiting to finish upload");
      }
      UPLOADS.put(dstUri, true);
    }
  }

  public static class FailThenSucceedPinotFS extends AlwaysSucceedPinotFS {
    private static final AtomicInteger COPY_COUNT = new AtomicInteger();
    private static final Map<URI, Boolean> UPLOADS = new ConcurrentHashMap<>();

    private static void reset() {
      COPY_COUNT.set(0);
      UPLOADS.clear();
    }

    private static int getCopyCount() {
      return COPY_COUNT.get();
    }

    @Override
    public boolean exists(URI fileUri) {
      return UPLOADS.containsKey(fileUri);
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      if (COPY_COUNT.incrementAndGet() == 1) {
        throw new IOException("First upload fails");
      }
      UPLOADS.put(dstUri, true);
    }
  }

  public static class DisappearingPinotFS extends AlwaysSucceedPinotFS {
    private static final AtomicInteger COPY_COUNT = new AtomicInteger();
    private static final Map<URI, Boolean> UPLOADS = new ConcurrentHashMap<>();

    private static void reset() {
      COPY_COUNT.set(0);
      UPLOADS.clear();
    }

    private static void remove(URI uri) {
      UPLOADS.remove(uri);
    }

    private static int getCopyCount() {
      return COPY_COUNT.get();
    }

    @Override
    public boolean exists(URI fileUri) {
      return UPLOADS.containsKey(fileUri);
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete) {
      return UPLOADS.remove(segmentUri) != null;
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri) {
      COPY_COUNT.incrementAndGet();
      UPLOADS.put(dstUri, true);
    }
  }

  public static class InterruptiblePinotFS extends AlwaysSucceedPinotFS {
    private static volatile CountDownLatch _uploadStarted = new CountDownLatch(1);
    private static volatile CountDownLatch _interrupted = new CountDownLatch(1);

    private static void reset() {
      _uploadStarted = new CountDownLatch(1);
      _interrupted = new CountDownLatch(1);
    }

    private static boolean awaitUploadStarted()
        throws InterruptedException {
      return _uploadStarted.await(5, TimeUnit.SECONDS);
    }

    private static boolean awaitInterrupted()
        throws InterruptedException {
      return _interrupted.await(5, TimeUnit.SECONDS);
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      _uploadStarted.countDown();
      try {
        new CountDownLatch(1).await();
      } catch (InterruptedException e) {
        _interrupted.countDown();
        throw e;
      }
    }
  }

  public static class GenerationalPinotFS extends AlwaysSucceedPinotFS {
    private static final AtomicInteger COPY_COUNT = new AtomicInteger();
    private static final AtomicInteger DELETE_COUNT = new AtomicInteger();
    private static final AtomicInteger EXISTS_COUNT = new AtomicInteger();
    private static final AtomicBoolean BLOCK_NEXT_EXISTS = new AtomicBoolean();
    private static final Map<URI, Boolean> UPLOADS = new ConcurrentHashMap<>();
    private static final List<URI> DESTINATIONS = new CopyOnWriteArrayList<>();
    private static volatile CountDownLatch _blockedExistsStarted = new CountDownLatch(1);
    private static volatile CountDownLatch _releaseBlockedExists = new CountDownLatch(1);
    private static volatile CountDownLatch _secondCopyStarted = new CountDownLatch(1);
    private static volatile CountDownLatch _finishSecondCopy = new CountDownLatch(1);

    private static void reset() {
      COPY_COUNT.set(0);
      DELETE_COUNT.set(0);
      EXISTS_COUNT.set(0);
      BLOCK_NEXT_EXISTS.set(false);
      UPLOADS.clear();
      DESTINATIONS.clear();
      _blockedExistsStarted = new CountDownLatch(1);
      _releaseBlockedExists = new CountDownLatch(1);
      _secondCopyStarted = new CountDownLatch(1);
      _finishSecondCopy = new CountDownLatch(1);
    }

    private static void remove(URI uri) {
      UPLOADS.remove(uri);
    }

    private static void blockNextExists() {
      BLOCK_NEXT_EXISTS.set(true);
    }

    private static boolean awaitBlockedExists()
        throws InterruptedException {
      return _blockedExistsStarted.await(5, TimeUnit.SECONDS);
    }

    private static void releaseBlockedExists() {
      _releaseBlockedExists.countDown();
    }

    private static boolean awaitSecondCopy()
        throws InterruptedException {
      return _secondCopyStarted.await(5, TimeUnit.SECONDS);
    }

    private static void finishSecondCopy() {
      _finishSecondCopy.countDown();
    }

    private static boolean hasSecondCopyStarted() {
      return _secondCopyStarted.getCount() == 0;
    }

    private static int getCopyCount() {
      return COPY_COUNT.get();
    }

    private static int getDeleteCount() {
      return DELETE_COUNT.get();
    }

    private static int getExistsCount() {
      return EXISTS_COUNT.get();
    }

    private static List<URI> getDestinations() {
      return List.copyOf(DESTINATIONS);
    }

    @Override
    public boolean exists(URI fileUri)
        throws IOException {
      EXISTS_COUNT.incrementAndGet();
      if (BLOCK_NEXT_EXISTS.compareAndSet(true, false)) {
        _blockedExistsStarted.countDown();
        try {
          if (!_releaseBlockedExists.await(5, TimeUnit.SECONDS)) {
            throw new IOException("Timed out waiting to release existence check");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while checking upload existence", e);
        }
      }
      return UPLOADS.containsKey(fileUri);
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete) {
      DELETE_COUNT.incrementAndGet();
      return UPLOADS.remove(segmentUri) != null;
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      int copyNumber = COPY_COUNT.incrementAndGet();
      DESTINATIONS.add(dstUri);
      if (copyNumber == 2) {
        _secondCopyStarted.countDown();
        if (!_finishSecondCopy.await(5, TimeUnit.SECONDS)) {
          throw new IOException("Timed out waiting to finish second upload generation");
        }
      }
      UPLOADS.put(dstUri, true);
    }
  }
}
