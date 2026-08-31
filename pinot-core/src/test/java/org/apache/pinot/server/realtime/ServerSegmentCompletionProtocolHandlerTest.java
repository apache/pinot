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
package org.apache.pinot.server.realtime;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest;
import org.apache.pinot.core.data.manager.realtime.SegmentCompletionUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests retry identity and production wiring for server-side segment uploads.
public class ServerSegmentCompletionProtocolHandlerTest {
  private static final String BLOCKING_PINOT_FS_SCHEME = "reingestion-blocking";
  private static final String RECORDING_PINOT_FS_SCHEME = "reingestion-recording";

  @Test
  public void testReingestionCoordinationIdIsScopedToJobAndServerProcess() {
    String segmentName = new LLCSegmentName("test_REALTIME", 1, 0, 1234L).getSegmentName();
    UUID firstJobId = UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID secondJobId = UUID.fromString("00000000-0000-0000-0000-000000000002");

    UUID firstId = ServerSegmentCompletionProtocolHandler.getReingestionUploadCoordinationId("test", segmentName,
        "server-process-one", firstJobId);
    UUID retryId = ServerSegmentCompletionProtocolHandler.getReingestionUploadCoordinationId("test", segmentName,
        "server-process-one", firstJobId);
    UUID rebuiltSegmentId = ServerSegmentCompletionProtocolHandler.getReingestionUploadCoordinationId("test",
        segmentName, "server-process-one", secondJobId);
    UUID restartedServerId = ServerSegmentCompletionProtocolHandler.getReingestionUploadCoordinationId("test",
        segmentName, "server-process-two", firstJobId);

    Assert.assertEquals(retryId, firstId);
    Assert.assertNotEquals(rebuiltSegmentId, firstId);
    Assert.assertNotEquals(restartedServerId, firstId);
  }

  @Test
  public void testSequentialReingestionAttemptsUseDifferentDestinations() {
    String segmentName = new LLCSegmentName("test_REALTIME", 1, 0, 1234L).getSegmentName();
    UUID firstAttemptId = UUID.fromString("00000000-0000-0000-0000-000000000003");
    UUID secondAttemptId = UUID.fromString("00000000-0000-0000-0000-000000000004");

    URI firstUri = ServerSegmentCompletionProtocolHandler.getReingestedSegmentUploadURI("s3://root", "test",
        segmentName, firstAttemptId);
    URI secondUri = ServerSegmentCompletionProtocolHandler.getReingestedSegmentUploadURI("s3://root", "test",
        segmentName, secondAttemptId);

    Assert.assertNotEquals(secondUri, firstUri);
    Assert.assertTrue(firstUri.toString().endsWith(".tmp." + firstAttemptId));
    Assert.assertTrue(secondUri.toString().endsWith(".tmp." + secondAttemptId));
  }

  @Test
  public void testOverlappingReingestionUploadForSameJobIsRejected()
      throws Exception {
    UUID reingestionBuildId = UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID uploadCoordinationId = ServerSegmentCompletionProtocolHandler.getReingestionUploadCoordinationId("test",
        "segment", "server-process", reingestionBuildId);
    CountDownLatch firstUploadStarted = new CountDownLatch(1);
    CountDownLatch finishFirstUpload = new CountDownLatch(1);
    AtomicBoolean secondUploadRan = new AtomicBoolean();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> firstUpload = executor.submit(
          () -> ServerSegmentCompletionProtocolHandler.tryWithReingestionUploadLock(uploadCoordinationId, () -> {
            firstUploadStarted.countDown();
            if (!finishFirstUpload.await(5, TimeUnit.SECONDS)) {
              throw new IllegalStateException("Timed out waiting to finish the first upload");
            }
          }));
      Assert.assertTrue(firstUploadStarted.await(5, TimeUnit.SECONDS));

      Assert.assertFalse(ServerSegmentCompletionProtocolHandler.tryWithReingestionUploadLock(uploadCoordinationId,
          () -> secondUploadRan.set(true)));
      Assert.assertFalse(secondUploadRan.get());

      finishFirstUpload.countDown();
      Assert.assertTrue(firstUpload.get(5, TimeUnit.SECONDS));
    } finally {
      finishFirstUpload.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testSequentialReingestionAttemptsWithSameJobAreBothAccepted()
      throws Exception {
    UUID uploadCoordinationId = UUID.fromString("00000000-0000-0000-0000-000000000005");
    URI firstUri = URI.create("s3://root/test/segment.tmp.00000000-0000-0000-0000-000000000006");
    URI secondUri = URI.create("s3://root/test/segment.tmp.00000000-0000-0000-0000-000000000007");
    List<URI> copiedDestinations = new CopyOnWriteArrayList<>();

    Assert.assertTrue(ServerSegmentCompletionProtocolHandler.tryWithReingestionUploadLock(uploadCoordinationId,
        () -> copiedDestinations.add(firstUri)));
    Assert.assertTrue(ServerSegmentCompletionProtocolHandler.tryWithReingestionUploadLock(uploadCoordinationId,
        () -> copiedDestinations.add(secondUri)));
    Assert.assertEquals(copiedDestinations, List.of(firstUri, secondUri));
  }

  @Test
  public void testPublicReingestionUploadRejectsOverlappingBuildId()
      throws Exception {
    PinotFSFactory.register(BLOCKING_PINOT_FS_SCHEME, BlockingReingestionPinotFS.class.getName(),
        new PinotConfiguration());
    BlockingReingestionPinotFS.reset();
    ServerSegmentCompletionProtocolHandler handler =
        new ServerSegmentCompletionProtocolHandler(Mockito.mock(ServerMetrics.class), "test_REALTIME");
    String segmentName = new LLCSegmentName("test_REALTIME", 1, 0, 1234L).getSegmentName();
    UUID reingestionBuildId = UUID.fromString("00000000-0000-0000-0000-000000000008");
    File segmentFile = File.createTempFile("reingestion-overlap", ".tar.gz");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Exception> firstUpload = executor.submit(() -> {
        try {
          handler.uploadReingestedSegment(segmentName, BLOCKING_PINOT_FS_SCHEME + "://root", segmentFile,
              reingestionBuildId);
          return null;
        } catch (Exception e) {
          return e;
        }
      });
      Assert.assertTrue(BlockingReingestionPinotFS.awaitUploadStarted());

      IOException overlappingUpload = Assert.expectThrows(IOException.class,
          () -> handler.uploadReingestedSegment(segmentName, BLOCKING_PINOT_FS_SCHEME + "://root", segmentFile,
              reingestionBuildId));
      Assert.assertTrue(overlappingUpload.getMessage().contains("already in progress"));
      Assert.assertEquals(BlockingReingestionPinotFS.getDestinations().size(), 1);

      BlockingReingestionPinotFS.releaseUpload();
      Assert.assertTrue(firstUpload.get(5, TimeUnit.SECONDS) instanceof IOException);
    } finally {
      BlockingReingestionPinotFS.releaseUpload();
      executor.shutdownNow();
      FileUtils.deleteQuietly(segmentFile);
    }
  }

  @Test
  public void testPublicSequentialReingestionUploadsUseFreshDestinations()
      throws Exception {
    PinotFSFactory.register(RECORDING_PINOT_FS_SCHEME, RecordingReingestionPinotFS.class.getName(),
        new PinotConfiguration());
    RecordingReingestionPinotFS.reset();
    ServerSegmentCompletionProtocolHandler handler =
        new ServerSegmentCompletionProtocolHandler(Mockito.mock(ServerMetrics.class), "test_REALTIME");
    String segmentName = new LLCSegmentName("test_REALTIME", 1, 0, 1234L).getSegmentName();
    UUID reingestionBuildId = UUID.fromString("00000000-0000-0000-0000-000000000009");
    File segmentFile = File.createTempFile("reingestion-sequential", ".tar.gz");
    try {
      Assert.expectThrows(IOException.class,
          () -> handler.uploadReingestedSegment(segmentName, RECORDING_PINOT_FS_SCHEME + "://root", segmentFile,
              reingestionBuildId));
      Assert.expectThrows(IOException.class,
          () -> handler.uploadReingestedSegment(segmentName, RECORDING_PINOT_FS_SCHEME + "://root", segmentFile,
              reingestionBuildId));

      List<URI> destinations = RecordingReingestionPinotFS.getDestinations();
      Assert.assertEquals(destinations.size(), 2);
      Assert.assertNotEquals(destinations.get(1), destinations.get(0));
      Assert.assertTrue(SegmentCompletionUtils.isTmpFile(destinations.get(0).toString()));
      Assert.assertTrue(SegmentCompletionUtils.isTmpFile(destinations.get(1).toString()));
    } finally {
      FileUtils.deleteQuietly(segmentFile);
    }
  }

  /// Test filesystem that holds one public reingestion call inside the build-ID coordination lock.
  public static class BlockingReingestionPinotFS extends PinotFSSegmentUploaderTest.AlwaysSucceedPinotFS {
    private static final List<URI> DESTINATIONS = new CopyOnWriteArrayList<>();
    private static volatile CountDownLatch _uploadStarted = new CountDownLatch(1);
    private static volatile CountDownLatch _releaseUpload = new CountDownLatch(1);

    private static void reset() {
      DESTINATIONS.clear();
      _uploadStarted = new CountDownLatch(1);
      _releaseUpload = new CountDownLatch(1);
    }

    private static boolean awaitUploadStarted()
        throws InterruptedException {
      return _uploadStarted.await(5, TimeUnit.SECONDS);
    }

    private static void releaseUpload() {
      _releaseUpload.countDown();
    }

    private static List<URI> getDestinations() {
      return List.copyOf(DESTINATIONS);
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      DESTINATIONS.add(dstUri);
      _uploadStarted.countDown();
      _releaseUpload.await();
      throw new IOException("Stop after recording the test upload destination");
    }
  }

  /// Test filesystem that records each destination before stopping the upload ahead of controller I/O.
  public static class RecordingReingestionPinotFS extends PinotFSSegmentUploaderTest.AlwaysSucceedPinotFS {
    private static final List<URI> DESTINATIONS = new CopyOnWriteArrayList<>();

    private static void reset() {
      DESTINATIONS.clear();
    }

    private static List<URI> getDestinations() {
      return List.copyOf(DESTINATIONS);
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws IOException {
      DESTINATIONS.add(dstUri);
      throw new IOException("Stop after recording the test upload destination");
    }
  }
}
