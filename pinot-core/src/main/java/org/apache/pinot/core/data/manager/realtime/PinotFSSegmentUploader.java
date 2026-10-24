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
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Uploads a segment to the configured segment store with PinotFS, bounded by a timeout.
/// Each attempt uses `{segment}.tmp.{uuid}`. The object is deleted only when that attempt's URI is not returned.
/// A timed-out copy that is still in flight is abandoned. The worker deletes it after the copy finishes, including a
/// late success. A copy that published before the timeout is returned and kept.
public class PinotFSSegmentUploader implements SegmentUploader {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSSegmentUploader.class);
  public static final int DEFAULT_SEGMENT_UPLOAD_TIMEOUT_MILLIS = 10 * 1000;

  /// Latches and callbacks used by tests to force the timeout and interrupt races. Production leaves this null.
  static volatile TestHooks _testHooks;

  private final String _segmentStoreUriStr;
  private final ExecutorService _executorService = Executors.newCachedThreadPool();
  private final int _timeoutInMs;
  private final ServerMetrics _serverMetrics;

  /// Per-attempt handoff. The state stays on the call, not on this uploader, because one uploader is shared across
  /// segments and a field would let one timeout delete another segment's object.
  enum AttemptState {
    IN_FLIGHT,
    PUBLISHED,
    ABANDONED,
    FAILED
  }

  public PinotFSSegmentUploader(String segmentStoreDirUri, int timeoutMillis, ServerMetrics serverMetrics) {
    _segmentStoreUriStr = segmentStoreDirUri;
    _timeoutInMs = timeoutMillis;
    _serverMetrics = serverMetrics;
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName) {
    return uploadSegment(segmentFile, segmentName, _timeoutInMs);
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName, int timeoutInMillis) {
    if (_segmentStoreUriStr == null || _segmentStoreUriStr.isEmpty()) {
      LOGGER.error("Missing segment store uri. Failed to upload segment file {} for {}.", segmentFile.getName(),
          segmentName.getSegmentName());
      return null;
    }
    final String rawTableName = TableNameBuilder.extractRawTableName(segmentName.getTableName());
    AtomicReference<AttemptState> state = new AtomicReference<>(AttemptState.IN_FLIGHT);
    AtomicReference<URI> destUriRef = new AtomicReference<>();
    Callable<URI> uploadTask = () -> copySegment(segmentFile, segmentName, rawTableName, state, destUriRef);
    Future<URI> future = _executorService.submit(uploadTask);
    try {
      URI segmentLocation = future.get(timeoutInMillis, TimeUnit.MILLISECONDS);
      if (segmentLocation != null) {
        LOGGER.info("Successfully upload segment {} to {}.", segmentName, segmentLocation);
        _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_SUCCESS, 1);
        return segmentLocation;
      }
    } catch (InterruptedException e) {
      LOGGER.info("Interrupted while waiting for segment upload of {} to {}.", segmentName, _segmentStoreUriStr);
      // future.get already cleared the interrupt flag. Leave it clear until a published object is deleted.
      // S3 and HDFS abort a delete when the calling thread is interrupted, and the worker will not delete a URI
      // this thread already published. Restore the flag after that delete.
      if (!state.compareAndSet(AttemptState.IN_FLIGHT, AttemptState.ABANDONED)
          && state.get() == AttemptState.PUBLISHED) {
        deleteBestEffort(destUriRef.get());
      }
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      URI publishedUri = resolveTimedOutAttempt(state, destUriRef);
      if (publishedUri != null) {
        LOGGER.info("Successfully upload segment {} to {}.", segmentName, publishedUri);
        _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_SUCCESS, 1);
        return publishedUri;
      }
      _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_TIMEOUT, 1);
      LOGGER.warn("Timed out waiting to upload segment: {} for table: {}", segmentName.getSegmentName(), rawTableName);
    } catch (Exception e) {
      LOGGER.warn("Failed to upload file {} of segment {} for table {}",
          segmentFile.getAbsolutePath(), segmentName, rawTableName, e);
    }
    _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_FAILURE, 1);
    return null;
  }

  /// Caller side of a timed-out `future.get`. Returns the published URI when the copy won the race with the deadline.
  /// The URI is read after that CAS loses. The worker stores it before its own publish CAS, so a timeout that fired
  /// while the ref was still empty still observes the URI when the worker has published. Do not pass a URI captured
  /// before the CAS.
  /// The caller must not delete. A returned URI is handed to the controller. An abandoned attempt is deleted by the
  /// worker after the copy finishes, including a late success. An early delete can race a copy that recreates the
  /// object. Does not cancel the future. An interrupt mid-upload can leave multipart parts this code cannot abort.
  static URI resolveTimedOutAttempt(AtomicReference<AttemptState> state, AtomicReference<URI> destUriRef) {
    TestHooks hooks = _testHooks;
    if (hooks != null && hooks._beforeTimeoutCas != null) {
      hooks._beforeTimeoutCas.run();
    }
    if (state.compareAndSet(AttemptState.IN_FLIGHT, AttemptState.ABANDONED)) {
      return null;
    }
    if (hooks != null && hooks._afterTimeoutCasLost != null) {
      hooks._afterTimeoutCasLost.run();
    }
    if (state.get() == AttemptState.PUBLISHED) {
      return destUriRef.get();
    }
    return null;
  }

  private URI copySegment(File segmentFile, LLCSegmentName segmentName, String rawTableName,
      AtomicReference<AttemptState> state, AtomicReference<URI> destUriRef) {
    URI destUri;
    try {
      destUri = new URI(StringUtil.join(File.separator, _segmentStoreUriStr, segmentName.getTableName(),
          SegmentCompletionUtils.generateTmpSegmentFileName(segmentName.getSegmentName())));
    } catch (Exception e) {
      state.compareAndSet(AttemptState.IN_FLIGHT, AttemptState.FAILED);
      LOGGER.warn("Failed copy segment tar file {} to segment store {}", segmentFile.getName(), _segmentStoreUriStr,
          e);
      return null;
    }
    // Minted inside the task. future.get throws the URI away on timeout, so the caller cannot rebuild it.
    // The store happens before the publish CAS. A timeout that loses that CAS must read the ref afterwards.
    awaitBeforeDestUriStoreForTest();
    destUriRef.set(destUri);
    long startTime = System.currentTimeMillis();
    try {
      PinotFS pinotFS = openSegmentStore(segmentFile, destUri, state);
      if (pinotFS == null) {
        return null;
      }
      try {
        pinotFS.copyFromLocalFile(segmentFile, destUri);
      } catch (Exception e) {
        // The SDK may have stored the object and then thrown. Delete that object, and do not let the delete
        // replace the copy failure.
        deleteBestEffort(pinotFS, destUri);
        state.compareAndSet(AttemptState.IN_FLIGHT, AttemptState.FAILED);
        LOGGER.warn("Failed copy segment tar file {} to segment store {}", segmentFile.getName(), destUri, e);
        return null;
      }
      if (state.compareAndSet(AttemptState.IN_FLIGHT, AttemptState.PUBLISHED)) {
        pauseAfterPublishForTest();
        return destUri;
      }
      // The caller already abandoned this attempt and will not return the URI.
      deleteBestEffort(pinotFS, destUri);
      return null;
    } finally {
      long duration = System.currentTimeMillis() - startTime;
      _serverMetrics.addTimedTableValue(rawTableName, ServerTimer.SEGMENT_UPLOAD_TIME_MS, duration,
          TimeUnit.MILLISECONDS);
    }
  }

  /// Returns null when the filesystem cannot be opened. Nothing was stored, so there is nothing to delete.
  private PinotFS openSegmentStore(File segmentFile, URI destUri, AtomicReference<AttemptState> state) {
    try {
      return PinotFSFactory.create(new URI(_segmentStoreUriStr).getScheme());
    } catch (Exception e) {
      state.compareAndSet(AttemptState.IN_FLIGHT, AttemptState.FAILED);
      LOGGER.warn("Failed copy segment tar file {} to segment store {}", segmentFile.getName(), destUri, e);
      return null;
    }
  }

  /// Blocks before `destUriRef` is stored so a test can time out while the ref is still empty.
  private static void awaitBeforeDestUriStoreForTest() {
    TestHooks hooks = _testHooks;
    if (hooks == null) {
      return;
    }
    CountDownLatch entered = hooks._enteredBeforeDestUriStore;
    if (entered != null) {
      entered.countDown();
    }
    CountDownLatch release = hooks._releaseBeforeDestUriStore;
    if (release != null) {
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /// Pauses after the publish CAS and before the worker returns, so a test can interrupt the caller while the future
  /// is still pending and the state is already `PUBLISHED`.
  private static void pauseAfterPublishForTest() {
    TestHooks hooks = _testHooks;
    if (hooks == null) {
      return;
    }
    CountDownLatch entered = hooks._enteredAfterPublish;
    if (entered != null) {
      entered.countDown();
    }
    CountDownLatch pause = hooks._pauseAfterPublish;
    if (pause != null) {
      try {
        pause.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    CountDownLatch resumed = hooks._resumedAfterPublish;
    if (resumed != null) {
      resumed.countDown();
    }
  }

  private void deleteBestEffort(URI destUri) {
    if (destUri == null) {
      return;
    }
    try {
      PinotFS pinotFS = PinotFSFactory.create(new URI(_segmentStoreUriStr).getScheme());
      deleteBestEffort(pinotFS, destUri);
    } catch (Exception e) {
      LOGGER.warn("Failed to delete temporary segment file: {}", destUri, e);
    }
  }

  private static void deleteBestEffort(PinotFS pinotFS, URI destUri) {
    try {
      // Local and GCS return false for a missing object. ADLS and S3 can throw. Ignore the boolean.
      pinotFS.delete(destUri, true);
    } catch (Exception e) {
      LOGGER.warn("Failed to delete temporary segment file: {}", destUri, e);
    }
  }

  /// Test-only gates for the timeout and interrupt races. Production leaves [PinotFSSegmentUploader#_testHooks] null,
  /// and every latch here stays null unless a test sets it.
  static final class TestHooks {
    /// Runs at the start of [PinotFSSegmentUploader#resolveTimedOutAttempt], before the abandon CAS.
    Runnable _beforeTimeoutCas;
    /// Runs after the abandon CAS loses and before the published URI is read.
    Runnable _afterTimeoutCasLost;
    /// Counted down when the worker is about to store the destination URI.
    CountDownLatch _enteredBeforeDestUriStore;
    /// The worker waits on this before storing the destination URI.
    CountDownLatch _releaseBeforeDestUriStore;
    /// Counted down after the publish CAS, before the worker returns the URI.
    CountDownLatch _enteredAfterPublish;
    /// The worker waits on this after the publish CAS.
    CountDownLatch _pauseAfterPublish;
    /// Counted down once the worker leaves the post-publish pause.
    CountDownLatch _resumedAfterPublish;
  }
}
