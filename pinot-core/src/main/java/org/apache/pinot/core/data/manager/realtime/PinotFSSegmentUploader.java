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
import java.net.URISyntaxException;
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
    final URI destUri;
    try {
      destUri = new URI(StringUtil.join(File.separator, _segmentStoreUriStr, segmentName.getTableName(),
          SegmentCompletionUtils.generateTmpSegmentFileName(segmentName.getSegmentName())));
    } catch (URISyntaxException e) {
      LOGGER.warn("Failed copy segment tar file {} to segment store {}", segmentFile.getName(), _segmentStoreUriStr,
          e);
      _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_FAILURE, 1);
      return null;
    }
    AtomicReference<AttemptState> state = new AtomicReference<>(AttemptState.IN_FLIGHT);
    Future<URI> future = _executorService.submit(() -> copySegment(segmentFile, destUri, rawTableName, state));
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
      // it already published. Restore the flag after that delete.
      if (abandonUnlessPublished(state)) {
        deleteBestEffort(destUri);
      }
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      if (abandonUnlessPublished(state)) {
        LOGGER.info("Successfully upload segment {} to {}.", segmentName, destUri);
        _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_SUCCESS, 1);
        return destUri;
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

  /// Settles a wait that ended without the worker's result (timeout or interrupt) against the worker's publish.
  /// Returns true when the worker already published, so the object belongs to the caller. Otherwise the attempt is
  /// abandoned (or had already failed) and the worker deletes whatever its copy leaves behind, including a late
  /// success. The caller must not delete in that case: an early delete can race a copy that recreates the object.
  /// Does not cancel the future. An interrupt mid-upload can leave multipart parts this code cannot abort.
  static boolean abandonUnlessPublished(AtomicReference<AttemptState> state) {
    return !state.compareAndSet(AttemptState.IN_FLIGHT, AttemptState.ABANDONED)
        && state.get() == AttemptState.PUBLISHED;
  }

  private URI copySegment(File segmentFile, URI destUri, String rawTableName, AtomicReference<AttemptState> state) {
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

  private void deleteBestEffort(URI destUri) {
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
}
