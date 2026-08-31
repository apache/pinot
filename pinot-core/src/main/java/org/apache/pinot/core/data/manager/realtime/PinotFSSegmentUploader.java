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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.annotation.Nullable;
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


/// A segment uploader which uploads to a segment store (with store root dir configured as \_segmentStoreUriStr) using
/// PinotFS within a configurable timeout period. Calls for one segment build join the same active upload and reuse its
/// temporary URI while it exists. Once the controller consumes that URI, the same build advances to a fresh generation
/// instead of rewriting a location that an earlier controller request may still be moving.
public class PinotFSSegmentUploader implements SegmentUploader, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSSegmentUploader.class);
  public static final int DEFAULT_SEGMENT_UPLOAD_TIMEOUT_MILLIS = 10 * 1000;
  private static final int MAX_COMPLETED_KEYED_UPLOADS = 10_000;
  private static final Consumer<UUID> NO_OP_KEYED_UPLOAD_JOIN_CALLBACK = ignored -> { };

  private final String _segmentStoreUriStr;
  private final ExecutorService _executorService = Executors.newCachedThreadPool();
  private final int _timeoutInMs;
  private final ServerMetrics _serverMetrics;
  // Test hook invoked after a retry has selected an active single-flight operation.
  private final Consumer<UUID> _keyedUploadJoinCallback;
  @Nullable
  private final UUID _segmentBuildId;
  // Active uploads must never expire: a pathological object-store request can outlive normal retry windows, and a
  // second writer for the same operation would race it. Completed shared operations cover the bounded HTTP retry
  // window; descriptor-bound completions live for the uploader lifetime.
  private final ConcurrentMap<UUID, KeyedUpload> _activeKeyedUploads = new ConcurrentHashMap<>();
  // A descriptor-bound uploader lives only for one build. Its successful ID must remain a tombstone for that entire
  // lifetime because the controller move has no finite upper bound.
  private final ConcurrentMap<UUID, KeyedUpload> _completedSegmentBuildUploads = new ConcurrentHashMap<>();
  private final Cache<UUID, KeyedUpload> _completedKeyedUploads;
  private final LoadingCache<UUID, Lock> _keyedUploadLocks =
      CacheBuilder.newBuilder().weakValues().build(new CacheLoader<>() {
        @Override
        public Lock load(UUID uploadId) {
          return new ReentrantLock();
        }
      });
  @Nullable
  private String _boundSegmentName;
  @Nullable
  private String _boundSegmentFilePath;

  public PinotFSSegmentUploader(String segmentStoreDirUri, int timeoutMillis, ServerMetrics serverMetrics) {
    this(segmentStoreDirUri, timeoutMillis, serverMetrics, null);
  }

  PinotFSSegmentUploader(String segmentStoreDirUri, int timeoutMillis, ServerMetrics serverMetrics,
      @Nullable UUID segmentBuildId) {
    this(segmentStoreDirUri, timeoutMillis, serverMetrics, segmentBuildId, Duration.ofHours(1));
  }

  PinotFSSegmentUploader(String segmentStoreDirUri, int timeoutMillis, ServerMetrics serverMetrics,
      @Nullable UUID segmentBuildId, Duration completedUploadRetention) {
    this(segmentStoreDirUri, timeoutMillis, serverMetrics, segmentBuildId, completedUploadRetention,
        NO_OP_KEYED_UPLOAD_JOIN_CALLBACK);
  }

  @VisibleForTesting
  PinotFSSegmentUploader(String segmentStoreDirUri, int timeoutMillis, ServerMetrics serverMetrics,
      @Nullable UUID segmentBuildId, Duration completedUploadRetention, Consumer<UUID> keyedUploadJoinCallback) {
    _segmentStoreUriStr = segmentStoreDirUri;
    _timeoutInMs = timeoutMillis;
    _serverMetrics = serverMetrics;
    _segmentBuildId = segmentBuildId;
    _keyedUploadJoinCallback = Preconditions.checkNotNull(keyedUploadJoinCallback);
    _completedKeyedUploads = CacheBuilder.newBuilder().maximumSize(MAX_COMPLETED_KEYED_UPLOADS)
        .expireAfterWrite(completedUploadRetention).build();
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName) {
    return uploadSegment(segmentFile, segmentName, _timeoutInMs);
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName, int timeoutInMillis) {
    bindToSegmentBuild(segmentFile, segmentName);
    return uploadSegment(segmentFile, segmentName, timeoutInMillis,
        _segmentBuildId != null
            ? SegmentCompletionUtils.generateTmpSegmentFileName(segmentName.getSegmentName(), _segmentBuildId)
            : SegmentCompletionUtils.generateTmpSegmentFileName(segmentName.getSegmentName()), _segmentBuildId);
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName, UUID uploadId) {
    if (_segmentBuildId == null && getClass() != PinotFSSegmentUploader.class) {
      // Preserve virtual dispatch for subclasses that override the original public method.
      return uploadSegment(segmentFile, segmentName);
    }
    return uploadSegment(segmentFile, segmentName, _timeoutInMs, uploadId);
  }

  @Override
  public URI uploadSegment(File segmentFile, LLCSegmentName segmentName, int timeoutInMillis, UUID uploadId) {
    if (_segmentBuildId == null && getClass() != PinotFSSegmentUploader.class) {
      // Preserve virtual dispatch for subclasses that override the original public method.
      return uploadSegment(segmentFile, segmentName, timeoutInMillis);
    }
    Preconditions.checkArgument(_segmentBuildId == null || _segmentBuildId.equals(uploadId),
        "Uploader for segment build %s cannot upload with ID %s", _segmentBuildId, uploadId);
    if (_segmentBuildId != null) {
      return uploadSegment(segmentFile, segmentName, timeoutInMillis);
    }
    return uploadSegment(segmentFile, segmentName, timeoutInMillis,
        SegmentCompletionUtils.generateTmpSegmentFileName(segmentName.getSegmentName()), uploadId);
  }

  private URI uploadSegment(File segmentFile, LLCSegmentName segmentName, int timeoutInMillis,
      String tmpSegmentFileName, @Nullable UUID keyedUploadId) {
    if (_segmentStoreUriStr == null || _segmentStoreUriStr.isEmpty()) {
      LOGGER.error("Missing segment store uri. Failed to upload segment file {} for {}.", segmentFile.getName(),
          segmentName.getSegmentName());
      return null;
    }
    final String rawTableName = TableNameBuilder.extractRawTableName(segmentName.getTableName());
    URI destUri;
    try {
      destUri = new URI(StringUtil.join(File.separator, _segmentStoreUriStr, segmentName.getTableName(),
          tmpSegmentFileName));
    } catch (Exception e) {
      LOGGER.warn("Failed to create segment store URI for segment {}: {}", segmentName, e.getMessage());
      _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_FAILURE, 1);
      return null;
    }
    Callable<URI> uploadTask = () -> uploadSegmentFile(segmentFile, destUri, rawTableName);
    Callable<URI> nextGenerationUploadTask = () -> uploadSegmentFile(segmentFile,
        getSegmentUri(segmentName, SegmentCompletionUtils.generateTmpSegmentFileName(segmentName.getSegmentName())),
        rawTableName);
    Future<URI> future = keyedUploadId != null
        ? getOrStartKeyedUpload(keyedUploadId, segmentName, uploadTask, nextGenerationUploadTask)
        : _executorService.submit(uploadTask);
    try {
      URI segmentLocation = future.get(timeoutInMillis, TimeUnit.MILLISECONDS);
      LOGGER.info("Successfully upload segment {} to {}.", segmentName, segmentLocation);
      _serverMetrics.addMeteredTableValue(rawTableName,
          segmentLocation == null ? ServerMeter.SEGMENT_UPLOAD_FAILURE : ServerMeter.SEGMENT_UPLOAD_SUCCESS, 1);
      return segmentLocation;
    } catch (InterruptedException e) {
      LOGGER.info("Interrupted while waiting for segment upload of {} to {}.", segmentName, _segmentStoreUriStr);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      // Emit a separate metric for timeout since this is relatively more common than other errors.
      _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_TIMEOUT, 1);
      LOGGER.warn("Timed out waiting to upload segment: {} for table: {}", segmentName.getSegmentName(), rawTableName);
    } catch (Exception e) {
      LOGGER.warn("Failed to upload file {} of segment {} for table {}",
              segmentFile.getAbsolutePath(), segmentName, rawTableName, e);
    }
    _serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_FAILURE, 1);

    return null;
  }

  @Nullable
  private URI uploadSegmentFile(File segmentFile, URI destUri, String rawTableName) {
    long startTime = System.currentTimeMillis();
    try {
      PinotFS pinotFS = PinotFSFactory.create(new URI(_segmentStoreUriStr).getScheme());
      // Delete only before the first write to this generation. Once its URI is returned, it is never written again.
      if (pinotFS.exists(destUri) && !pinotFS.delete(destUri, true)) {
        throw new IOException("Failed to delete existing temporary segment at " + destUri);
      }
      pinotFS.copyFromLocalFile(segmentFile, destUri);
      return destUri;
    } catch (Exception e) {
      LOGGER.warn("Failed copy segment tar file {} to segment store {}: {}", segmentFile.getName(), destUri, e);
      return null;
    } finally {
      long duration = System.currentTimeMillis() - startTime;
      _serverMetrics.addTimedTableValue(rawTableName, ServerTimer.SEGMENT_UPLOAD_TIME_MS, duration,
          TimeUnit.MILLISECONDS);
    }
  }

  private URI getSegmentUri(LLCSegmentName segmentName, String tmpSegmentFileName)
      throws URISyntaxException {
    return new URI(StringUtil.join(File.separator, _segmentStoreUriStr, segmentName.getTableName(),
        tmpSegmentFileName));
  }

  private Future<URI> getOrStartKeyedUpload(UUID uploadId, LLCSegmentName segmentName, Callable<URI> uploadTask,
      Callable<URI> nextGenerationUploadTask) {
    Lock uploadLock = _keyedUploadLocks.getUnchecked(uploadId);
    uploadLock.lock();
    try {
      KeyedUpload activeUpload = _activeKeyedUploads.get(uploadId);
      if (activeUpload != null) {
        activeUpload.validateInput(segmentName);
        _keyedUploadJoinCallback.accept(uploadId);
        return activeUpload._future;
      }

      KeyedUpload completedUpload = getCompletedUpload(uploadId);
      if (completedUpload != null) {
        completedUpload.validateInput(segmentName);
        removeCompletedUpload(uploadId, completedUpload);
      }

      KeyedUpload newUpload = new KeyedUpload(segmentName);
      _activeKeyedUploads.put(uploadId, newUpload);
      startKeyedOperation(uploadId, newUpload,
          completedUpload == null ? uploadTask
              : () -> reuseOrUploadNextGeneration(completedUpload, nextGenerationUploadTask), completedUpload);
      return newUpload._future;
    } finally {
      uploadLock.unlock();
    }
  }

  private void startKeyedOperation(UUID uploadId, KeyedUpload keyedUpload, Callable<URI> operation,
      @Nullable KeyedUpload completedUploadToRestore) {
    try {
      _executorService.submit(() -> {
        URI completedLocation = null;
        Throwable failure = null;
        try {
          completedLocation = operation.call();
        } catch (Throwable t) {
          failure = t;
        }

        Lock uploadLock = _keyedUploadLocks.getUnchecked(uploadId);
        uploadLock.lock();
        try {
          if (completedLocation != null) {
            putCompletedUpload(uploadId, keyedUpload);
          } else if (completedUploadToRestore != null) {
            putCompletedUpload(uploadId, completedUploadToRestore);
          }
          if (failure == null) {
            keyedUpload._future.complete(completedLocation);
          } else {
            keyedUpload._future.completeExceptionally(failure);
          }
        } finally {
          _activeKeyedUploads.remove(uploadId, keyedUpload);
          uploadLock.unlock();
        }
      });
    } catch (RuntimeException e) {
      Lock uploadLock = _keyedUploadLocks.getUnchecked(uploadId);
      uploadLock.lock();
      try {
        if (completedUploadToRestore != null) {
          putCompletedUpload(uploadId, completedUploadToRestore);
        }
        keyedUpload._future.completeExceptionally(e);
        _activeKeyedUploads.remove(uploadId, keyedUpload);
      } finally {
        uploadLock.unlock();
      }
    }
  }

  @Nullable
  private URI reuseOrUploadNextGeneration(KeyedUpload completedUpload, Callable<URI> nextGenerationUploadTask) {
    URI completedLocation;
    try {
      completedLocation = completedUpload._future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (Exception e) {
      return null;
    }
    try {
      PinotFS pinotFS = PinotFSFactory.create(new URI(_segmentStoreUriStr).getScheme());
      if (pinotFS.exists(completedLocation)) {
        return completedLocation;
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to verify completed segment upload at {}. Will not start another upload: {}",
          completedLocation, e.getMessage());
      return null;
    }
    try {
      return nextGenerationUploadTask.call();
    } catch (Exception e) {
      LOGGER.warn("Failed to start a new segment upload generation after {} was consumed: {}", completedLocation,
          e.getMessage());
      return null;
    }
  }

  @Nullable
  private KeyedUpload getCompletedUpload(UUID uploadId) {
    return isSegmentBuildUpload(uploadId) ? _completedSegmentBuildUploads.get(uploadId)
        : _completedKeyedUploads.getIfPresent(uploadId);
  }

  private void putCompletedUpload(UUID uploadId, KeyedUpload completedUpload) {
    if (isSegmentBuildUpload(uploadId)) {
      _completedSegmentBuildUploads.put(uploadId, completedUpload);
    } else {
      _completedKeyedUploads.put(uploadId, completedUpload);
    }
  }

  private void removeCompletedUpload(UUID uploadId, KeyedUpload completedUpload) {
    if (isSegmentBuildUpload(uploadId)) {
      _completedSegmentBuildUploads.remove(uploadId, completedUpload);
    } else {
      _completedKeyedUploads.asMap().remove(uploadId, completedUpload);
    }
  }

  private boolean isSegmentBuildUpload(UUID uploadId) {
    return uploadId.equals(_segmentBuildId);
  }

  private synchronized void bindToSegmentBuild(File segmentFile, LLCSegmentName segmentName) {
    if (_segmentBuildId == null) {
      return;
    }
    String segmentNameStr = segmentName.getSegmentName();
    String segmentFilePath = segmentFile.getAbsolutePath();
    if (_boundSegmentName == null) {
      _boundSegmentName = segmentNameStr;
      _boundSegmentFilePath = segmentFilePath;
      return;
    }
    Preconditions.checkArgument(_boundSegmentName.equals(segmentNameStr) && _boundSegmentFilePath.equals(
            segmentFilePath),
        "Segment build ID %s is already bound to segment %s and file %s", _segmentBuildId, _boundSegmentName,
        _boundSegmentFilePath);
  }

  void retire() {
    _executorService.shutdownNow();
  }

  @Override
  public void close() {
    _executorService.shutdownNow();
  }

  private static class KeyedUpload {
    private final String _segmentName;
    private final CompletableFuture<URI> _future = new CompletableFuture<>();

    private KeyedUpload(LLCSegmentName segmentName) {
      _segmentName = segmentName.getSegmentName();
    }

    private void validateInput(LLCSegmentName segmentName) {
      Preconditions.checkArgument(_segmentName.equals(segmentName.getSegmentName()),
          "Upload ID is already bound to segment %s", _segmentName);
    }
  }
}
