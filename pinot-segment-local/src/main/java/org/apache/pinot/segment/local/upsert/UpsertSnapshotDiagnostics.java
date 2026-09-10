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
package org.apache.pinot.segment.local.upsert;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.spi.IndexSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Opt-in capture accounting and bounded publication state. All work runs at existing snapshot boundaries.
/// The only per-segment retained state lives on the immutable segment; completed reports contain no segment list.
final class UpsertSnapshotDiagnostics {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertSnapshotDiagnostics.class);
  private final String _runtimeEpoch = UUID.randomUUID().toString();
  private final AtomicLong _nextCaptureId = new AtomicLong();
  private final AtomicInteger _activeSnapshots = new AtomicInteger();
  private final AtomicReference<UpsertSnapshotMetadata.Counters> _counters =
      new AtomicReference<>(new UpsertSnapshotMetadata.Counters(0, 0, 0));
  private final AtomicLong _publicationFailures = new AtomicLong();
  private final AtomicReference<Publication> _latest = new AtomicReference<>();
  private final File _tableIndexDir;
  private final int _partitionId;
  private final boolean _hasQueryableBitmap;
  @Nullable
  private final UpsertKeyDigest _keyDigest;

  UpsertSnapshotDiagnostics(int partitionId, UpsertContext context, @Nullable UpsertKeyDigest keyDigest) {
    _partitionId = partitionId;
    _tableIndexDir = context.getTableIndexDir();
    _hasQueryableBitmap = context.getDeleteRecordColumn() != null;
    _keyDigest = keyDigest;
  }

  Capture begin(@Nullable String consumingSegmentName, @Nullable String startOffset,
      UpsertSnapshotMetadata.CleanupProgress cleanup) {
    // Register activity before assigning the sequence, so a delayed starter cannot escape overlap detection.
    boolean concurrent = _activeSnapshots.incrementAndGet() > 1;
    UpsertKeyDigest.Mark digestStart = _keyDigest != null ? _keyDigest.freeze() : null;
    return new Capture(_nextCaptureId.incrementAndGet(), consumingSegmentName, startOffset, cleanup, concurrent,
        digestStart);
  }

  void finish(Capture capture, boolean aborted, UpsertSnapshotMetadata.CleanupProgress cleanupAfter) {
    UpsertSnapshotMetadata.Attempt attempt = new UpsertSnapshotMetadata.Attempt(capture._selected,
        capture._written, capture._lockSkipped, capture._otherSkipped, capture._failed, aborted);
    UpsertSnapshotMetadata.Content content = collectSavedContent(capture);
    UpsertSnapshotMetadata.KeyDigest keyDigest = capture._digestStart != null && _keyDigest != null
        ? UpsertSnapshotMetadata.KeyDigest.from(capture._digestStart, _keyDigest.freeze()) : null;
    int remaining = _activeSnapshots.decrementAndGet();
    boolean concurrent = capture._concurrentAtStart || remaining > 0 || _nextCaptureId.get() != capture._id;
    UpsertSnapshotMetadata.Counters counters = _counters.updateAndGet(previous -> new UpsertSnapshotMetadata.Counters(
        previous.attemptsTotal() + 1, previous.attemptsWithLockSkipsTotal() + (capture._lockSkipped > 0 ? 1 : 0),
        previous.failedAttemptsTotal() + (aborted || capture._failed > 0 ? 1 : 0)));
    UpsertSnapshotMetadata metadata = new UpsertSnapshotMetadata(UpsertSnapshotMetadata.FORMAT_VERSION, _partitionId,
        capture._consumingSegmentName, capture._startOffset, capture._startedAtMillis, System.currentTimeMillis(),
        _runtimeEpoch, capture._id, attempt, counters, content, keyDigest, capture._cleanup, cleanupAfter,
        concurrent);
    publish(metadata);
  }

  private UpsertSnapshotMetadata.Content collectSavedContent(Capture capture) {
    try {
      List<UpsertSnapshotFingerprint.File> files = new ArrayList<>();
      for (ImmutableSegmentImpl segment : capture._segments) {
        UpsertSnapshotFingerprint.State state = segment.getSnapshotFingerprints();
        String dataCrc = segment.getSegmentMetadata().getDataCrc();
        files.add(new UpsertSnapshotFingerprint.File(segment.getSegmentName(), dataCrc, "VALID",
            state != null ? state.fingerprint(false) : null));
        if (_hasQueryableBitmap) {
          files.add(new UpsertSnapshotFingerprint.File(segment.getSegmentName(), dataCrc, "QUERYABLE",
              state != null ? state.fingerprint(true) : null));
        }
      }
      return UpsertSnapshotFingerprint.aggregate(files);
    } catch (RuntimeException e) {
      LOGGER.warn("Could not collect snapshot fingerprints for partition: {}", _partitionId, e);
      return UpsertSnapshotMetadata.Content.unavailable(capture._segments.size() * (_hasQueryableBitmap ? 2 : 1));
    }
  }

  private void publish(UpsertSnapshotMetadata metadata) {
    Publication pending = new Publication(metadata, "PENDING");
    _latest.accumulateAndGet(pending, (previous, next) -> {
      if (previous == null
          || previous.metadata().counters().attemptsTotal() < next.metadata().counters().attemptsTotal()) {
        return next;
      }
      return previous;
    });
    boolean persisted = UpsertSnapshotMetadataStore.persist(_tableIndexDir, metadata);
    if (!persisted) {
      _publicationFailures.incrementAndGet();
    }
    // Do not replace a newer concurrent completion with an older attempt's persistence result.
    _latest.compareAndSet(pending, new Publication(metadata, persisted ? "PERSISTED" : "FAILED"));
  }

  @Nullable
  UpsertSnapshotMetadata.Status status(UpsertSnapshotMetadata.CleanupProgress cleanupNow) {
    Publication publication = _latest.get();
    return publication != null ? new UpsertSnapshotMetadata.Status(publication.metadata(), publication.status(),
        _publicationFailures.get(), cleanupNow) : null;
  }

  private record Publication(UpsertSnapshotMetadata metadata, String status) {
  }

  /// Confined to the thread executing one existing snapshot attempt; discarded after publication.
  static final class Capture {
    private final long _id;
    private final String _consumingSegmentName;
    private final String _startOffset;
    private final long _startedAtMillis = System.currentTimeMillis();
    private final UpsertSnapshotMetadata.CleanupProgress _cleanup;
    private final boolean _concurrentAtStart;
    @Nullable
    private final UpsertKeyDigest.Mark _digestStart;
    private final List<ImmutableSegmentImpl> _segments = new ArrayList<>();
    int _selected;
    int _written;
    int _lockSkipped;
    int _otherSkipped;
    int _failed;

    private Capture(long id, String consumingSegmentName, String startOffset,
        UpsertSnapshotMetadata.CleanupProgress cleanup, boolean concurrentAtStart,
        @Nullable UpsertKeyDigest.Mark digestStart) {
      _id = id;
      _consumingSegmentName = consumingSegmentName;
      _startOffset = startOffset;
      _cleanup = cleanup;
      _concurrentAtStart = concurrentAtStart;
      _digestStart = digestStart;
    }

    void track(IndexSegment segment) {
      if (segment instanceof ImmutableSegmentImpl immutableSegment) {
        immutableSegment.enableSnapshotFingerprints();
        _segments.add(immutableSegment);
      }
    }
  }
}
