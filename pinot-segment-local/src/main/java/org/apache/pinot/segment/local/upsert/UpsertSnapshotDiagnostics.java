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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Opt-in capture accounting and bounded publication state. All work runs at existing snapshot/lifecycle boundaries.
/// The only per-segment retained state lives on the immutable segment; completed reports contain no segment list.
final class UpsertSnapshotDiagnostics {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertSnapshotDiagnostics.class);
  private final String _runtimeEpoch = UUID.randomUUID().toString();
  private final AtomicLong _nextCaptureId = new AtomicLong();
  private final UpsertSnapshotActivity _captures = new UpsertSnapshotActivity();
  private final AtomicReference<UpsertSnapshotMetadata.Counters> _counters =
      new AtomicReference<>(new UpsertSnapshotMetadata.Counters(0, 0, 0));
  private final AtomicLong _publicationFailures = new AtomicLong();
  private final AtomicReference<Publication> _latest = new AtomicReference<>();
  private final File _tableIndexDir;
  private final int _partitionId;
  private final boolean _hasQueryableBitmap;
  @Nullable
  private final String _configurationFingerprint;

  UpsertSnapshotDiagnostics(int partitionId, UpsertContext context) {
    _partitionId = partitionId;
    _tableIndexDir = context.getTableIndexDir();
    _hasQueryableBitmap = context.getDeleteRecordColumn() != null;
    _configurationFingerprint = configurationFingerprint(context);
  }

  Capture begin(@Nullable String consumingSegmentName, @Nullable String startOffset,
      UpsertSnapshotMetadata.CleanupProgress cleanup, UpsertSnapshotMetadata.Activity lifecycle) {
    return new Capture(_nextCaptureId.incrementAndGet(), consumingSegmentName, startOffset, cleanup, lifecycle,
        _captures.begin());
  }

  void finish(Capture capture, boolean aborted, UpsertSnapshotMetadata.CleanupProgress cleanupAfter,
      UpsertSnapshotMetadata.Activity lifecycleAfter) {
    UpsertSnapshotMetadata.Attempt attempt = new UpsertSnapshotMetadata.Attempt(capture._selected,
        capture._written, capture._lockSkipped, capture._otherSkipped, capture._failed, aborted);
    List<String> issues = new ArrayList<>();
    UpsertSnapshotMetadata.Content content;
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
      content = UpsertSnapshotFingerprint.aggregate(files);
    } catch (RuntimeException e) {
      LOGGER.warn("Could not collect snapshot fingerprints for partition: {}", _partitionId, e);
      content = UpsertSnapshotMetadata.Content.unavailable(capture._segments.size() * (_hasQueryableBitmap ? 2 : 1));
      issues.add("FINGERPRINT_COLLECTION_FAILED");
    }
    UpsertSnapshotMetadata.Activity completed = _captures.end(!aborted);
    if (capture._captureActivity.activeOperations() != 1 || completed.activeOperations() != 0
        || completed.version() != capture._captureActivity.version() + 1) {
      issues.add("CONCURRENT_SNAPSHOT");
    }
    if (attempt.aborted() || attempt.failedSegments() > 0 || attempt.lockSkippedSegments() > 0
        || attempt.otherSkippedSegments() > 0) {
      issues.add("INCOMPLETE_SNAPSHOT_ATTEMPT");
    }
    if (content.populationFingerprint() == null || content.savedFilesFingerprint() == null) {
      issues.add("INCOMPLETE_FINGERPRINT_COVERAGE");
    }
    if (capture._consumingSegments > 0) {
      issues.add("CONSUMING_SEGMENTS_PRESENT");
    }
    if (capture._lifecycle.activeOperations() > 0 || lifecycleAfter.activeOperations() > 0
        || capture._lifecycle.version() != lifecycleAfter.version()) {
      issues.add("LIFECYCLE_OVERLAP");
    }
    if (lifecycleAfter.failedOperations() > 0) {
      issues.add("LIFECYCLE_FAILURE_OBSERVED");
    }
    if (cleanupOverlaps(capture._cleanup, cleanupAfter)) {
      issues.add("CLEANUP_OVERLAP_OR_UNKNOWN");
    }
    if (cleanupAfter.failedPasses() > 0) {
      issues.add("CLEANUP_FAILURE_OBSERVED");
    }
    if (_configurationFingerprint == null) {
      issues.add("CONFIGURATION_UNKNOWN");
    }
    if (capture._consumingSegmentName == null || capture._startOffset == null) {
      issues.add("NO_STARTUP_CONTEXT");
    }
    // Manager-local observations do not attest stream identity/reconciliation or pre-manager file replacement.
    // A future consumer must not turn the absence of an observed overlap into a verified logical cut.
    issues.add("SOURCE_BOUNDARY_UNVERIFIED");
    UpsertSnapshotMetadata.Counters counters = _counters.updateAndGet(previous -> new UpsertSnapshotMetadata.Counters(
        previous.attemptsTotal() + 1, previous.attemptsWithLockSkipsTotal() + (capture._lockSkipped > 0 ? 1 : 0),
        previous.failedAttemptsTotal() + (aborted || capture._failed > 0 ? 1 : 0)));
    UpsertSnapshotMetadata metadata = new UpsertSnapshotMetadata(UpsertSnapshotMetadata.FORMAT_VERSION, _partitionId,
        capture._consumingSegmentName, capture._startOffset, capture._startedAtMillis, System.currentTimeMillis(),
        _runtimeEpoch, capture._id, attempt, counters, content, capture._cleanup, cleanupAfter, capture._lifecycle,
        lifecycleAfter, _configurationFingerprint, issues);
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

  static boolean cleanupOverlaps(UpsertSnapshotMetadata.CleanupProgress before,
      UpsertSnapshotMetadata.CleanupProgress after) {
    return before.version() != after.version() || !isIdle(before.phase()) || !isIdle(after.phase());
  }

  private static boolean isIdle(String phase) {
    return "COMPLETED".equals(phase) || "DISABLED".equals(phase);
  }

  @Nullable
  private static String configurationFingerprint(UpsertContext context) {
    if (context.getTableConfig() == null || context.getSchema() == null) {
      return null;
    }
    try {
      ObjectNode node = JsonUtils.newObjectNode();
      node.set("table", JsonUtils.objectToJsonNode(context.getTableConfig()));
      node.set("schema", JsonUtils.objectToJsonNode(context.getSchema()));
      node.set("metadataManagerConfigs", JsonUtils.objectToJsonNode(context.getMetadataManagerConfigs()));
      Map<String, Object> effective = new TreeMap<>();
      effective.put("primaryKeyColumns", context.getPrimaryKeyColumns());
      effective.put("comparisonColumns", context.getComparisonColumns());
      effective.put("hashFunction", context.getHashFunction());
      effective.put("deleteRecordColumn", context.getDeleteRecordColumn());
      effective.put("metadataTTL", context.getMetadataTTL());
      effective.put("deletedKeysTTL", context.getDeletedKeysTTL());
      effective.put("consistencyMode", context.getConsistencyMode());
      node.set("effectiveUpsert", JsonUtils.objectToJsonNode(effective));
      return UpsertSnapshotFingerprint.hash(JsonUtils.objectToBytes(canonical(node)));
    } catch (Exception e) {
      LOGGER.warn("Could not fingerprint upsert snapshot configuration", e);
      return null;
    }
  }

  private static JsonNode canonical(JsonNode node) {
    if (node.isObject()) {
      Map<String, JsonNode> fields = new TreeMap<>();
      Iterator<String> names = node.fieldNames();
      while (names.hasNext()) {
        String name = names.next();
        fields.put(name, canonical(node.get(name)));
      }
      ObjectNode sorted = JsonUtils.newObjectNode();
      fields.forEach(sorted::set);
      return sorted;
    }
    if (node.isArray()) {
      ArrayNode array = JsonUtils.newArrayNode();
      node.forEach(element -> array.add(canonical(element)));
      return array;
    }
    return node;
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
    private final UpsertSnapshotMetadata.Activity _lifecycle;
    private final UpsertSnapshotMetadata.Activity _captureActivity;
    private final List<ImmutableSegmentImpl> _segments = new ArrayList<>();
    int _selected;
    int _written;
    int _lockSkipped;
    int _otherSkipped;
    int _failed;
    int _consumingSegments;

    private Capture(long id, String consumingSegmentName, String startOffset,
        UpsertSnapshotMetadata.CleanupProgress cleanup, UpsertSnapshotMetadata.Activity lifecycle,
        UpsertSnapshotMetadata.Activity captureActivity) {
      _id = id;
      _consumingSegmentName = consumingSegmentName;
      _startOffset = startOffset;
      _cleanup = cleanup;
      _lifecycle = lifecycle;
      _captureActivity = captureActivity;
    }

    void track(IndexSegment segment) {
      if (segment instanceof ImmutableSegmentImpl immutableSegment) {
        immutableSegment.enableSnapshotFingerprints();
        _segments.add(immutableSegment);
      } else {
        _consumingSegments++;
      }
    }
  }
}
