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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used to provide the specified consistency mode for upsert table by tracking the segments and
 * synchronizing the accesses to the validDocIds of those tracked segments properly. Two consistency modes are
 * supported currently:
 * - SYNC mode, the upsert threads take the WLock when the upsert involves two segments' bitmaps; and the query
 * threads take the RLock when getting bitmaps for all its selected segments.
 * - SNAPSHOT mode, the query threads don't need to take lock when getting bitmaps for all its selected segments, as
 * the query threads access a copy of bitmaps that are kept updated by upsert thread periodically. But the query
 * thread can specify a freshness threshold query option to refresh the bitmap copies if not fresh enough.
 */
public class UpsertViewManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertViewManager.class);
  private final UpsertConfig.ConsistencyMode _consistencyMode;

  // NOTE that we can't reuse _trackedSegments map in BasePartitionUpsertMetadataManager, as it doesn't track all
  // segments like those out of the metadata TTL, and it's called after adding segments to the table manager so the
  // new segments become queryable before upsert view can get updated. So we use a separate map to track the segments
  // properly. Besides, updating the set of tracked segments must be synchronized with queries getting segment
  // contexts, so the need of the R/W lock.
  private final ReadWriteLock _trackedSegmentsLock = new ReentrantReadWriteLock();
  private final Set<IndexSegment> _trackedSegments = ConcurrentHashMap.newKeySet();
  // Optional segments are part of the tracked segments. They can get processed by server before getting included in
  // broker's routing table, like the new consuming segment. Although broker misses such segments, the server needs
  // to acquire them to avoid missing the new valid docs in them.
  private final Set<String> _optionalSegments = ConcurrentHashMap.newKeySet();

  // Updating and accessing segments' validDocIds bitmaps are synchronized with a separate R/W lock for clarity.
  // The query threads always get _upsertViewTrackedSegmentsLock then _upsertViewSegmentDocIdsLock to avoid deadlock.
  // And the upsert threads never nest the two locks.
  private final ReadWriteLock _upsertViewLock = new ReentrantReadWriteLock();
  private volatile Map<IndexSegment, MutableRoaringBitmap> _segmentQueryableDocIdsMap;

  // For SNAPSHOT mode, track segments that get new updates since last refresh to reduce the overhead of refreshing.
  private final Set<IndexSegment> _updatedSegmentsSinceLastRefresh = ConcurrentHashMap.newKeySet();
  private volatile long _lastUpsertViewRefreshTimeMs = 0;
  private final long _upsertViewRefreshIntervalMs;

  public UpsertViewManager(UpsertConfig.ConsistencyMode consistencyMode, UpsertContext context) {
    _consistencyMode = consistencyMode;
    _upsertViewRefreshIntervalMs = context.getUpsertViewRefreshIntervalMs();
  }

  public void replaceDocId(IndexSegment newSegment, ThreadSafeMutableRoaringBitmap validDocIds,
      ThreadSafeMutableRoaringBitmap queryableDocIds, IndexSegment oldSegment, int oldDocId, int newDocId,
      RecordInfo recordInfo) {
    if (_consistencyMode == UpsertConfig.ConsistencyMode.SYNC) {
      _upsertViewLock.writeLock().lock();
      try {
        UpsertUtils.doRemoveDocId(oldSegment, oldDocId);
        UpsertUtils.doAddDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
        return;
      } finally {
        _upsertViewLock.writeLock().unlock();
      }
    }
    // For SNAPSHOT mode, take read lock to sync with the batch refresh.
    _upsertViewLock.readLock().lock();
    try {
      UpsertUtils.doRemoveDocId(oldSegment, oldDocId);
      UpsertUtils.doAddDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
      _updatedSegmentsSinceLastRefresh.add(newSegment);
      _updatedSegmentsSinceLastRefresh.add(oldSegment);
    } finally {
      _upsertViewLock.readLock().unlock();
      // Batch refresh takes WLock. Do it outside RLock for clarity. The R/W lock ensures that only one thread
      // can refresh the bitmaps. The other threads that are about to update the bitmaps will be blocked until
      // refreshing is done.
      doBatchRefreshUpsertView(_upsertViewRefreshIntervalMs, false);
    }
  }

  public void replaceDocId(IndexSegment segment, ThreadSafeMutableRoaringBitmap validDocIds,
      ThreadSafeMutableRoaringBitmap queryableDocIds, int oldDocId, int newDocId, RecordInfo recordInfo) {
    if (_consistencyMode == UpsertConfig.ConsistencyMode.SYNC) {
      UpsertUtils.doReplaceDocId(validDocIds, queryableDocIds, oldDocId, newDocId, recordInfo);
      return;
    }
    // For SNAPSHOT mode, take read lock to sync with the batch refresh.
    _upsertViewLock.readLock().lock();
    try {
      UpsertUtils.doReplaceDocId(validDocIds, queryableDocIds, oldDocId, newDocId, recordInfo);
      _updatedSegmentsSinceLastRefresh.add(segment);
    } finally {
      _upsertViewLock.readLock().unlock();
      // Batch refresh takes WLock. Do it outside RLock for clarity.
      doBatchRefreshUpsertView(_upsertViewRefreshIntervalMs, false);
    }
  }

  public void addDocId(IndexSegment segment, ThreadSafeMutableRoaringBitmap validDocIds,
      ThreadSafeMutableRoaringBitmap queryableDocIds, int docId, RecordInfo recordInfo) {
    if (_consistencyMode == UpsertConfig.ConsistencyMode.SYNC) {
      UpsertUtils.doAddDocId(validDocIds, queryableDocIds, docId, recordInfo);
      return;
    }
    // For SNAPSHOT mode, take read lock to sync with the batch refresh.
    _upsertViewLock.readLock().lock();
    try {
      UpsertUtils.doAddDocId(validDocIds, queryableDocIds, docId, recordInfo);
      _updatedSegmentsSinceLastRefresh.add(segment);
    } finally {
      _upsertViewLock.readLock().unlock();
      // Batch refresh takes WLock. Do it outside RLock for clarity.
      doBatchRefreshUpsertView(_upsertViewRefreshIntervalMs, false);
    }
  }

  public void removeDocId(IndexSegment segment, int docId) {
    if (_consistencyMode == UpsertConfig.ConsistencyMode.SYNC) {
      UpsertUtils.doRemoveDocId(segment, docId);
      return;
    }
    // For SNAPSHOT mode, take read lock to sync with the batch refresh.
    _upsertViewLock.readLock().lock();
    try {
      UpsertUtils.doRemoveDocId(segment, docId);
      _updatedSegmentsSinceLastRefresh.add(segment);
    } finally {
      _upsertViewLock.readLock().unlock();
      // Batch refresh takes WLock. Do it outside RLock for clarity.
      doBatchRefreshUpsertView(_upsertViewRefreshIntervalMs, false);
    }
  }

  /**
   * Use the segmentContexts to collect the contexts for selected segments. Reuse the segmentContext object if
   * present, to avoid overwriting the contexts specified at the others places.
   */
  public void setSegmentContexts(List<SegmentContext> segmentContexts, Map<String, String> queryOptions) {
    if (_consistencyMode == UpsertConfig.ConsistencyMode.SYNC) {
      _upsertViewLock.readLock().lock();
      try {
        setSegmentContexts(segmentContexts);
        return;
      } finally {
        _upsertViewLock.readLock().unlock();
      }
    }
    // If batch refresh is enabled, the copy of bitmaps is kept updated and ready to use for a consistent view.
    // The locking between query threads and upsert threads can be avoided when using batch refresh.
    // Besides, queries can share the copy of bitmaps, w/o cloning the bitmaps by every single query.
    // If query has specified a need for certain freshness, check the view and refresh it as needed.
    // When refreshing the copy of map, we need to take the WLock so only one thread is refreshing view.
    long upsertViewFreshnessMs =
        Math.min(QueryOptionsUtils.getUpsertViewFreshnessMs(queryOptions), _upsertViewRefreshIntervalMs);
    if (upsertViewFreshnessMs < 0) {
      upsertViewFreshnessMs = _upsertViewRefreshIntervalMs;
    }
    doBatchRefreshUpsertView(upsertViewFreshnessMs, needForceRefresh());
    Map<IndexSegment, MutableRoaringBitmap> currentUpsertView = _segmentQueryableDocIdsMap;
    for (SegmentContext segmentContext : segmentContexts) {
      IndexSegment segment = segmentContext.getIndexSegment();
      MutableRoaringBitmap segmentView = currentUpsertView.get(segment);
      if (segmentView != null) {
        segmentContext.setQueryableDocIdsSnapshot(segmentView);
      }
    }
  }

  private boolean needForceRefresh() {
    // Check if any segment membership changes against the current upsert view, if so, force refresh. Need this check
    // because when replacing segment, we need to include the new segment into the upsert view for the query, and we
    // need to remove the old segment from upsert view when replacement is done for the query. This check is done by
    // the query thread after getting the _upsertViewTrackedSegmentsLock, so that the tracked segments are not changed.
    Map<IndexSegment, MutableRoaringBitmap> currentView = _segmentQueryableDocIdsMap;
    if (currentView == null) {
      return true;
    }
    Set<IndexSegment> currentSegments = currentView.keySet();
    return !_trackedSegments.containsAll(currentSegments) || !currentSegments.containsAll(_trackedSegments);
  }

  private void setSegmentContexts(List<SegmentContext> segmentContexts) {
    for (SegmentContext segmentContext : segmentContexts) {
      IndexSegment segment = segmentContext.getIndexSegment();
      if (_trackedSegments.contains(segment)) {
        segmentContext.setQueryableDocIdsSnapshot(UpsertUtils.getQueryableDocIdsSnapshotFromSegment(segment, true));
      }
    }
  }

  private boolean skipUpsertViewRefresh(long upsertViewFreshnessMs) {
    if (upsertViewFreshnessMs < 0) {
      return true;
    }
    return _lastUpsertViewRefreshTimeMs + upsertViewFreshnessMs > System.currentTimeMillis();
  }

  private void doBatchRefreshUpsertView(long upsertViewFreshnessMs, boolean forceRefresh) {
    // Always refresh if the current view is still empty.
    if (!forceRefresh && skipUpsertViewRefresh(upsertViewFreshnessMs) && _segmentQueryableDocIdsMap != null) {
      return;
    }
    _upsertViewLock.writeLock().lock();
    try {
      // Check again with lock, and always refresh if the current view is still empty.
      Map<IndexSegment, MutableRoaringBitmap> current = _segmentQueryableDocIdsMap;
      if (!forceRefresh && skipUpsertViewRefresh(upsertViewFreshnessMs) && current != null) {
        return;
      }
      if (LOGGER.isDebugEnabled()) {
        if (current == null) {
          LOGGER.debug("Current upsert view is still null");
        } else {
          current.forEach((segment, bitmap) -> LOGGER.debug(
              "Current upsert view of segment: {}, type: {}, ref: {}, total: {}, valid: {}", segment.getSegmentName(),
              (segment instanceof ImmutableSegment ? "imm" : "mut"), segment.hashCode(),
              segment.getSegmentMetadata().getTotalDocs(), bitmap.getCardinality()));
        }
      }
      Map<IndexSegment, MutableRoaringBitmap> updated = new HashMap<>();
      for (IndexSegment segment : _trackedSegments) {
        // Update bitmap for segment updated since last refresh or not in the view yet. This also handles segments
        // that are tracked by _trackedSegments but not by _updatedSegmentsSinceLastRefresh, like those didn't update
        // any bitmaps as their docs simply lost all the upsert comparisons with the existing docs.
        if (current == null || current.get(segment) == null || _updatedSegmentsSinceLastRefresh.contains(segment)) {
          updated.put(segment, UpsertUtils.getQueryableDocIdsSnapshotFromSegment(segment, true));
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Update upsert view of segment: {}, type: {}, ref: {}, total: {}, valid: {}, reason: {}",
                segment.getSegmentName(), (segment instanceof ImmutableSegment ? "imm" : "mut"), segment.hashCode(),
                segment.getSegmentMetadata().getTotalDocs(), updated.get(segment).getCardinality(),
                current == null || current.get(segment) == null ? "no view yet" : "bitmap updated");
          }
        } else {
          updated.put(segment, current.get(segment));
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reuse upsert view of segment: {}, type: {}, ref: {}, total: {}, valid: {}",
                segment.getSegmentName(), (segment instanceof ImmutableSegment ? "imm" : "mut"), segment.hashCode(),
                segment.getSegmentMetadata().getTotalDocs(), updated.get(segment).getCardinality());
          }
        }
      }
      // Swap in the new consistent set of bitmaps.
      if (LOGGER.isDebugEnabled()) {
        updated.forEach((segment, bitmap) -> LOGGER.debug(
            "Updated upsert view of segment: {}, type: {}, ref: {}, total: {}, valid: {}", segment.getSegmentName(),
            (segment instanceof ImmutableSegment ? "imm" : "mut"), segment.hashCode(),
            segment.getSegmentMetadata().getTotalDocs(), bitmap.getCardinality()));
      }
      _segmentQueryableDocIdsMap = updated;
      _updatedSegmentsSinceLastRefresh.clear();
      _lastUpsertViewRefreshTimeMs = System.currentTimeMillis();
    } finally {
      _upsertViewLock.writeLock().unlock();
    }
  }

  public void lockTrackedSegments() {
    _trackedSegmentsLock.readLock().lock();
  }

  public void unlockTrackedSegments() {
    _trackedSegmentsLock.readLock().unlock();
  }

  public Set<String> getOptionalSegments() {
    return _optionalSegments;
  }

  public void trackSegment(IndexSegment segment) {
    _trackedSegmentsLock.writeLock().lock();
    try {
      _trackedSegments.add(segment);
      if (segment instanceof MutableSegment) {
        _optionalSegments.add(segment.getSegmentName());
      }
    } finally {
      _trackedSegmentsLock.writeLock().unlock();
    }
  }

  public void untrackSegment(IndexSegment segment) {
    _trackedSegmentsLock.writeLock().lock();
    try {
      _trackedSegments.remove(segment);
      if (segment instanceof MutableSegment) {
        _optionalSegments.remove(segment.getSegmentName());
      }
    } finally {
      _trackedSegmentsLock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  Map<IndexSegment, MutableRoaringBitmap> getSegmentQueryableDocIdsMap() {
    return _segmentQueryableDocIdsMap;
  }

  @VisibleForTesting
  Set<IndexSegment> getUpdatedSegmentsSinceLastRefresh() {
    return _updatedSegmentsSinceLastRefresh;
  }
}
