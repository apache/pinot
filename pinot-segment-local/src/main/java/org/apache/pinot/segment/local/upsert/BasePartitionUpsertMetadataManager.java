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
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BasePartitionUpsertMetadataManager implements PartitionUpsertMetadataManager {
  protected static final long OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS = TimeUnit.MINUTES.toNanos(1);

  protected final String _tableNameWithType;
  protected final int _partitionId;
  protected final List<String> _primaryKeyColumns;
  protected final List<String> _comparisonColumns;
  protected final String _deleteRecordColumn;
  protected final HashFunction _hashFunction;
  protected final PartialUpsertHandler _partialUpsertHandler;
  protected final boolean _enableSnapshot;
  protected final ServerMetrics _serverMetrics;
  protected final Logger _logger;

  // Tracks all the segments managed by this manager (excluding EmptySegment)
  protected final Set<IndexSegment> _trackedSegments = ConcurrentHashMap.newKeySet();

  // NOTE: We do not persist snapshot on the first consuming segment because most segments might not be loaded yet
  protected volatile boolean _gotFirstConsumingSegment = false;
  protected final ReadWriteLock _snapshotLock;

  protected volatile boolean _stopped = false;
  // Initialize with 1 pending operation to indicate the metadata manager can take more operations
  protected final AtomicInteger _numPendingOperations = new AtomicInteger(1);

  protected long _lastOutOfOrderEventReportTimeNs = Long.MIN_VALUE;
  protected int _numOutOfOrderEvents = 0;

  protected BasePartitionUpsertMetadataManager(String tableNameWithType, int partitionId,
      List<String> primaryKeyColumns, List<String> comparisonColumns, @Nullable String deleteRecordColumn,
      HashFunction hashFunction, @Nullable PartialUpsertHandler partialUpsertHandler, boolean enableSnapshot,
      ServerMetrics serverMetrics) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _primaryKeyColumns = primaryKeyColumns;
    _comparisonColumns = comparisonColumns;
    _deleteRecordColumn = deleteRecordColumn;
    _hashFunction = hashFunction;
    _partialUpsertHandler = partialUpsertHandler;
    _enableSnapshot = enableSnapshot;
    _snapshotLock = enableSnapshot ? new ReentrantReadWriteLock() : null;
    _serverMetrics = serverMetrics;
    _logger = LoggerFactory.getLogger(tableNameWithType + "-" + partitionId + "-" + getClass().getSimpleName());
  }

  @Override
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  @Override
  public void addSegment(ImmutableSegment segment) {
    String segmentName = segment.getSegmentName();
    if (_stopped) {
      _logger.info("Skip adding segment: {} because metadata manager is already stopped", segment.getSegmentName());
      return;
    }
    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      return;
    }
    Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
        "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
        _tableNameWithType);

    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    startOperation();
    try {
      doAddSegment((ImmutableSegmentImpl) segment);
      _trackedSegments.add(segment);
    } finally {
      finishOperation();
      if (_enableSnapshot) {
        _snapshotLock.readLock().unlock();
      }
    }
  }

  protected void doAddSegment(ImmutableSegmentImpl segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Adding segment: {}, current primary key count: {}", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    MutableRoaringBitmap validDocIds;
    if (_enableSnapshot) {
      validDocIds = segment.loadValidDocIdsFromSnapshot();
      if (validDocIds != null && validDocIds.isEmpty()) {
        _logger.info("Skip adding segment: {} without valid doc, current primary key count: {}",
            segment.getSegmentName(), getNumPrimaryKeys());
        segment.enableUpsert(this, new ThreadSafeMutableRoaringBitmap(), null);
        return;
      }
    } else {
      validDocIds = null;
      segment.deleteValidDocIdsSnapshot();
    }

    try (UpsertUtils.RecordInfoReader recordInfoReader = new UpsertUtils.RecordInfoReader(segment, _primaryKeyColumns,
        _comparisonColumns, _deleteRecordColumn)) {
      Iterator<RecordInfo> recordInfoIterator;
      if (validDocIds != null) {
        recordInfoIterator = UpsertUtils.getRecordInfoIterator(recordInfoReader, validDocIds);
      } else {
        recordInfoIterator =
            UpsertUtils.getRecordInfoIterator(recordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      }
      addSegment(segment, null, null, recordInfoIterator);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while adding segment: %s, table: %s", segmentName, _tableNameWithType), e);
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finished adding segment: {} in {}ms, current primary key count: {}", segmentName,
        System.currentTimeMillis() - startTimeMs, numPrimaryKeys);
  }

  /**
   * NOTE: We allow passing in validDocIds and queryableDocIds here so that the value can be easily accessed from the
   *       tests. The passed in bitmaps should always be empty.
   */
  @VisibleForTesting
  public void addSegment(ImmutableSegmentImpl segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      if (validDocIds == null) {
        validDocIds = new ThreadSafeMutableRoaringBitmap();
      }
      if (queryableDocIds == null && _deleteRecordColumn != null) {
        queryableDocIds = new ThreadSafeMutableRoaringBitmap();
      }
      addOrReplaceSegment(segment, validDocIds, queryableDocIds, recordInfoIterator, null, null);
    } finally {
      segmentLock.unlock();
    }
  }

  protected abstract long getNumPrimaryKeys();

  protected abstract void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
      @Nullable IndexSegment oldSegment, @Nullable MutableRoaringBitmap validDocIdsForOldSegment);

  @Override
  public void addRecord(MutableSegment segment, RecordInfo recordInfo) {
    if (_stopped) {
      _logger.debug("Skip adding record to segment: {} because metadata manager is already stopped",
          segment.getSegmentName());
      return;
    }

    // NOTE: We don't acquire snapshot read lock here because snapshot is always taken before a new consuming segment
    //       starts consuming, so it won't overlap with this method
    _gotFirstConsumingSegment = true;
    startOperation();
    try {
      doAddRecord(segment, recordInfo);
      _trackedSegments.add(segment);
    } finally {
      finishOperation();
    }
  }

  protected abstract void doAddRecord(MutableSegment segment, RecordInfo recordInfo);

  @Override
  public void replaceSegment(ImmutableSegment segment, IndexSegment oldSegment) {
    if (_stopped) {
      _logger.info("Skip replacing segment: {} because metadata manager is already stopped", segment.getSegmentName());
      return;
    }

    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    startOperation();
    try {
      doReplaceSegment(segment, oldSegment);
      if (!(segment instanceof EmptyIndexSegment)) {
        _trackedSegments.add(segment);
      }
      _trackedSegments.remove(oldSegment);
    } finally {
      finishOperation();
      if (_enableSnapshot) {
        _snapshotLock.readLock().lock();
      }
    }
  }

  protected void doReplaceSegment(ImmutableSegment segment, IndexSegment oldSegment) {
    String segmentName = segment.getSegmentName();
    Preconditions.checkArgument(segmentName.equals(oldSegment.getSegmentName()),
        "Cannot replace segment with different name for table: {}, old segment: {}, new segment: {}",
        _tableNameWithType, oldSegment.getSegmentName(), segmentName);
    _logger.info("Replacing {} segment: {}, current primary key count: {}",
        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      replaceSegment(segment, null, null, null, oldSegment);
      return;
    }

    try (UpsertUtils.RecordInfoReader recordInfoReader = new UpsertUtils.RecordInfoReader(segment, _primaryKeyColumns,
        _comparisonColumns, _deleteRecordColumn)) {
      Iterator<RecordInfo> recordInfoIterator =
          UpsertUtils.getRecordInfoIterator(recordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      replaceSegment(segment, null, null, recordInfoIterator, oldSegment);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while replacing segment: %s, table: %s", segmentName, _tableNameWithType), e);
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finished replacing segment: {} in {}ms, current primary key count: {}", segmentName,
        System.currentTimeMillis() - startTimeMs, numPrimaryKeys);
  }

  /**
   * NOTE: We allow passing in validDocIds and queryableDocIds here so that the value can be easily accessed from the
   *       tests. The passed in bitmaps should always be empty.
   */
  @VisibleForTesting
  public void replaceSegment(ImmutableSegment segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, @Nullable Iterator<RecordInfo> recordInfoIterator,
      IndexSegment oldSegment) {
    String segmentName = segment.getSegmentName();
    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      MutableRoaringBitmap validDocIdsForOldSegment =
          oldSegment.getValidDocIds() != null ? oldSegment.getValidDocIds().getMutableRoaringBitmap() : null;
      if (recordInfoIterator != null) {
        Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
            "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
            _tableNameWithType);
        if (validDocIds == null) {
          validDocIds = new ThreadSafeMutableRoaringBitmap();
        }
        if (queryableDocIds == null && _deleteRecordColumn != null) {
          queryableDocIds = new ThreadSafeMutableRoaringBitmap();
        }
        addOrReplaceSegment((ImmutableSegmentImpl) segment, validDocIds, queryableDocIds, recordInfoIterator,
            oldSegment, validDocIdsForOldSegment);
      }

      if (validDocIdsForOldSegment != null && !validDocIdsForOldSegment.isEmpty()) {
        int numKeysNotReplaced = validDocIdsForOldSegment.getCardinality();
        if (_partialUpsertHandler != null) {
          // For partial-upsert table, because we do not restore the original record location when removing the primary
          // keys not replaced, it can potentially cause inconsistency between replicas. This can happen when a
          // consuming segment is replaced by a committed segment that is consumed from a different server with
          // different records (some stream consumer cannot guarantee consuming the messages in the same order).
          _logger.warn("Found {} primary keys not replaced when replacing segment: {} for partial-upsert table. This "
              + "can potentially cause inconsistency between replicas", numKeysNotReplaced, segmentName);
          _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.PARTIAL_UPSERT_KEYS_NOT_REPLACED,
              numKeysNotReplaced);
        } else {
          _logger.info("Found {} primary keys not replaced when replacing segment: {}", numKeysNotReplaced,
              segmentName);
        }
        removeSegment(oldSegment, validDocIdsForOldSegment);
      }
    } finally {
      segmentLock.unlock();
    }
  }

  protected abstract void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds);

  @Override
  public void removeSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    if (_stopped) {
      _logger.info("Skip removing segment: {} because metadata manager is already stopped", segmentName);
      return;
    }
    if (!_trackedSegments.contains(segment)) {
      _logger.info("Skip removing untracked (replaced or empty) segment: {}", segmentName);
      return;
    }

    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    startOperation();
    try {
      doRemoveSegment(segment);
      _trackedSegments.remove(segment);
    } finally {
      finishOperation();
      if (_enableSnapshot) {
        _snapshotLock.readLock().unlock();
      }
    }
  }

  protected void doRemoveSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Removing {} segment: {}, current primary key count: {}",
        segment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    MutableRoaringBitmap validDocIds =
        segment.getValidDocIds() != null ? segment.getValidDocIds().getMutableRoaringBitmap() : null;
    if (validDocIds == null || validDocIds.isEmpty()) {
      _logger.info("Skip removing segment without valid docs: {}", segmentName);
      return;
    }

    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      _logger.info("Removing {} primary keys for segment: {}", validDocIds.getCardinality(), segmentName);
      removeSegment(segment, validDocIds);
    } finally {
      segmentLock.unlock();
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finished removing segment: {} in {}ms, current primary key count: {}", segmentName,
        System.currentTimeMillis() - startTimeMs, numPrimaryKeys);
  }

  @Override
  public GenericRow updateRecord(GenericRow record, RecordInfo recordInfo) {
    // Directly return the record when partial-upsert is not enabled
    if (_partialUpsertHandler == null) {
      return record;
    }
    if (_stopped) {
      _logger.debug("Skip updating record because metadata manager is already stopped");
      return record;
    }

    startOperation();
    try {
      return doUpdateRecord(record, recordInfo);
    } finally {
      finishOperation();
    }
  }

  protected abstract GenericRow doUpdateRecord(GenericRow record, RecordInfo recordInfo);

  protected void handleOutOfOrderEvent(Object currentComparisonValue, Object recordComparisonValue) {
    boolean isPartialUpsertTable = (_partialUpsertHandler != null);
    _serverMetrics.addMeteredTableValue(_tableNameWithType,
        isPartialUpsertTable ? ServerMeter.PARTIAL_UPSERT_OUT_OF_ORDER : ServerMeter.UPSERT_OUT_OF_ORDER, 1L);
    _numOutOfOrderEvents++;
    long currentTimeNs = System.nanoTime();
    if (currentTimeNs - _lastOutOfOrderEventReportTimeNs > OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS) {
      _logger.warn("Skipped {} out-of-order events for {} upsert table {} (the last event has current comparison "
              + "value: {}, record comparison value: {})", _numOutOfOrderEvents,
          (isPartialUpsertTable ? "partial" : ""), _tableNameWithType, currentComparisonValue, recordComparisonValue);
      _lastOutOfOrderEventReportTimeNs = currentTimeNs;
      _numOutOfOrderEvents = 0;
    }
  }

  @Override
  public void takeSnapshot() {
    if (!_enableSnapshot) {
      return;
    }
    if (_stopped) {
      _logger.info("Skip taking snapshot because metadata manager is already stopped");
      return;
    }
    if (!_gotFirstConsumingSegment) {
      _logger.info("Skip taking snapshot before getting the first consuming segment");
      return;
    }

    _snapshotLock.writeLock().lock();
    startOperation();
    try {
      doTakeSnapshot();
    } finally {
      finishOperation();
      _snapshotLock.writeLock().unlock();
    }
  }

  // TODO: Consider optimizing it by tracking and persisting only the changed snapshot
  protected void doTakeSnapshot() {
    int numTrackedSegments = _trackedSegments.size();
    _logger.info("Taking snapshot for {} segments", numTrackedSegments);
    long startTimeMs = System.currentTimeMillis();

    int numImmutableSegments = 0;
    for (IndexSegment segment : _trackedSegments) {
      if (segment instanceof ImmutableSegmentImpl) {
        ((ImmutableSegmentImpl) segment).persistValidDocIdsSnapshot();
        numImmutableSegments++;
      }
    }

    _logger.info("Finished taking snapshot for {} immutable segments (out of {} total segments) in {}ms",
        numImmutableSegments, numTrackedSegments, System.currentTimeMillis() - startTimeMs);
  }

  protected void startOperation() {
    _numPendingOperations.getAndIncrement();
  }

  protected void finishOperation() {
    if (_numPendingOperations.decrementAndGet() == 0) {
      synchronized (_numPendingOperations) {
        _numPendingOperations.notifyAll();
      }
    }
  }

  @Override
  public void stop() {
    _stopped = true;
    int numPendingOperations = _numPendingOperations.decrementAndGet();
    _logger.info("Stopped the metadata manager with {} pending operations, current primary key count: {}",
        numPendingOperations, getNumPrimaryKeys());
  }

  @Override
  public void close()
      throws IOException {
    Preconditions.checkState(_stopped, "Must stop the metadata manager before closing it");
    _logger.info("Closing the metadata manager");
    synchronized (_numPendingOperations) {
      int numPendingOperations;
      while ((numPendingOperations = _numPendingOperations.get()) != 0) {
        _logger.info("Waiting for {} pending operations to finish", numPendingOperations);
        try {
          _numPendingOperations.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(
              String.format("Interrupted while waiting for %d pending operations to finish", numPendingOperations), e);
        }
      }
    }
    doClose();
    _logger.info("Closed the metadata manager");
  }

  protected void doClose()
      throws IOException {
  }
}
