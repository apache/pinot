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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.roaringbitmap.PeekableIntIterator;
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
  protected final double _metadataTTL;
  protected final File _tableIndexDir;
  protected final ServerMetrics _serverMetrics;
  protected final Logger _logger;

  // Tracks all the segments managed by this manager (excluding EmptySegment)
  protected final Set<IndexSegment> _trackedSegments = ConcurrentHashMap.newKeySet();

  // NOTE: We do not persist snapshot on the first consuming segment because most segments might not be loaded yet
  protected volatile boolean _gotFirstConsumingSegment = false;
  protected final ReadWriteLock _snapshotLock;

  protected long _lastOutOfOrderEventReportTimeNs = Long.MIN_VALUE;
  protected int _numOutOfOrderEvents = 0;

  // Used to maintain the largestSeenComparisonValue to avoid handling out-of-ttl segments/records.
  // If upsertTTL enabled, we will keep track of largestSeenComparisonValue to compute expired segments.
  protected volatile double _largestSeenComparisonValue;

  // The following variables are always accessed within synchronized block
  private boolean _stopped;
  // Initialize with 1 pending operation to indicate the metadata manager can take more operations
  private int _numPendingOperations = 1;
  private boolean _closed;

  protected BasePartitionUpsertMetadataManager(String tableNameWithType, int partitionId,
      List<String> primaryKeyColumns, List<String> comparisonColumns, @Nullable String deleteRecordColumn,
      HashFunction hashFunction, @Nullable PartialUpsertHandler partialUpsertHandler, boolean enableSnapshot,
      double metadataTTL, File tableIndexDir, ServerMetrics serverMetrics) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _primaryKeyColumns = primaryKeyColumns;
    _comparisonColumns = comparisonColumns;
    _deleteRecordColumn = deleteRecordColumn;
    _hashFunction = hashFunction;
    _partialUpsertHandler = partialUpsertHandler;
    _enableSnapshot = enableSnapshot;
    _metadataTTL = metadataTTL;
    _tableIndexDir = tableIndexDir;
    _snapshotLock = enableSnapshot ? new ReentrantReadWriteLock() : null;
    _serverMetrics = serverMetrics;
    _logger = LoggerFactory.getLogger(tableNameWithType + "-" + partitionId + "-" + getClass().getSimpleName());
    if (metadataTTL > 0) {
      _largestSeenComparisonValue = loadWatermark();
    } else {
      deleteWatermark();
    }
  }

  @Override
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  @Nullable
  protected MutableRoaringBitmap getQueryableDocIds(IndexSegment segment, MutableRoaringBitmap validDocIds) {
    if (_deleteRecordColumn == null) {
      return null;
    }
    MutableRoaringBitmap queryableDocIds = new MutableRoaringBitmap();
    try (PinotSegmentColumnReader deleteRecordColumnReader = new PinotSegmentColumnReader(segment,
        _deleteRecordColumn)) {
      PeekableIntIterator docIdIterator = validDocIds.getIntIterator();
      while (docIdIterator.hasNext()) {
        int docId = docIdIterator.next();
        if (!BooleanUtils.toBoolean(deleteRecordColumnReader.getValue(docId))) {
          queryableDocIds.add(docId);
        }
      }
    } catch (IOException e) {
      _logger.error("Failed to close column reader for delete record column: {} for segment: {} ", _deleteRecordColumn,
          segment.getSegmentName(), e);
    }
    return queryableDocIds;
  }

  @Override
  public void addSegment(ImmutableSegment segment) {
    String segmentName = segment.getSegmentName();
    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      return;
    }
    Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
        "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
        _tableNameWithType);
    ImmutableSegmentImpl immutableSegment = (ImmutableSegmentImpl) segment;

    // Skip adding segment that has max comparison value smaller than (largestSeenComparisonValue - TTL)
    if (_largestSeenComparisonValue > 0) {
      Preconditions.checkState(_enableSnapshot, "Upsert TTL must have snapshot enabled");
      Preconditions.checkState(_comparisonColumns.size() == 1,
          "Upsert TTL does not work with multiple comparison columns");
      Number maxComparisonValue =
          (Number) segment.getSegmentMetadata().getColumnMetadataMap().get(_comparisonColumns.get(0)).getMaxValue();
      if (maxComparisonValue.doubleValue() < _largestSeenComparisonValue - _metadataTTL) {
        _logger.info("Skip adding segment: {} because it's out of TTL", segmentName);
        MutableRoaringBitmap validDocIdsSnapshot = immutableSegment.loadValidDocIdsFromSnapshot();
        if (validDocIdsSnapshot != null) {
          MutableRoaringBitmap queryableDocIds = getQueryableDocIds(segment, validDocIdsSnapshot);
          immutableSegment.enableUpsert(this, new ThreadSafeMutableRoaringBitmap(validDocIdsSnapshot),
              new ThreadSafeMutableRoaringBitmap(queryableDocIds));
        } else {
          _logger.warn("Failed to find snapshot from segment: {} which is out of TTL, treating all documents as valid",
              segmentName);
        }
        return;
      }
    }

    if (!startOperation()) {
      _logger.info("Skip adding segment: {} because metadata manager is already stopped", segment.getSegmentName());
      return;
    }
    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    try {
      doAddSegment(immutableSegment);
      _trackedSegments.add(segment);
    } finally {
      if (_enableSnapshot) {
        _snapshotLock.readLock().unlock();
      }
      finishOperation();
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

  @Override
  public void preloadSegment(ImmutableSegment segment) {
    String segmentName = segment.getSegmentName();
    Preconditions.checkArgument(_enableSnapshot, "Snapshot must be enabled to preload segment: {}, table: {}",
        segmentName, _tableNameWithType);
    // Note that EmptyIndexSegment should not reach here either, as it doesn't have validDocIds snapshot.
    Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
        "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
        _tableNameWithType);
    if (!startOperation()) {
      _logger.info("Skip preloading segment: {} because metadata manager is already stopped", segmentName);
      return;
    }
    _snapshotLock.readLock().lock();
    try {
      doPreloadSegment((ImmutableSegmentImpl) segment);
      _trackedSegments.add(segment);
    } finally {
      _snapshotLock.readLock().unlock();
      finishOperation();
    }
  }

  private void doPreloadSegment(ImmutableSegmentImpl segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Preloading segment: {}, current primary key count: {}", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    MutableRoaringBitmap validDocIds = segment.loadValidDocIdsFromSnapshot();
    Preconditions.checkState(validDocIds != null,
        "Snapshot of validDocIds is required to preload segment: {}, table: {}", segmentName, _tableNameWithType);
    if (validDocIds.isEmpty()) {
      _logger.info("Skip preloading segment: {} without valid doc, current primary key count: {}",
          segment.getSegmentName(), getNumPrimaryKeys());
      segment.enableUpsert(this, new ThreadSafeMutableRoaringBitmap(), null);
      return;
    }

    try (UpsertUtils.RecordInfoReader recordInfoReader = new UpsertUtils.RecordInfoReader(segment, _primaryKeyColumns,
        _comparisonColumns, _deleteRecordColumn)) {
      addSegment(segment, null, null, UpsertUtils.getRecordInfoIterator(recordInfoReader, validDocIds), true);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while preloading segment: %s, table: %s", segmentName, _tableNameWithType),
          e);
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);
    _logger.info("Finished preloading segment: {} in {}ms, current primary key count: {}", segmentName,
        System.currentTimeMillis() - startTimeMs, numPrimaryKeys);
  }

  /**
   * NOTE: We allow passing in validDocIds and queryableDocIds here so that the value can be easily accessed from the
   *       tests. The passed in bitmaps should always be empty.
   */
  @VisibleForTesting
  public void addSegment(ImmutableSegmentImpl segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator) {
    addSegment(segment, validDocIds, queryableDocIds, recordInfoIterator, false);
  }

  @VisibleForTesting
  public void addSegment(ImmutableSegmentImpl segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
      boolean isPreloading) {
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
      if (isPreloading) {
        addSegmentWithoutUpsert(segment, validDocIds, queryableDocIds, recordInfoIterator);
      } else {
        addOrReplaceSegment(segment, validDocIds, queryableDocIds, recordInfoIterator, null, null);
      }
    } finally {
      segmentLock.unlock();
    }
  }

  protected abstract long getNumPrimaryKeys();

  protected abstract void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
      @Nullable IndexSegment oldSegment, @Nullable MutableRoaringBitmap validDocIdsForOldSegment);

  protected void addSegmentWithoutUpsert(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator) {
    addOrReplaceSegment(segment, validDocIds, queryableDocIds, recordInfoIterator, null, null);
  }

  @Override
  public boolean addRecord(MutableSegment segment, RecordInfo recordInfo) {
    _gotFirstConsumingSegment = true;
    if (!startOperation()) {
      _logger.debug("Skip adding record to segment: {} because metadata manager is already stopped",
          segment.getSegmentName());
      return false;
    }
    // NOTE: We don't acquire snapshot read lock here because snapshot is always taken before a new consuming segment
    //       starts consuming, so it won't overlap with this method
    try {
      boolean addRecord = doAddRecord(segment, recordInfo);
      _trackedSegments.add(segment);
      return addRecord;
    } finally {
      finishOperation();
    }
  }

  // doAddRecord returns whether the event is out-of-order or not, if out-of-order returns false else true
  protected abstract boolean doAddRecord(MutableSegment segment, RecordInfo recordInfo);

  @Override
  public void replaceSegment(ImmutableSegment segment, IndexSegment oldSegment) {
    if (!startOperation()) {
      _logger.info("Skip replacing segment: {} because metadata manager is already stopped", segment.getSegmentName());
      return;
    }
    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    try {
      doReplaceSegment(segment, oldSegment);
      if (!(segment instanceof EmptyIndexSegment)) {
        _trackedSegments.add(segment);
      }
      _trackedSegments.remove(oldSegment);
    } finally {
      if (_enableSnapshot) {
        _snapshotLock.readLock().unlock();
      }
      finishOperation();
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

    // Currently when TTL is enabled, we don't support skip loading out-of-TTL segment with snapshots, since we don't
    // know which docs are valid in the new segment.
    // TODO: when ttl is enabled, we can allow
    //       (1) skip loading segments without any invalid docs.
    //       (2) assign the invalid docs from the replaced segment to the new segment.
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
    if (!_trackedSegments.contains(segment)) {
      _logger.info("Skip removing untracked (replaced or empty) segment: {}", segmentName);
      return;
    }
    // Skip removing segment that has max comparison value smaller than (largestSeenComparisonValue - TTL)
    if (_largestSeenComparisonValue > 0) {
      Number maxComparisonValue =
          (Number) segment.getSegmentMetadata().getColumnMetadataMap().get(_comparisonColumns.get(0)).getMaxValue();
      if (maxComparisonValue.doubleValue() < _largestSeenComparisonValue - _metadataTTL) {
        _logger.info("Skip removing segment: {} because it's out of TTL", segmentName);
        return;
      }
    }
    if (!startOperation()) {
      _logger.info("Skip removing segment: {} because metadata manager is already stopped", segmentName);
      return;
    }
    if (_enableSnapshot) {
      _snapshotLock.readLock().lock();
    }
    try {
      doRemoveSegment(segment);
      _trackedSegments.remove(segment);
    } finally {
      if (_enableSnapshot) {
        _snapshotLock.readLock().unlock();
      }
      finishOperation();
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
    if (!startOperation()) {
      _logger.debug("Skip updating record because metadata manager is already stopped");
      return record;
    }
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
          (isPartialUpsertTable ? "partial" : "full"), _tableNameWithType, currentComparisonValue,
          recordComparisonValue);
      _lastOutOfOrderEventReportTimeNs = currentTimeNs;
      _numOutOfOrderEvents = 0;
    }
  }

  @Override
  public void takeSnapshot() {
    if (!_enableSnapshot) {
      return;
    }
    if (!_gotFirstConsumingSegment) {
      _logger.info("Skip taking snapshot before getting the first consuming segment");
      return;
    }
    if (!startOperation()) {
      _logger.info("Skip taking snapshot because metadata manager is already stopped");
      return;
    }
    _snapshotLock.writeLock().lock();
    try {
      doTakeSnapshot();
    } finally {
      _snapshotLock.writeLock().unlock();
      finishOperation();
    }
  }

  // TODO: Consider optimizing it by tracking and persisting only the changed snapshot
  protected void doTakeSnapshot() {
    int numTrackedSegments = _trackedSegments.size();
    long numPrimaryKeysInSnapshot = 0L;
    _logger.info("Taking snapshot for {} segments", numTrackedSegments);
    long startTimeMs = System.currentTimeMillis();

    int numImmutableSegments = 0;
    for (IndexSegment segment : _trackedSegments) {
      if (segment instanceof ImmutableSegmentImpl) {
        ((ImmutableSegmentImpl) segment).persistValidDocIdsSnapshot();
        numImmutableSegments++;
        numPrimaryKeysInSnapshot += segment.getValidDocIds().getMutableRoaringBitmap().getCardinality();
      }
    }

    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId,
        ServerGauge.UPSERT_VALID_DOC_ID_SNAPSHOT_COUNT, numImmutableSegments);
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId,
        ServerGauge.UPSERT_PRIMARY_KEYS_IN_SNAPSHOT_COUNT, numPrimaryKeysInSnapshot);
    _logger.info("Finished taking snapshot for {} immutable segments (out of {} total segments) in {}ms",
        numImmutableSegments, numTrackedSegments, System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Loads watermark from the file if exists.
   */
  protected double loadWatermark() {
    File watermarkFile = getWatermarkFile();
    if (watermarkFile.exists()) {
      try {
        byte[] bytes = FileUtils.readFileToByteArray(watermarkFile);
        double watermark = ByteBuffer.wrap(bytes).getDouble();
        _logger.info("Loaded watermark: {} from file for table: {} partition_id: {}", watermark, _tableNameWithType,
            _partitionId);
        return watermark;
      } catch (Exception e) {
        _logger.warn("Caught exception while loading watermark file: {}, skipping", watermarkFile);
      }
    }
    return Double.MIN_VALUE;
  }

  /**
   * Persists watermark to the file.
   */
  protected void persistWatermark(double watermark) {
    File watermarkFile = getWatermarkFile();
    try {
      if (watermarkFile.exists()) {
        if (!FileUtils.deleteQuietly(watermarkFile)) {
          _logger.warn("Cannot delete watermark file: {}, skipping", watermarkFile);
          return;
        }
      }
      try (OutputStream outputStream = new FileOutputStream(watermarkFile, false);
          DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
        dataOutputStream.writeDouble(watermark);
      }
      _logger.info("Persisted watermark: {} to file: {}", watermark, watermarkFile);
    } catch (Exception e) {
      _logger.warn("Caught exception while persisting watermark file: {}, skipping", watermarkFile);
    }
  }

  /**
   * Deletes the watermark file.
   */
  protected void deleteWatermark() {
    File watermarkFile = getWatermarkFile();
    if (watermarkFile.exists()) {
      if (!FileUtils.deleteQuietly(watermarkFile)) {
        _logger.warn("Cannot delete watermark file: {}, skipping", watermarkFile);
      }
    }
  }

  protected File getWatermarkFile() {
    return new File(_tableIndexDir, V1Constants.TTL_WATERMARK_TABLE_PARTITION + _partitionId);
  }

  @Override
  public void removeExpiredPrimaryKeys() {
    if (_metadataTTL <= 0) {
      return;
    }
    if (!startOperation()) {
      _logger.info("Skip removing expired primary keys because metadata manager is already stopped");
      return;
    }
    try {
      doRemoveExpiredPrimaryKeys();
    } finally {
      finishOperation();
    }
  }

  /**
   * Removes all primary keys that have comparison value smaller than (largestSeenComparisonValue - TTL).
   */
  protected abstract void doRemoveExpiredPrimaryKeys();

  protected synchronized boolean startOperation() {
    if (_stopped || _numPendingOperations == 0) {
      return false;
    }
    _numPendingOperations++;
    return true;
  }

  protected synchronized void finishOperation() {
    _numPendingOperations--;
    if (_numPendingOperations == 0) {
      notifyAll();
    }
  }

  @Override
  public synchronized void stop() {
    if (_stopped) {
      _logger.warn("Metadata manager is already stopped");
      return;
    }
    _stopped = true;
    _numPendingOperations--;
    _logger.info("Stopped the metadata manager with {} pending operations, current primary key count: {}",
        _numPendingOperations, getNumPrimaryKeys());
  }

  @Override
  public synchronized void close()
      throws IOException {
    Preconditions.checkState(_stopped, "Must stop the metadata manager before closing it");
    if (_closed) {
      _logger.warn("Metadata manager is already closed");
      return;
    }
    _closed = true;
    _logger.info("Closing the metadata manager");
    while (_numPendingOperations != 0) {
      _logger.info("Waiting for {} pending operations to finish", _numPendingOperations);
      try {
        wait();
      } catch (InterruptedException e) {
        throw new RuntimeException(
            String.format("Interrupted while waiting for %d pending operations to finish", _numPendingOperations), e);
      }
    }
    doClose();
    _logger.info("Closed the metadata manager");
  }

  protected void doClose()
      throws IOException {
  }
}
