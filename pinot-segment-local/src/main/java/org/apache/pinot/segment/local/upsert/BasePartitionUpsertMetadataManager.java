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
import java.util.concurrent.locks.Lock;
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
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BasePartitionUpsertMetadataManager implements PartitionUpsertMetadataManager {
  protected static final long OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS = TimeUnit.MINUTES.toNanos(1);

  protected final String _tableNameWithType;
  protected final int _partitionId;
  protected final List<String> _primaryKeyColumns;
  protected final String _comparisonColumn;
  protected final HashFunction _hashFunction;
  protected final PartialUpsertHandler _partialUpsertHandler;
  protected final boolean _enableSnapshot;
  protected final ServerMetrics _serverMetrics;
  protected final Logger _logger;

  @VisibleForTesting
  public final Set<IndexSegment> _replacedSegments = ConcurrentHashMap.newKeySet();

  protected volatile boolean _closed = false;

  protected long _lastOutOfOrderEventReportTimeNs = Long.MIN_VALUE;
  protected int _numOutOfOrderEvents = 0;

  protected BasePartitionUpsertMetadataManager(String tableNameWithType, int partitionId,
      List<String> primaryKeyColumns, String comparisonColumn, HashFunction hashFunction,
      @Nullable PartialUpsertHandler partialUpsertHandler, boolean enableSnapshot, ServerMetrics serverMetrics) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _primaryKeyColumns = primaryKeyColumns;
    _comparisonColumn = comparisonColumn;
    _hashFunction = hashFunction;
    _partialUpsertHandler = partialUpsertHandler;
    _enableSnapshot = enableSnapshot;
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
    _logger.info("Adding segment: {}, current primary key count: {}", segmentName, getNumPrimaryKeys());

    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      return;
    }

    Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
        "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
        _tableNameWithType);

    ImmutableSegmentImpl immutableSegmentImpl = (ImmutableSegmentImpl) segment;
    MutableRoaringBitmap validDocIds;
    if (_enableSnapshot) {
      validDocIds = immutableSegmentImpl.loadValidDocIdsFromSnapshot();
      if (validDocIds != null && validDocIds.isEmpty()) {
        _logger.info("Skip adding segment: {} without valid doc, current primary key count: {}",
            segment.getSegmentName(), getNumPrimaryKeys());
        return;
      }
    } else {
      validDocIds = null;
      immutableSegmentImpl.deleteValidDocIdsSnapshot();
    }

    try (UpsertUtils.RecordInfoReader recordInfoReader = new UpsertUtils.RecordInfoReader(segment, _primaryKeyColumns,
        _comparisonColumn)) {
      Iterator<RecordInfo> recordInfoIterator;
      if (validDocIds != null) {
        recordInfoIterator = UpsertUtils.getRecordInfoIterator(recordInfoReader, validDocIds);
      } else {
        recordInfoIterator =
            UpsertUtils.getRecordInfoIterator(recordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      }
      addSegment(immutableSegmentImpl, null, recordInfoIterator);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while adding segment: %s, table: %s", segmentName, _tableNameWithType), e);
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finished adding segment: {}, current primary key count: {}", segmentName, numPrimaryKeys);
  }

  /**
   * NOTE: We allow passing in validDocIds here so that the value can be easily accessed from the tests. The passed in
   *       validDocIds should always be empty.
   */
  @VisibleForTesting
  public void addSegment(ImmutableSegmentImpl segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      if (validDocIds == null) {
        validDocIds = new ThreadSafeMutableRoaringBitmap();
      }
      addOrReplaceSegment(segment, validDocIds, recordInfoIterator, null, null);
    } finally {
      segmentLock.unlock();
    }
  }

  protected abstract long getNumPrimaryKeys();

  protected abstract void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      Iterator<RecordInfo> recordInfoIterator, @Nullable IndexSegment oldSegment,
      @Nullable MutableRoaringBitmap validDocIdsForOldSegment);

  @Override
  public void replaceSegment(ImmutableSegment segment, IndexSegment oldSegment) {
    String segmentName = segment.getSegmentName();
    Preconditions.checkArgument(segmentName.equals(oldSegment.getSegmentName()),
        "Cannot replace segment with different name for table: {}, old segment: {}, new segment: {}",
        _tableNameWithType, oldSegment.getSegmentName(), segmentName);
    _logger.info("Replacing {} segment: {}, current primary key count: {}",
        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, getNumPrimaryKeys());

    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      replaceSegment(segment, null, null, oldSegment);
      return;
    }

    try (UpsertUtils.RecordInfoReader recordInfoReader = new UpsertUtils.RecordInfoReader(segment, _primaryKeyColumns,
        _comparisonColumn)) {
      Iterator<RecordInfo> recordInfoIterator =
          UpsertUtils.getRecordInfoIterator(recordInfoReader, segment.getSegmentMetadata().getTotalDocs());
      replaceSegment(segment, null, recordInfoIterator, oldSegment);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while replacing segment: %s, table: %s", segmentName, _tableNameWithType), e);
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finished replacing segment: {}, current primary key count: {}", segmentName, numPrimaryKeys);
  }

  /**
   * NOTE: We allow passing in validDocIds here so that the value can be easily accessed from the tests. The passed in
   *       validDocIds should always be empty.
   */
  @VisibleForTesting
  public void replaceSegment(ImmutableSegment segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable Iterator<RecordInfo> recordInfoIterator, IndexSegment oldSegment) {
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
        addOrReplaceSegment((ImmutableSegmentImpl) segment, validDocIds, recordInfoIterator, oldSegment,
            validDocIdsForOldSegment);
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

    if (!(oldSegment instanceof EmptyIndexSegment)) {
      _replacedSegments.add(oldSegment);
    }
  }

  protected abstract void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds);

  @Override
  public void removeSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Removing {} segment: {}, current primary key count: {}",
        segment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, getNumPrimaryKeys());

    if (_replacedSegments.remove(segment)) {
      _logger.info("Skip removing replaced segment: {}", segmentName);
      return;
    }

    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      MutableRoaringBitmap validDocIds =
          segment.getValidDocIds() != null ? segment.getValidDocIds().getMutableRoaringBitmap() : null;

      if (_enableSnapshot && segment instanceof ImmutableSegmentImpl && validDocIds != null) {
        ((ImmutableSegmentImpl) segment).persistValidDocIdsSnapshot(validDocIds);
      }

      if (_closed) {
        _logger.info("Skip removing segment: {} because metadata manager is already closed", segment);
        return;
      }

      if (validDocIds == null || validDocIds.isEmpty()) {
        _logger.info("Skip removing segment without valid docs: {}", segmentName);
        return;
      }

      _logger.info("Removing {} primary keys for segment: {}", validDocIds.getCardinality(), segmentName);
      removeSegment(segment, validDocIds);
    } finally {
      segmentLock.unlock();
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finished removing segment: {}, current primary key count: {}", segmentName, numPrimaryKeys);
  }

  protected void handleOutOfOrderEvent(Object currentComparisonValue, Object recordComparisonValue) {
    _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.PARTIAL_UPSERT_OUT_OF_ORDER, 1L);
    _numOutOfOrderEvents++;
    long currentTimeNs = System.nanoTime();
    if (currentTimeNs - _lastOutOfOrderEventReportTimeNs > OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS) {
      _logger.warn("Skipped {} out-of-order events for partial-upsert table (the last event has current comparison "
              + "value: {}, record comparison value: {})", _numOutOfOrderEvents, currentComparisonValue,
          recordComparisonValue);
      _lastOutOfOrderEventReportTimeNs = currentTimeNs;
      _numOutOfOrderEvents = 0;
    }
  }

  @Override
  public void close()
      throws IOException {
    _logger.info("Closing the metadata manager, current primary key count: {}", getNumPrimaryKeys());
    _closed = true;
  }
}
