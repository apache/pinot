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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link PartitionUpsertMetadataManager} that is backed by a {@link ConcurrentHashMap}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@ThreadSafe
public class ConcurrentMapPartitionUpsertMetadataManager implements PartitionUpsertMetadataManager {
  private static final long OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS = TimeUnit.MINUTES.toNanos(1);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final List<String> _primaryKeyColumns;
  private final String _comparisonColumn;
  private final HashFunction _hashFunction;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final ServerMetrics _serverMetrics;
  private final Logger _logger;

  // TODO(upsert): consider an off-heap KV store to persist this mapping to improve the recovery speed.
  @VisibleForTesting
  final ConcurrentHashMap<Object, RecordLocation> _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();

  @VisibleForTesting
  final Set<IndexSegment> _replacedSegments = ConcurrentHashMap.newKeySet();

  // Reused for reading previous record during partial upsert
  private final GenericRow _reuse = new GenericRow();

  private long _lastOutOfOrderEventReportTimeNs = Long.MIN_VALUE;
  private int _numOutOfOrderEvents = 0;

  public ConcurrentMapPartitionUpsertMetadataManager(String tableNameWithType, int partitionId,
      List<String> primaryKeyColumns, String comparisonColumn, HashFunction hashFunction,
      @Nullable PartialUpsertHandler partialUpsertHandler, ServerMetrics serverMetrics) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _primaryKeyColumns = primaryKeyColumns;
    _comparisonColumn = comparisonColumn;
    _hashFunction = hashFunction;
    _partialUpsertHandler = partialUpsertHandler;
    _serverMetrics = serverMetrics;
    _logger = LoggerFactory.getLogger(tableNameWithType + "-" + partitionId + "-" + getClass().getSimpleName());
  }

  @Override
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  @Override
  public void addSegment(ImmutableSegment segment) {
    addSegment(segment, null, null);
  }

  @VisibleForTesting
  void addSegment(ImmutableSegment segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    _logger.info("Adding segment: {}, current primary key count: {}", segmentName,
        _primaryKeyToRecordLocationMap.size());

    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding empty segment: {}", segmentName);
      return;
    }

    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
          "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
          _tableNameWithType);
      if (validDocIds == null) {
        validDocIds = new ThreadSafeMutableRoaringBitmap();
      }
      if (recordInfoIterator == null) {
        recordInfoIterator = UpsertUtils.getRecordInfoIterator(segment, _primaryKeyColumns, _comparisonColumn);
      }
      addOrReplaceSegment((ImmutableSegmentImpl) segment, validDocIds, recordInfoIterator, null, null);
    } finally {
      segmentLock.unlock();
    }

    // Update metrics
    int numPrimaryKeys = _primaryKeyToRecordLocationMap.size();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finished adding segment: {}, current primary key count: {}", segmentName, numPrimaryKeys);
  }

  private void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      Iterator<RecordInfo> recordInfoIterator, @Nullable IndexSegment oldSegment,
      @Nullable MutableRoaringBitmap validDocIdsForOldSegment) {
    String segmentName = segment.getSegmentName();
    segment.enableUpsert(this, validDocIds);

    AtomicInteger numKeysInWrongSegment = new AtomicInteger();
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      _primaryKeyToRecordLocationMap.compute(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
          (primaryKey, currentRecordLocation) -> {
            if (currentRecordLocation != null) {
              // Existing primary key
              IndexSegment currentSegment = currentRecordLocation.getSegment();
              int comparisonResult =
                  recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue());

              // The current record is in the same segment
              // Update the record location when there is a tie to keep the newer record. Note that the record info
              // iterator will return records with incremental doc ids.
              if (currentSegment == segment) {
                if (comparisonResult >= 0) {
                  validDocIds.replace(currentRecordLocation.getDocId(), recordInfo.getDocId());
                  return new RecordLocation(segment, recordInfo.getDocId(), recordInfo.getComparisonValue());
                } else {
                  return currentRecordLocation;
                }
              }

              // The current record is in an old segment being replaced
              // This could happen when committing a consuming segment, or reloading a completed segment. In this
              // case, we want to update the record location when there is a tie because the record locations should
              // point to the new added segment instead of the old segment being replaced. Also, do not update the valid
              // doc ids for the old segment because it has not been replaced yet. We pass in an optional valid doc ids
              // snapshot for the old segment, which can be updated and used to track the docs not replaced yet.
              if (currentSegment == oldSegment) {
                if (comparisonResult >= 0) {
                  validDocIds.add(recordInfo.getDocId());
                  if (validDocIdsForOldSegment != null) {
                    validDocIdsForOldSegment.remove(currentRecordLocation.getDocId());
                  }
                  return new RecordLocation(segment, recordInfo.getDocId(), recordInfo.getComparisonValue());
                } else {
                  return currentRecordLocation;
                }
              }

              // This should not happen because the previously replaced segment should have all keys removed. We still
              // handle it here, and also track the number of keys not properly replaced previously.
              String currentSegmentName = currentSegment.getSegmentName();
              if (currentSegmentName.equals(segmentName)) {
                numKeysInWrongSegment.getAndIncrement();
                if (comparisonResult >= 0) {
                  validDocIds.add(recordInfo.getDocId());
                  return new RecordLocation(segment, recordInfo.getDocId(), recordInfo.getComparisonValue());
                } else {
                  return currentRecordLocation;
                }
              }

              // The current record is in a different segment
              // Update the record location when getting a newer comparison value, or the value is the same as the
              // current value, but the segment has a larger sequence number (the segment is newer than the current
              // segment).
              if (comparisonResult > 0 || (comparisonResult == 0 && LLCSegmentName.isLowLevelConsumerSegmentName(
                  segmentName) && LLCSegmentName.isLowLevelConsumerSegmentName(currentSegmentName)
                  && LLCSegmentName.getSequenceNumber(segmentName) > LLCSegmentName.getSequenceNumber(
                  currentSegmentName))) {
                Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentRecordLocation.getDocId());
                validDocIds.add(recordInfo.getDocId());
                return new RecordLocation(segment, recordInfo.getDocId(), recordInfo.getComparisonValue());
              } else {
                return currentRecordLocation;
              }
            } else {
              // New primary key
              validDocIds.add(recordInfo.getDocId());
              return new RecordLocation(segment, recordInfo.getDocId(), recordInfo.getComparisonValue());
            }
          });
    }
    int numKeys = numKeysInWrongSegment.get();
    if (numKeys > 0) {
      _logger.warn("Found {} primary keys in the wrong segment when adding segment: {}", numKeys, segmentName);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.UPSERT_KEYS_IN_WRONG_SEGMENT, numKeys);
    }
  }

  @Override
  public void addRecord(MutableSegment segment, RecordInfo recordInfo) {
    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    _primaryKeyToRecordLocationMap.compute(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
        (primaryKey, currentRecordLocation) -> {
          if (currentRecordLocation != null) {
            // Existing primary key

            // Update the record location when the new comparison value is greater than or equal to the current value.
            // Update the record location when there is a tie to keep the newer record.
            if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
              IndexSegment currentSegment = currentRecordLocation.getSegment();
              int currentDocId = currentRecordLocation.getDocId();
              if (segment == currentSegment) {
                validDocIds.replace(currentDocId, recordInfo.getDocId());
              } else {
                Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentDocId);
                validDocIds.add(recordInfo.getDocId());
              }
              return new RecordLocation(segment, recordInfo.getDocId(), recordInfo.getComparisonValue());
            } else {
              return currentRecordLocation;
            }
          } else {
            // New primary key
            validDocIds.add(recordInfo.getDocId());
            return new RecordLocation(segment, recordInfo.getDocId(), recordInfo.getComparisonValue());
          }
        });

    // Update metrics
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        _primaryKeyToRecordLocationMap.size());
  }

  @Override
  public void replaceSegment(ImmutableSegment segment, IndexSegment oldSegment) {
    replaceSegment(segment, null, null, oldSegment);
  }

  @VisibleForTesting
  void replaceSegment(ImmutableSegment segment, @Nullable ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable Iterator<RecordInfo> recordInfoIterator, IndexSegment oldSegment) {
    String segmentName = segment.getSegmentName();
    Preconditions.checkArgument(segmentName.equals(oldSegment.getSegmentName()),
        "Cannot replace segment with different name for table: {}, old segment: {}, new segment: {}",
        _tableNameWithType, oldSegment.getSegmentName(), segmentName);
    _logger.info("Replacing {} segment: {}, current primary key count: {}",
        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName,
        _primaryKeyToRecordLocationMap.size());

    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      MutableRoaringBitmap validDocIdsForOldSegment =
          oldSegment.getValidDocIds() != null ? oldSegment.getValidDocIds().getMutableRoaringBitmap() : null;
      if (segment instanceof EmptyIndexSegment) {
        _logger.info("Skip adding empty segment: {}", segmentName);
      } else {
        Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
            "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
            _tableNameWithType);
        if (validDocIds == null) {
          validDocIds = new ThreadSafeMutableRoaringBitmap();
        }
        if (recordInfoIterator == null) {
          recordInfoIterator = UpsertUtils.getRecordInfoIterator(segment, _primaryKeyColumns, _comparisonColumn);
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

    // Update metrics
    int numPrimaryKeys = _primaryKeyToRecordLocationMap.size();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finished replacing segment: {}, current primary key count: {}", segmentName, numPrimaryKeys);
  }

  @Override
  public void removeSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Removing {} segment: {}, current primary key count: {}",
        segment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName,
        _primaryKeyToRecordLocationMap.size());

    if (_replacedSegments.remove(segment)) {
      _logger.info("Skip removing replaced segment: {}", segmentName);
      return;
    }

    Lock segmentLock = SegmentLocks.getSegmentLock(_tableNameWithType, segmentName);
    segmentLock.lock();
    try {
      MutableRoaringBitmap validDocIds =
          segment.getValidDocIds() != null ? segment.getValidDocIds().getMutableRoaringBitmap() : null;
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
    int numPrimaryKeys = _primaryKeyToRecordLocationMap.size();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finished removing segment: {}, current primary key count: {}", segmentName, numPrimaryKeys);
  }

  private void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds) {
    assert !validDocIds.isEmpty();
    PrimaryKey primaryKey = new PrimaryKey(new Object[_primaryKeyColumns.size()]);
    PeekableIntIterator iterator = validDocIds.getIntIterator();
    while (iterator.hasNext()) {
      int docId = iterator.next();
      UpsertUtils.getPrimaryKey(segment, _primaryKeyColumns, docId, primaryKey);
      _primaryKeyToRecordLocationMap.computeIfPresent(HashUtils.hashPrimaryKey(primaryKey, _hashFunction),
          (pk, recordLocation) -> {
            if (recordLocation.getSegment() == segment) {
              return null;
            }
            return recordLocation;
          });
    }
  }

  @Override
  public GenericRow updateRecord(GenericRow record, RecordInfo recordInfo) {
    // Directly return the record when partial-upsert is not enabled
    if (_partialUpsertHandler == null) {
      return record;
    }

    AtomicReference<GenericRow> previousRecordReference = new AtomicReference<>();
    RecordLocation currentRecordLocation = _primaryKeyToRecordLocationMap.computeIfPresent(
        HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction), (pk, recordLocation) -> {
          if (recordInfo.getComparisonValue().compareTo(recordLocation.getComparisonValue()) >= 0) {
            _reuse.clear();
            previousRecordReference.set(recordLocation.getSegment().getRecord(recordLocation.getDocId(), _reuse));
          }
          return recordLocation;
        });
    if (currentRecordLocation != null) {
      // Existing primary key
      GenericRow previousRecord = previousRecordReference.get();
      if (previousRecord != null) {
        return _partialUpsertHandler.merge(previousRecord, record);
      } else {
        _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.PARTIAL_UPSERT_OUT_OF_ORDER, 1L);
        _numOutOfOrderEvents++;
        long currentTimeNs = System.nanoTime();
        if (currentTimeNs - _lastOutOfOrderEventReportTimeNs > OUT_OF_ORDER_EVENT_MIN_REPORT_INTERVAL_NS) {
          _logger.warn("Skipped {} out-of-order events for partial-upsert table (the last event has current comparison "
                  + "value: {}, record comparison value: {})", _numOutOfOrderEvents,
              currentRecordLocation.getComparisonValue(), recordInfo.getComparisonValue());
          _lastOutOfOrderEventReportTimeNs = currentTimeNs;
          _numOutOfOrderEvents = 0;
        }
        return record;
      }
    } else {
      // New primary key
      return record;
    }
  }

  @Override
  public void close() {
    _logger.info("Closing metadata manager for table {} and partition {}, current primary key count: {}",
        _tableNameWithType, _partitionId, _primaryKeyToRecordLocationMap.size());
  }

  @VisibleForTesting
  static class RecordLocation {
    private final IndexSegment _segment;
    private final int _docId;
    private final Comparable _comparisonValue;

    public RecordLocation(IndexSegment indexSegment, int docId, Comparable comparisonValue) {
      _segment = indexSegment;
      _docId = docId;
      _comparisonValue = comparisonValue;
    }

    public IndexSegment getSegment() {
      return _segment;
    }

    public int getDocId() {
      return _docId;
    }

    public Comparable getComparisonValue() {
      return _comparisonValue;
    }
  }
}
