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
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Implementation of {@link PartitionUpsertMetadataManager} that is backed by a {@link ConcurrentHashMap}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@ThreadSafe
public class ConcurrentMapPartitionUpsertMetadataManager extends BasePartitionUpsertMetadataManager {

  // Used to initialize a reference to previous row for merging in partial upsert
  private final LazyRow _reusePreviousRow = new LazyRow();

  @VisibleForTesting
  final ConcurrentHashMap<Object, RecordLocation> _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();

  public ConcurrentMapPartitionUpsertMetadataManager(String tableNameWithType, int partitionId,
      List<String> primaryKeyColumns, List<String> comparisonColumns, @Nullable String deleteRecordColumn,
      HashFunction hashFunction, @Nullable PartialUpsertHandler partialUpsertHandler, boolean enableSnapshot,
      boolean dropOutOfOrderRecord, double metadataTTL, File tableIndexDir, ServerMetrics serverMetrics) {
    super(tableNameWithType, partitionId, primaryKeyColumns, comparisonColumns, deleteRecordColumn, hashFunction,
        partialUpsertHandler, enableSnapshot, dropOutOfOrderRecord, metadataTTL, tableIndexDir, serverMetrics);
  }

  @Override
  protected long getNumPrimaryKeys() {
    return _primaryKeyToRecordLocationMap.size();
  }

  @Override
  protected void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
      @Nullable IndexSegment oldSegment, @Nullable MutableRoaringBitmap validDocIdsForOldSegment) {
    String segmentName = segment.getSegmentName();
    segment.enableUpsert(this, validDocIds, queryableDocIds);

    AtomicInteger numKeysInWrongSegment = new AtomicInteger();
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      int newDocId = recordInfo.getDocId();
      Comparable newComparisonValue = recordInfo.getComparisonValue();
      _primaryKeyToRecordLocationMap.compute(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
          (primaryKey, currentRecordLocation) -> {
            if (currentRecordLocation != null) {
              // Existing primary key
              IndexSegment currentSegment = currentRecordLocation.getSegment();
              int currentDocId = currentRecordLocation.getDocId();
              int comparisonResult = newComparisonValue.compareTo(currentRecordLocation.getComparisonValue());

              // The current record is in the same segment
              // Update the record location when there is a tie to keep the newer record. Note that the record info
              // iterator will return records with incremental doc ids.
              if (currentSegment == segment) {
                if (comparisonResult >= 0) {
                  replaceDocId(validDocIds, queryableDocIds, currentDocId, newDocId, recordInfo);
                  return new RecordLocation(segment, newDocId, newComparisonValue);
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
                  addDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
                  if (validDocIdsForOldSegment != null) {
                    validDocIdsForOldSegment.remove(currentDocId);
                  }
                  return new RecordLocation(segment, newDocId, newComparisonValue);
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
                  addDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
                  return new RecordLocation(segment, newDocId, newComparisonValue);
                } else {
                  return currentRecordLocation;
                }
              }

              // The current record is in a different segment
              // Update the record location when getting a newer comparison value, or the value is the same as the
              // current value, but the segment has a larger sequence number (the segment is newer than the current
              // segment).
              if (comparisonResult > 0 || (comparisonResult == 0 && LLCSegmentName.isLLCSegment(segmentName)
                  && LLCSegmentName.isLLCSegment(currentSegmentName)
                  && LLCSegmentName.getSequenceNumber(segmentName) > LLCSegmentName.getSequenceNumber(
                  currentSegmentName))) {
                removeDocId(currentSegment, currentDocId);
                addDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
                return new RecordLocation(segment, newDocId, newComparisonValue);
              } else {
                return currentRecordLocation;
              }
            } else {
              // New primary key
              addDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
              return new RecordLocation(segment, newDocId, newComparisonValue);
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
  protected void addSegmentWithoutUpsert(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator) {
    segment.enableUpsert(this, validDocIds, queryableDocIds);
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      int newDocId = recordInfo.getDocId();
      Comparable newComparisonValue = recordInfo.getComparisonValue();
      addDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
      _primaryKeyToRecordLocationMap.put(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
          new RecordLocation(segment, newDocId, newComparisonValue));
    }
  }

  private static void replaceDocId(ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, int oldDocId, int newDocId, RecordInfo recordInfo) {
    validDocIds.replace(oldDocId, newDocId);
    if (queryableDocIds != null) {
      if (recordInfo.isDeleteRecord()) {
        queryableDocIds.remove(oldDocId);
      } else {
        queryableDocIds.replace(oldDocId, newDocId);
      }
    }
  }

  private static void addDocId(ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, int docId, RecordInfo recordInfo) {
    validDocIds.add(docId);
    if (queryableDocIds != null && !recordInfo.isDeleteRecord()) {
      queryableDocIds.add(docId);
    }
  }

  private static void removeDocId(IndexSegment segment, int docId) {
    Objects.requireNonNull(segment.getValidDocIds()).remove(docId);
    ThreadSafeMutableRoaringBitmap currentQueryableDocIds = segment.getQueryableDocIds();
    if (currentQueryableDocIds != null) {
      currentQueryableDocIds.remove(docId);
    }
  }

  @Override
  protected void removeSegment(IndexSegment segment, MutableRoaringBitmap validDocIds) {
    assert !validDocIds.isEmpty();

    PrimaryKey primaryKey = new PrimaryKey(new Object[_primaryKeyColumns.size()]);
    PeekableIntIterator iterator = validDocIds.getIntIterator();
    try (
        UpsertUtils.PrimaryKeyReader primaryKeyReader = new UpsertUtils.PrimaryKeyReader(segment, _primaryKeyColumns)) {
      while (iterator.hasNext()) {
        primaryKeyReader.getPrimaryKey(iterator.next(), primaryKey);
        _primaryKeyToRecordLocationMap.computeIfPresent(HashUtils.hashPrimaryKey(primaryKey, _hashFunction),
            (pk, recordLocation) -> {
              if (recordLocation.getSegment() == segment) {
                return null;
              }
              return recordLocation;
            });
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while removing segment: %s, table: %s", segment.getSegmentName(),
              _tableNameWithType), e);
    }
  }

  @Override
  public void doRemoveExpiredPrimaryKeys() {
    double threshold = _largestSeenComparisonValue - _metadataTTL;
    _primaryKeyToRecordLocationMap.forEach((primaryKey, recordLocation) -> {
      if (((Number) recordLocation.getComparisonValue()).doubleValue() < threshold) {
        _primaryKeyToRecordLocationMap.remove(primaryKey, recordLocation);
      }
    });
    persistWatermark(_largestSeenComparisonValue);
  }

  @Override
  protected boolean doAddRecord(MutableSegment segment, RecordInfo recordInfo) {
    AtomicBoolean shouldDropRecord = new AtomicBoolean(false);
    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    ThreadSafeMutableRoaringBitmap queryableDocIds = segment.getQueryableDocIds();
    int newDocId = recordInfo.getDocId();
    Comparable newComparisonValue = recordInfo.getComparisonValue();

    // When TTL is enabled, update largestSeenComparisonValue when adding new record
    if (_metadataTTL > 0) {
      double comparisonValue = ((Number) newComparisonValue).doubleValue();
      _largestSeenComparisonValue = Math.max(_largestSeenComparisonValue, comparisonValue);
    }

    _primaryKeyToRecordLocationMap.compute(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
        (primaryKey, currentRecordLocation) -> {
          if (currentRecordLocation != null) {
            // Existing primary key

            // Update the record location when the new comparison value is greater than or equal to the current value.
            // Update the record location when there is a tie to keep the newer record.
            if (newComparisonValue.compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
              IndexSegment currentSegment = currentRecordLocation.getSegment();
              int currentDocId = currentRecordLocation.getDocId();
              if (segment == currentSegment) {
                replaceDocId(validDocIds, queryableDocIds, currentDocId, newDocId, recordInfo);
              } else {
                removeDocId(currentSegment, currentDocId);
                addDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
              }
              return new RecordLocation(segment, newDocId, newComparisonValue);
            } else {
              handleOutOfOrderEvent(currentRecordLocation.getComparisonValue(), recordInfo.getComparisonValue());
              // this is a out-of-order record, if upsert config _dropOutOfOrderRecord is true, then set
              // shouldDropRecord to true. This method returns inverse of this value
              shouldDropRecord.set(_dropOutOfOrderRecord);
              return currentRecordLocation;
            }
          } else {
            // New primary key
            addDocId(validDocIds, queryableDocIds, newDocId, recordInfo);
            return new RecordLocation(segment, newDocId, newComparisonValue);
          }
        });

    // Update metrics
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        _primaryKeyToRecordLocationMap.size());
    return !shouldDropRecord.get();
  }

  @Override
  protected GenericRow doUpdateRecord(GenericRow record, RecordInfo recordInfo) {
    assert _partialUpsertHandler != null;
    _primaryKeyToRecordLocationMap.computeIfPresent(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
        (pk, recordLocation) -> {
          // Read the previous record if the following conditions are met:
          // - New record is not a DELETE record
          // - New record is not out-of-order
          // - Previous record is not deleted
          if (!recordInfo.isDeleteRecord()
              && recordInfo.getComparisonValue().compareTo(recordLocation.getComparisonValue()) >= 0) {
            IndexSegment currentSegment = recordLocation.getSegment();
            ThreadSafeMutableRoaringBitmap currentQueryableDocIds = currentSegment.getQueryableDocIds();
            int currentDocId = recordLocation.getDocId();
            if (currentQueryableDocIds == null || currentQueryableDocIds.contains(currentDocId)) {
              _reusePreviousRow.init(currentSegment, currentDocId);
              _partialUpsertHandler.merge(_reusePreviousRow, record);
            }
          }
          return recordLocation;
        });
    return record;
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
