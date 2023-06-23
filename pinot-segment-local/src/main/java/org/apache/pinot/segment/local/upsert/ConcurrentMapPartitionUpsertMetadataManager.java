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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
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

  @VisibleForTesting
  final ConcurrentHashMap<Object, RecordLocation> _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();

  // Reused for reading previous record during partial upsert
  private final GenericRow _reuse = new GenericRow();

  public ConcurrentMapPartitionUpsertMetadataManager(String tableNameWithType, int partitionId,
      List<String> primaryKeyColumns, List<String> comparisonColumns, HashFunction hashFunction,
      @Nullable PartialUpsertHandler partialUpsertHandler, boolean enableSnapshot, ServerMetrics serverMetrics) {
    super(tableNameWithType, partitionId, primaryKeyColumns, comparisonColumns, hashFunction, partialUpsertHandler,
        enableSnapshot, serverMetrics);
  }

  @Override
  protected long getNumPrimaryKeys() {
    return _primaryKeyToRecordLocationMap.size();
  }

  @Override
  protected void addOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
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
  protected void doAddRecord(MutableSegment segment, RecordInfo recordInfo) {
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
              handleOutOfOrderEvent(currentRecordLocation.getComparisonValue(), recordInfo.getComparisonValue());
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
  protected GenericRow doUpdateRecord(GenericRow record, RecordInfo recordInfo) {
    assert _partialUpsertHandler != null;
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
        return record;
      }
    } else {
      // New primary key
      return record;
    }
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
