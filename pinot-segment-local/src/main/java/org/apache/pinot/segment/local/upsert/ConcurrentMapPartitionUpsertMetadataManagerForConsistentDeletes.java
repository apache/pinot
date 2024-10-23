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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.segment.readers.PrimaryKeyReader;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.SegmentLocks;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Implementation of {@link PartitionUpsertMetadataManager} that is backed by a {@link ConcurrentHashMap} and ensures
 * consistent deletions. This should be used when the table is configured with 'enableDeletedKeysCompactionConsistency'
 * set to true.
 *
 * Consistent deletion ensures that when deletedKeysTTL is enabled with UpsertCompaction, the key metadata is
 * removed from the HashMap only after all other records in the old segments are compacted. This guarantees
 * data consistency. Without this, there can be a scenario where a deleted record is compacted first, while an
 * old record remains non-compacted in a previous segment. During a server restart, this could lead to the old
 * record reappearing. For the end-user, this would result in a data loss or inconsistency scenario, as the
 * record was marked for deletion.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@ThreadSafe
public class ConcurrentMapPartitionUpsertMetadataManagerForConsistentDeletes
    extends BasePartitionUpsertMetadataManager {

  @VisibleForTesting
  final ConcurrentHashMap<Object, ConcurrentMapPartitionUpsertMetadataManagerForConsistentDeletes.RecordLocation>
      _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();
  // Used to initialize a reference to previous row for merging in partial upsert
  private final LazyRow _reusePreviousRow = new LazyRow();
  private final Map<String, Object> _reuseMergeResultHolder = new HashMap<>();

  public ConcurrentMapPartitionUpsertMetadataManagerForConsistentDeletes(String tableNameWithType, int partitionId,
      UpsertContext context) {
    super(tableNameWithType, partitionId, context);
  }

  @Override
  protected long getNumPrimaryKeys() {
    return _primaryKeyToRecordLocationMap.size();
  }

  @Override
  protected void doAddOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
      @Nullable IndexSegment oldSegment, @Nullable MutableRoaringBitmap validDocIdsForOldSegment) {
    if (_partialUpsertHandler == null) {
      // for full upsert, we are de-duping primary key once here to make sure that we are not adding
      // primary-key multiple times and subtracting just once in removeSegment.
      // for partial-upsert, we call this method in base class.
      recordInfoIterator = resolveComparisonTies(recordInfoIterator, _hashFunction);
    }
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
              Comparable currentComparisonValue = currentRecordLocation.getComparisonValue();
              int comparisonResult = newComparisonValue.compareTo(currentComparisonValue);
              int currentDistinctSegmentCount = currentRecordLocation.getDistinctSegmentCount();

              // The current record is in the same segment
              // Update the record location when there is a tie to keep the newer record. Note that the record info
              // iterator will return records with incremental doc ids.
              if (currentSegment == segment) {
                if (comparisonResult >= 0) {
                  replaceDocId(segment, validDocIds, queryableDocIds, currentDocId, newDocId, recordInfo);
                  return new ConcurrentMapPartitionUpsertMetadataManagerForConsistentDeletes.RecordLocation(segment,
                      newDocId, newComparisonValue, currentDistinctSegmentCount);
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
                  if (validDocIdsForOldSegment == null && oldSegment != null && oldSegment.getValidDocIds() != null) {
                    // Update the old segment's bitmap in place if a copy of the bitmap was not provided.
                    replaceDocId(segment, validDocIds, queryableDocIds, oldSegment, currentDocId, newDocId, recordInfo);
                  } else {
                    addDocId(segment, validDocIds, queryableDocIds, newDocId, recordInfo);
                    if (validDocIdsForOldSegment != null) {
                      validDocIdsForOldSegment.remove(currentDocId);
                    }
                  }
                  return new RecordLocation(segment, newDocId, newComparisonValue,
                      RecordLocation.incrementSegmentCount(currentDistinctSegmentCount));
                } else {
                  return new RecordLocation(currentSegment, currentDocId, currentComparisonValue,
                      RecordLocation.incrementSegmentCount(currentDistinctSegmentCount));
                }
              }

              // This should not happen because the previously replaced segment should have all keys removed. We still
              // handle it here, and also track the number of keys not properly replaced previously.
              String currentSegmentName = currentSegment.getSegmentName();
              if (currentSegmentName.equals(segmentName)) {
                numKeysInWrongSegment.getAndIncrement();
                if (comparisonResult >= 0) {
                  addDocId(segment, validDocIds, queryableDocIds, newDocId, recordInfo);
                  return new RecordLocation(segment, newDocId, newComparisonValue,
                      RecordLocation.incrementSegmentCount(currentDistinctSegmentCount));
                } else {
                  return currentRecordLocation;
                }
              }

              // The current record is in a different segment
              // Update the record location when getting a newer comparison value, or the value is the same as the
              // current value, but the segment has a larger sequence number (the segment is newer than the current
              // segment).
              if (comparisonResult > 0 || (comparisonResult == 0 && shouldReplaceOnComparisonTie(segmentName,
                  currentSegmentName, segment.getSegmentMetadata().getIndexCreationTime(),
                  currentSegment.getSegmentMetadata().getIndexCreationTime()))) {
                replaceDocId(segment, validDocIds, queryableDocIds, currentSegment, currentDocId, newDocId, recordInfo);
                return new RecordLocation(segment, newDocId, newComparisonValue,
                    RecordLocation.incrementSegmentCount(currentDistinctSegmentCount));
              } else {
                return new RecordLocation(currentSegment, currentDocId, currentComparisonValue,
                    RecordLocation.incrementSegmentCount(currentDistinctSegmentCount));
              }
            } else {
              // New primary key
              addDocId(segment, validDocIds, queryableDocIds, newDocId, recordInfo);
              return new RecordLocation(segment, newDocId, newComparisonValue, 1);
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
    throw new UnsupportedOperationException("Consistent-deletion does not support preloading of segments.");
  }

  @Override
  protected void doRemoveSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Removing {} segment: {}, current primary key count: {}",
        segment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName, getNumPrimaryKeys());
    long startTimeMs = System.currentTimeMillis();

    try (PrimaryKeyReader primaryKeyReader = new PrimaryKeyReader(segment, _primaryKeyColumns)) {
      removeSegment(segment,
          UpsertUtils.getPrimaryKeyIterator(primaryKeyReader, segment.getSegmentMetadata().getTotalDocs()));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Caught exception while removing segment: %s, table: %s", segment.getSegmentName(),
              _tableNameWithType), e);
    }

    // Update metrics
    long numPrimaryKeys = getNumPrimaryKeys();
    updatePrimaryKeyGauge(numPrimaryKeys);
    _logger.info("Finished removing segment: {} in {}ms, current primary key count: {}", segmentName,
        System.currentTimeMillis() - startTimeMs, numPrimaryKeys);
  }

  @Override
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
      if (validDocIdsForOldSegment != null && !validDocIdsForOldSegment.isEmpty() && _partialUpsertHandler != null) {
        int numKeysNotReplaced = validDocIdsForOldSegment.getCardinality();
        // For partial-upsert table, because we do not restore the original record location when removing the primary
        // keys not replaced, it can potentially cause inconsistency between replicas. This can happen when a
        // consuming segment is replaced by a committed segment that is consumed from a different server with
        // different records (some stream consumer cannot guarantee consuming the messages in the same order).
        _logger.warn("Found {} primary keys not replaced when replacing segment: {} for partial-upsert table. This "
            + "can potentially cause inconsistency between replicas", numKeysNotReplaced, segmentName);
        _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.PARTIAL_UPSERT_KEYS_NOT_REPLACED,
            numKeysNotReplaced);
      }
      // we want to always remove a segment in case of enableDeletedKeysCompactionConsistency = true
      // this is to account for the removal of primary-key in the to-be-removed segment and reduce
      // distinctSegmentCount by 1
      doRemoveSegment(oldSegment);
    } finally {
      segmentLock.unlock();
    }
  }

  @Override
  protected void removeSegment(IndexSegment segment, Iterator<PrimaryKey> primaryKeyIterator) {
    // We need to decrease the distinctSegmentCount for each unique primary key in this deleting segment by 1
    // as the occurrence of the key in this segment is being removed. We are taking a set of unique primary keys
    // to avoid double counting the same key in the same segment.
    Set<Object> uniquePrimaryKeys = new HashSet<>();
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey primaryKey = primaryKeyIterator.next();
      _primaryKeyToRecordLocationMap.computeIfPresent(HashUtils.hashPrimaryKey(primaryKey, _hashFunction),
          (pk, recordLocation) -> {
            if (recordLocation.getSegment() == segment) {
              return null;
            }
            if (!uniquePrimaryKeys.add(pk)) {
              return recordLocation;
            }
            return new RecordLocation(recordLocation.getSegment(), recordLocation.getDocId(),
                recordLocation.getComparisonValue(),
                RecordLocation.decrementSegmentCount(recordLocation.getDistinctSegmentCount()));
          });
    }
  }

  @Override
  public void doRemoveExpiredPrimaryKeys() {
    AtomicInteger numTotalKeysMarkForDeletion = new AtomicInteger();
    AtomicInteger numDeletedTTLKeysRemoved = new AtomicInteger();
    AtomicInteger numDeletedKeysWithinTTLWindow = new AtomicInteger();
    AtomicInteger numDeletedTTLKeysInMultipleSegments = new AtomicInteger();
    double largestSeenComparisonValue = _largestSeenComparisonValue.get();
    double deletedKeysThreshold =
        _deletedKeysTTL > 0 ? largestSeenComparisonValue - _deletedKeysTTL : Double.NEGATIVE_INFINITY;
    _primaryKeyToRecordLocationMap.forEach((primaryKey, recordLocation) -> {
      double comparisonValue = ((Number) recordLocation.getComparisonValue()).doubleValue();
      // We need to verify that the record belongs to only one segment. If a record is part of multiple segments,
      // an issue can arise where the upsert compaction might first process the segment containing the delete record
      // while the previous segment(s) are not compacted. Upon restart, this can inadvertently revive the key
      // that was originally marked for deletion.
      if (_deletedKeysTTL > 0) {
        ThreadSafeMutableRoaringBitmap currentQueryableDocIds = recordLocation.getSegment().getQueryableDocIds();
        // if key not part of queryable doc id, it means it is deleted
        if (currentQueryableDocIds != null && !currentQueryableDocIds.contains(recordLocation.getDocId())) {
          numTotalKeysMarkForDeletion.getAndIncrement();
          if (comparisonValue >= deletedKeysThreshold) {
            // If key is within the TTL window, do not remove it from the primary hashmap
            numDeletedKeysWithinTTLWindow.getAndIncrement();
          } else if (recordLocation.getDistinctSegmentCount() > 1) {
            // If key is part of multiple segments, do not remove it from the primary hashmap
            numDeletedTTLKeysInMultipleSegments.getAndIncrement();
          } else {
            // delete key from primary hashmap
            _primaryKeyToRecordLocationMap.remove(primaryKey, recordLocation);
            removeDocId(recordLocation.getSegment(), recordLocation.getDocId());
            numDeletedTTLKeysRemoved.getAndIncrement();
          }
        }
      }
    });

    // Update metrics
    updatePrimaryKeyGauge();
    int numTotalKeysMarkedForDeletion = numTotalKeysMarkForDeletion.get();
    if (numTotalKeysMarkedForDeletion > 0) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.TOTAL_KEYS_MARKED_FOR_DELETION,
          numTotalKeysMarkedForDeletion);
    }
    int numDeletedKeysWithinTTLWindowValue = numDeletedKeysWithinTTLWindow.get();
    if (numDeletedKeysWithinTTLWindowValue > 0) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETED_KEYS_WITHIN_TTL_WINDOW,
          numDeletedKeysWithinTTLWindowValue);
    }
    int numDeletedTTLKeysInMultipleSegmentsValue = numDeletedTTLKeysInMultipleSegments.get();
    if (numDeletedTTLKeysInMultipleSegmentsValue > 0) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETED_TTL_KEYS_IN_MULTIPLE_SEGMENTS,
          numDeletedTTLKeysInMultipleSegmentsValue);
    }
    int numDeletedTTLKeys = numDeletedTTLKeysRemoved.get();
    if (numDeletedTTLKeys > 0) {
      _logger.info("Deleted {} primary keys based on deletedKeysTTL", numDeletedTTLKeys);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETED_KEYS_TTL_PRIMARY_KEYS_REMOVED,
          numDeletedTTLKeys);
    }
  }

  @Override
  protected boolean doAddRecord(MutableSegment segment, RecordInfo recordInfo) {
    AtomicBoolean isOutOfOrderRecord = new AtomicBoolean(false);
    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    ThreadSafeMutableRoaringBitmap queryableDocIds = segment.getQueryableDocIds();
    int newDocId = recordInfo.getDocId();
    Comparable newComparisonValue = recordInfo.getComparisonValue();

    // When TTL is enabled, update largestSeenComparisonValue when adding new record
    if (_deletedKeysTTL > 0) {
      double comparisonValue = ((Number) newComparisonValue).doubleValue();
      _largestSeenComparisonValue.getAndUpdate(v -> Math.max(v, comparisonValue));
    }

    _primaryKeyToRecordLocationMap.compute(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
        (primaryKey, currentRecordLocation) -> {
          if (currentRecordLocation != null) {
            // Existing primary key
            IndexSegment currentSegment = currentRecordLocation.getSegment();
            // Update the record location when the new comparison value is greater than or equal to the current value.
            // Update the record location when there is a tie to keep the newer record.
            if (newComparisonValue.compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
              int currentDocId = currentRecordLocation.getDocId();
              if (segment == currentSegment) {
                replaceDocId(segment, validDocIds, queryableDocIds, currentDocId, newDocId, recordInfo);
                return new RecordLocation(segment, newDocId, newComparisonValue,
                    currentRecordLocation.getDistinctSegmentCount());
              } else {
                replaceDocId(segment, validDocIds, queryableDocIds, currentSegment, currentDocId, newDocId, recordInfo);
                return new RecordLocation(segment, newDocId, newComparisonValue,
                    RecordLocation.incrementSegmentCount(currentRecordLocation.getDistinctSegmentCount()));
              }
            } else {
              // Out-of-order record
              handleOutOfOrderEvent(currentRecordLocation.getComparisonValue(), recordInfo.getComparisonValue());
              isOutOfOrderRecord.set(true);
              if (segment == currentSegment) {
                return currentRecordLocation;
              } else {
                return new RecordLocation(currentSegment, currentRecordLocation.getDocId(),
                    currentRecordLocation.getComparisonValue(),
                    _context.isDropOutOfOrderRecord() ? currentRecordLocation.getDistinctSegmentCount()
                        : RecordLocation.incrementSegmentCount(currentRecordLocation.getDistinctSegmentCount()));
              }
            }
          } else {
            // New primary key
            addDocId(segment, validDocIds, queryableDocIds, newDocId, recordInfo);
            return new RecordLocation(segment, newDocId, newComparisonValue, 1);
          }
        });

    updatePrimaryKeyGauge();
    return !isOutOfOrderRecord.get();
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
              _partialUpsertHandler.merge(_reusePreviousRow, record, _reuseMergeResultHolder);
              _reuseMergeResultHolder.clear();
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
    // The number of distinct segments in which the record is present. If this count is less than or equal to 1,
    // we proceed to remove the record from the primary hashmap during the deletedKeysTTL process.
    private final int _distinctSegmentCount;

    public RecordLocation(IndexSegment indexSegment, int docId, Comparable comparisonValue, int distinctSegmentCount) {
      _segment = indexSegment;
      _docId = docId;
      _comparisonValue = comparisonValue;
      _distinctSegmentCount = distinctSegmentCount;
    }

    public static int incrementSegmentCount(int count) {
      return count + 1;
    }

    public static int decrementSegmentCount(int count) {
      return count - 1;
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

    public int getDistinctSegmentCount() {
      return _distinctSegmentCount;
    }
  }
}
