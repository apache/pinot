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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.EmptyIndexSegment;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the upsert metadata per partition.
 * <p>For multiple records with the same comparison value (default to timestamp), the manager will preserve the latest
 * record based on the sequence number of the segment. If 2 records with the same comparison value are in the same
 * segment, the one with larger doc id will be preserved. Note that for tables with sorted column, the records will be
 * re-ordered when committing the segment, and we will use the re-ordered doc ids instead of the ingestion doc ids to
 * decide the record to preserve.
 *
 * <p>There will be short term inconsistency when updating the upsert metadata, but should be consistent after the
 * operation is done:
 * <ul>
 *   <li>
 *     When updating a new record, it first removes the doc id from the current location, then update the new location.
 *   </li>
 *   <li>
 *     When adding a new segment, it removes the doc ids from the current locations before the segment being added to
 *     the RealtimeTableDataManager.
 *   </li>
 *   <li>
 *     When replacing an existing segment, after the record location being replaced with the new segment, the following
 *     updates applied to the new segment's valid doc ids won't be reflected to the replaced segment's valid doc ids.
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@ThreadSafe
public class PartitionUpsertMetadataManager {
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

  // Reused for reading previous record during partial upsert
  private final GenericRow _reuse = new GenericRow();

  private long _lastOutOfOrderEventReportTimeNs = Long.MIN_VALUE;
  private int _numOutOfOrderEvents = 0;

  public PartitionUpsertMetadataManager(String tableNameWithType, int partitionId, List<String> primaryKeyColumns,
      String comparisonColumn, HashFunction hashFunction, @Nullable PartialUpsertHandler partialUpsertHandler,
      ServerMetrics serverMetrics) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _primaryKeyColumns = primaryKeyColumns;
    _comparisonColumn = comparisonColumn;
    _hashFunction = hashFunction;
    _partialUpsertHandler = partialUpsertHandler;
    _serverMetrics = serverMetrics;
    _logger = LoggerFactory.getLogger(tableNameWithType + "-" + partitionId + "-" + getClass().getSimpleName());
  }

  /**
   * Returns the primary key columns.
   */
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  /**
   * Initializes the upsert metadata for the given immutable segment.
   */
  public void addSegment(ImmutableSegment segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Adding upsert metadata for segment: {}, primary key count: {}", segmentName,
        _primaryKeyToRecordLocationMap.size());

    if (segment instanceof EmptyIndexSegment) {
      _logger.info("Skip adding upsert metadata for empty segment: {}", segmentName);
      return;
    }

    Preconditions.checkArgument(segment instanceof ImmutableSegmentImpl,
        "Got unsupported segment implementation: {} for segment: {}, table: {}", segment.getClass(), segmentName,
        _tableNameWithType);
    addSegment((ImmutableSegmentImpl) segment, new ThreadSafeMutableRoaringBitmap(), getRecordInfoIterator(segment));

    // Update metrics
    int numPrimaryKeys = _primaryKeyToRecordLocationMap.size();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finish adding upsert metadata for segment: {}, primary key count: {}", segmentName, numPrimaryKeys);
  }

  private Iterator<RecordInfo> getRecordInfoIterator(ImmutableSegment segment) {
    int numTotalDocs = segment.getSegmentMetadata().getTotalDocs();
    return new Iterator<RecordInfo>() {
      private int _docId = 0;

      @Override
      public boolean hasNext() {
        return _docId < numTotalDocs;
      }

      @Override
      public RecordInfo next() {
        PrimaryKey primaryKey = new PrimaryKey(new Object[_primaryKeyColumns.size()]);
        getPrimaryKey(segment, _docId, primaryKey);

        Object comparisonValue = segment.getValue(_docId, _comparisonColumn);
        if (comparisonValue instanceof byte[]) {
          comparisonValue = new ByteArray((byte[]) comparisonValue);
        }
        return new RecordInfo(primaryKey, _docId++, (Comparable) comparisonValue);
      }
    };
  }

  private void getPrimaryKey(IndexSegment segment, int docId, PrimaryKey buffer) {
    Object[] values = buffer.getValues();
    int numPrimaryKeyColumns = values.length;
    for (int i = 0; i < numPrimaryKeyColumns; i++) {
      Object value = segment.getValue(docId, _primaryKeyColumns.get(i));
      if (value instanceof byte[]) {
        value = new ByteArray((byte[]) value);
      }
      values[i] = value;
    }
  }

  @VisibleForTesting
  void addSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    segment.enableUpsert(this, validDocIds);
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
              if (segment == currentSegment) {
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
              // doc ids for the old segment because it has not been replaced yet.
              String currentSegmentName = currentSegment.getSegmentName();
              if (segmentName.equals(currentSegmentName)) {
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
  }

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
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

  /**
   * Replaces the upsert metadata for the old segment with the new immutable segment.
   */
  public void replaceSegment(ImmutableSegment newSegment, IndexSegment oldSegment) {
    String segmentName = newSegment.getSegmentName();
    Preconditions.checkArgument(segmentName.equals(oldSegment.getSegmentName()),
        "Cannot replace segment with different name for table: {}, old segment: {}, new segment: {}",
        _tableNameWithType, oldSegment.getSegmentName(), segmentName);
    _logger.info("Replacing upsert metadata for {} segment: {}",
        oldSegment instanceof ImmutableSegment ? "immutable" : "mutable", segmentName);

    addSegment(newSegment);

    MutableRoaringBitmap validDocIds =
        oldSegment.getValidDocIds() != null ? oldSegment.getValidDocIds().getMutableRoaringBitmap() : null;
    if (validDocIds != null && !validDocIds.isEmpty()) {
      int numDocsNotReplaced = validDocIds.getCardinality();
      if (_partialUpsertHandler != null) {
        _logger.error("Got {} primary keys not replaced when replacing segment: {} for partial upsert table. This can "
            + "potentially cause inconsistency between replicas", numDocsNotReplaced, segmentName);
        _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.PARTIAL_UPSERT_ROWS_NOT_REPLACED,
            numDocsNotReplaced);
      } else {
        _logger.info("Got {} primary keys not replaced when replacing segment: {}", numDocsNotReplaced, segmentName);
      }
      removeSegment(oldSegment);
    }

    _logger.info("Finish replacing upsert metadata for segment: {}", segmentName);
  }

  /**
   * Removes the upsert metadata for the given segment.
   */
  public void removeSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    _logger.info("Removing upsert metadata for segment: {}, primary key count: {}", segmentName,
        _primaryKeyToRecordLocationMap.size());

    MutableRoaringBitmap validDocIds =
        segment.getValidDocIds() != null ? segment.getValidDocIds().getMutableRoaringBitmap() : null;
    if (validDocIds == null || validDocIds.isEmpty()) {
      _logger.info("Skipping removing upsert metadata for segment without valid docs: {}", segmentName);
      return;
    }

    _logger.info("Trying to remove {} primary keys from upsert metadata for segment: {}", validDocIds.getCardinality(),
        segmentName);
    PrimaryKey primaryKey = new PrimaryKey(new Object[_primaryKeyColumns.size()]);
    PeekableIntIterator iterator = validDocIds.getIntIterator();
    while (iterator.hasNext()) {
      int docId = iterator.next();
      getPrimaryKey(segment, docId, primaryKey);
      _primaryKeyToRecordLocationMap.computeIfPresent(HashUtils.hashPrimaryKey(primaryKey, _hashFunction),
          (pk, recordLocation) -> {
            if (recordLocation.getSegment() == segment) {
              return null;
            }
            return recordLocation;
          });
    }

    // Update metrics
    int numPrimaryKeys = _primaryKeyToRecordLocationMap.size();
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        numPrimaryKeys);

    _logger.info("Finish removing upsert metadata for segment: {}, primary key count: {}", segmentName, numPrimaryKeys);
  }

  /**
   * Returns the merged record when partial-upsert is enabled.
   */
  public GenericRow updateRecord(GenericRow record, RecordInfo recordInfo) {
    // Directly return the record when partial-upsert is not enabled
    if (_partialUpsertHandler == null) {
      return record;
    }

    RecordLocation currentRecordLocation =
        _primaryKeyToRecordLocationMap.get(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction));
    if (currentRecordLocation != null) {
      // Existing primary key
      if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
        _reuse.clear();
        GenericRow previousRecord =
            currentRecordLocation.getSegment().getRecord(currentRecordLocation.getDocId(), _reuse);
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
}
