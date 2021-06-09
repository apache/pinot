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
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the upsert metadata per partition.
 * <p>For multiple records with the same timestamp, the manager will preserve the latest record based on the sequence
 * number of the segment. If 2 records with the same timestamp are in the same segment, the one with larger doc id will
 * be preserved. Note that for tables with sorted column, the records will be re-ordered when committing the segment,
 * and we will use the re-ordered doc ids instead of the ingestion order to decide which record to preserve.
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
@ThreadSafe
public class PartitionUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertMetadataManager.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;

  // TODO(upset): consider an off-heap KV store to persist this index to improve the recovery speed.
  @VisibleForTesting
  final ConcurrentHashMap<PrimaryKey, RecordLocation> _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();

  // Reused for reading previous record during partial upsert
  private final GenericRow _reuse = new GenericRow();
  // Stores the result of updateRecord()
  private GenericRow _result;

  public PartitionUpsertMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
  }

  /**
   * Initializes the upsert metadata for the given immutable segment.
   */
  public void addSegment(ImmutableSegmentImpl immutableSegment, Iterator<RecordInfo> recordInfoIterator) {
    addSegment(immutableSegment, recordInfoIterator, new ThreadSafeMutableRoaringBitmap());
  }

  @VisibleForTesting
  void addSegment(ImmutableSegmentImpl immutableSegment, Iterator<RecordInfo> recordInfoIterator,
      ThreadSafeMutableRoaringBitmap validDocIds) {
    String segmentName = immutableSegment.getSegmentName();
    LOGGER.info("Adding upsert metadata for segment: {}", segmentName);
    immutableSegment.enableUpsert(this, validDocIds);

    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      _primaryKeyToRecordLocationMap.compute(recordInfo._primaryKey, (primaryKey, currentRecordLocation) -> {
        if (currentRecordLocation != null) {
          // Existing primary key

          // The current record is in the same segment
          // Update the record location when there is a tie to keep the newer record. Note that the record info iterator
          // will return records with incremental doc ids.
          IndexSegment currentSegment = currentRecordLocation.getSegment();
          if (immutableSegment == currentSegment) {
            if (recordInfo._timestamp >= currentRecordLocation.getTimestamp()) {
              validDocIds.remove(currentRecordLocation.getDocId());
              validDocIds.add(recordInfo._docId);
              return new RecordLocation(immutableSegment, recordInfo._docId, recordInfo._timestamp);
            } else {
              return currentRecordLocation;
            }
          }

          // The current record is in an old segment being replaced
          // This could happen when committing a consuming segment, or reloading a completed segment. In this case, we
          // want to update the record location when there is a tie because the record locations should point to the new
          // added segment instead of the old segment being replaced. Also, do not update the valid doc ids for the old
          // segment because it has not been replaced yet.
          String currentSegmentName = currentSegment.getSegmentName();
          if (segmentName.equals(currentSegmentName)) {
            if (recordInfo._timestamp >= currentRecordLocation.getTimestamp()) {
              validDocIds.add(recordInfo._docId);
              return new RecordLocation(immutableSegment, recordInfo._docId, recordInfo._timestamp);
            } else {
              return currentRecordLocation;
            }
          }

          // The current record is in a different segment
          // Update the record location when getting a newer timestamp, or the timestamp is the same as the current
          // timestamp, but the segment has a larger sequence number (the segment is newer than the current segment).
          if (recordInfo._timestamp > currentRecordLocation.getTimestamp() || (
              recordInfo._timestamp == currentRecordLocation.getTimestamp() && LLCSegmentName
                  .isLowLevelConsumerSegmentName(segmentName) && LLCSegmentName
                  .isLowLevelConsumerSegmentName(currentSegmentName)
                  && LLCSegmentName.getSequenceNumber(segmentName) > LLCSegmentName
                  .getSequenceNumber(currentSegmentName))) {
            assert currentSegment.getValidDocIds() != null;
            currentSegment.getValidDocIds().remove(currentRecordLocation.getDocId());
            validDocIds.add(recordInfo._docId);
            return new RecordLocation(immutableSegment, recordInfo._docId, recordInfo._timestamp);
          } else {
            return currentRecordLocation;
          }
        } else {
          // New primary key
          validDocIds.add(recordInfo._docId);
          return new RecordLocation(immutableSegment, recordInfo._docId, recordInfo._timestamp);
        }
      });
    }
    // Update metrics
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        _primaryKeyToRecordLocationMap.size());
  }

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment. Returns the merged record if
   * partial-upsert is enabled.
   */
  public GenericRow updateRecord(IndexSegment segment, RecordInfo recordInfo, GenericRow record) {
    // For partial-upsert, need to ensure all previous records are loaded before inserting new records.
    if (_partialUpsertHandler != null) {
      while (!_partialUpsertHandler.isAllSegmentsLoaded()) {
        LOGGER
            .info("Sleeping 1 second waiting for all segments loaded for partial-upsert table: {}", _tableNameWithType);
        try {
          //noinspection BusyWait
          Thread.sleep(1000L);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    _result = record;
    _primaryKeyToRecordLocationMap.compute(recordInfo._primaryKey, (primaryKey, currentRecordLocation) -> {
      if (currentRecordLocation != null) {
        // Existing primary key

        // Update the record location when the new timestamp is greater than or equal to the current timestamp. Update
        // the record location when there is a tie to keep the newer record.
        if (recordInfo._timestamp >= currentRecordLocation.getTimestamp()) {
          IndexSegment currentSegment = currentRecordLocation.getSegment();
          if (_partialUpsertHandler != null) {
            // Partial upsert
            GenericRow previousRecord = currentSegment.getRecord(currentRecordLocation.getDocId(), _reuse);
            _result = _partialUpsertHandler.merge(previousRecord, record);
          }
          assert currentSegment.getValidDocIds() != null;
          currentSegment.getValidDocIds().remove(currentRecordLocation.getDocId());
          assert segment.getValidDocIds() != null;
          segment.getValidDocIds().add(recordInfo._docId);
          return new RecordLocation(segment, recordInfo._docId, recordInfo._timestamp);
        } else {
          if (_partialUpsertHandler != null) {
            LOGGER.warn(
                "Got late event for partial upsert: {} (current timestamp: {}, record timestamp: {}), skipping updating the record",
                record, currentRecordLocation.getTimestamp(), recordInfo._timestamp);
          }
          return currentRecordLocation;
        }
      } else {
        // New primary key
        assert segment.getValidDocIds() != null;
        segment.getValidDocIds().add(recordInfo._docId);
        return new RecordLocation(segment, recordInfo._docId, recordInfo._timestamp);
      }
    });
    // Update metrics
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        _primaryKeyToRecordLocationMap.size());
    return _result;
  }

  /**
   * Removes the upsert metadata for the given immutable segment. No need to remove the upsert metadata for the
   * consuming segment because it should be replaced by the committed segment.
   */
  public void removeSegment(IndexSegment segment) {
    String segmentName = segment.getSegmentName();
    LOGGER.info("Removing upsert metadata for segment: {}", segmentName);

    assert segment.getValidDocIds() != null;
    if (!segment.getValidDocIds().getMutableRoaringBitmap().isEmpty()) {
      // Remove all the record locations that point to the removed segment
      _primaryKeyToRecordLocationMap.forEach((primaryKey, recordLocation) -> {
        if (recordLocation.getSegment() == segment) {
          // Check and remove to prevent removing the key that is just updated
          _primaryKeyToRecordLocationMap.remove(primaryKey, recordLocation);
        }
      });
    }
    // Update metrics
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        _primaryKeyToRecordLocationMap.size());
  }

  public static final class RecordInfo {
    private final PrimaryKey _primaryKey;
    private final int _docId;
    private final long _timestamp;

    public RecordInfo(PrimaryKey primaryKey, int docId, long timestamp) {
      _primaryKey = primaryKey;
      _docId = docId;
      _timestamp = timestamp;
    }
  }
}
