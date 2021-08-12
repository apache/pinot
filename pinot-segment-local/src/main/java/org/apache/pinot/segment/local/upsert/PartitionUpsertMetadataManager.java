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
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the upsert metadata per partition.
 * <p>For multiple records with the same comparison value (default to timestamp), the manager will preserve the latest
 * record based on the sequence number of the segment. If 2 records with the same comparison value are in the same
 * segment, the one with larger doc id will be preserved. Note that for tables with sorted column, the records will be
 * re-ordered when committing the segment, and we will use the re-ordered doc ids instead of the ingestion order to
 *  decide which record to preserve.
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
@ThreadSafe
public class PartitionUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertMetadataManager.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final UpsertConfig.HashFunction _hashFunction;

  // TODO(upsert): consider an off-heap KV store to persist this mapping to improve the recovery speed.
  @VisibleForTesting
  final ConcurrentHashMap<Object, RecordLocation> _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();

  // Reused for reading previous record during partial upsert
  private final GenericRow _reuse = new GenericRow();
  // Stores the result of updateRecord()
  private GenericRow _result;

  public PartitionUpsertMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, UpsertConfig.HashFunction hashFunction) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;
  }

  /**
   * Initializes the upsert metadata for the given immutable segment.
   */
  public void addSegment(IndexSegment segment, Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    LOGGER.info("Adding upsert metadata for segment: {}", segmentName);
    ThreadSafeMutableRoaringBitmap validDocIds = segment.getValidDocIds();
    assert validDocIds != null;

    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      _primaryKeyToRecordLocationMap
          .compute(hashPrimaryKey(recordInfo._primaryKey, _hashFunction), (primaryKey, currentRecordLocation) -> {
        if (currentRecordLocation != null) {
          // Existing primary key

          // The current record is in the same segment
          // Update the record location when there is a tie to keep the newer record. Note that the record info iterator
          // will return records with incremental doc ids.
          IndexSegment currentSegment = currentRecordLocation.getSegment();
          if (segment == currentSegment) {
            if (recordInfo._comparisonValue.compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
              validDocIds.remove(currentRecordLocation.getDocId());
              validDocIds.add(recordInfo._docId);
              return new RecordLocation(segment, recordInfo._docId, recordInfo._comparisonValue);
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
            if (recordInfo._comparisonValue.compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
              validDocIds.add(recordInfo._docId);
              return new RecordLocation(segment, recordInfo._docId, recordInfo._comparisonValue);
            } else {
              return currentRecordLocation;
            }
          }

          // The current record is in a different segment
          // Update the record location when getting a newer comparison value, or the value is the same as the current
          // value, but the segment has a larger sequence number (the segment is newer than the current segment).
          if (recordInfo._comparisonValue.compareTo(currentRecordLocation.getComparisonValue()) > 0 || (
              recordInfo._comparisonValue == currentRecordLocation.getComparisonValue() && LLCSegmentName
                  .isLowLevelConsumerSegmentName(segmentName) && LLCSegmentName
                  .isLowLevelConsumerSegmentName(currentSegmentName)
                  && LLCSegmentName.getSequenceNumber(segmentName) > LLCSegmentName
                  .getSequenceNumber(currentSegmentName))) {
            assert currentSegment.getValidDocIds() != null;
            currentSegment.getValidDocIds().remove(currentRecordLocation.getDocId());
            validDocIds.add(recordInfo._docId);
            return new RecordLocation(segment, recordInfo._docId, recordInfo._comparisonValue);
          } else {
            return currentRecordLocation;
          }
        } else {
          // New primary key
          validDocIds.add(recordInfo._docId);
          return new RecordLocation(segment, recordInfo._docId, recordInfo._comparisonValue);
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
    _primaryKeyToRecordLocationMap
        .compute(hashPrimaryKey(recordInfo._primaryKey, _hashFunction), (primaryKey, currentRecordLocation) -> {
      if (currentRecordLocation != null) {
        // Existing primary key

        // Update the record location when the new comparison value is greater than or equal to the current value. Update
        // the record location when there is a tie to keep the newer record.
        if (recordInfo._comparisonValue.compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
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
          return new RecordLocation(segment, recordInfo._docId, recordInfo._comparisonValue);
        } else {
          if (_partialUpsertHandler != null) {
            LOGGER.warn(
                "Got late event for partial upsert: {} (current comparison value: {}, record comparison value: {}), skipping updating the record",
                record, currentRecordLocation.getComparisonValue(), recordInfo._comparisonValue);
          }
          return currentRecordLocation;
        }
      } else {
        // New primary key
        assert segment.getValidDocIds() != null;
        segment.getValidDocIds().add(recordInfo._docId);
        return new RecordLocation(segment, recordInfo._docId, recordInfo._comparisonValue);
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

  protected static Object hashPrimaryKey(PrimaryKey primaryKey, UpsertConfig.HashFunction hashFunction) {
    switch (hashFunction) {
      case NONE:
        return primaryKey;
      case MD5:
        return new ByteArray(HashUtils.hashMD5(primaryKey.asBytes()));
      case MURMUR3:
        return new ByteArray(HashUtils.hashMurmur3(primaryKey.asBytes()));
      default:
        throw new IllegalArgumentException(String.format("Unrecognized hash function %s", hashFunction));
    }
  }

  public static final class RecordInfo {
    private final PrimaryKey _primaryKey;
    private final int _docId;
    private final Comparable _comparisonValue;

    public RecordInfo(PrimaryKey primaryKey, int docId, Comparable comparisonValue) {
      _primaryKey = primaryKey;
      _docId = docId;
      _comparisonValue = comparisonValue;
    }
  }
}
