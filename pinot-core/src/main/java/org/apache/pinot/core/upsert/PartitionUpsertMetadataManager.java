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
package org.apache.pinot.core.upsert;

import com.google.common.annotations.VisibleForTesting;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
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

  public PartitionUpsertMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
  }

  // TODO(upset): consider an off-heap KV store to persist this index to improve the recovery speed.
  @VisibleForTesting
  final ConcurrentHashMap<PrimaryKey, RecordLocation> _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();

  /**
   * Initializes the upsert metadata for the given immutable segment, returns the valid doc ids for the segment.
   */
  public ThreadSafeMutableRoaringBitmap addSegment(String segmentName, Iterator<RecordInfo> recordInfoIterator) {
    LOGGER.info("Adding upsert metadata for segment: {}", segmentName);

    ThreadSafeMutableRoaringBitmap validDocIds = new ThreadSafeMutableRoaringBitmap();
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      _primaryKeyToRecordLocationMap.compute(recordInfo._primaryKey, (primaryKey, currentRecordLocation) -> {
        if (currentRecordLocation != null) {
          // Existing primary key

          if (segmentName.equals(currentRecordLocation.getSegmentName())) {
            // The current record location has the same segment name

            // Update the record location when the new timestamp is greater than or equal to the current timestamp.
            // There are 2 scenarios:
            //   1. The current record location is pointing to the same segment (the segment being added). In this case,
            //      we want to update the record location when there is a tie to keep the newer record. Note that the
            //      record info iterator will return records with incremental doc ids.
            //   2. The current record location is pointing to the old segment being replaced. This could happen when
            //      committing a consuming segment, or reloading a completed segment. In this case, we want to update
            //      the record location when there is a tie because the record locations should point to the new added
            //      segment instead of the old segment being replaced. Also, do not update the valid doc ids for the old
            //      segment because it has not been replaced yet.
            if (recordInfo._timestamp >= currentRecordLocation.getTimestamp()) {
              // Only update the valid doc ids for the new segment
              if (validDocIds == currentRecordLocation.getValidDocIds()) {
                validDocIds.remove(currentRecordLocation.getDocId());
              }
              validDocIds.add(recordInfo._docId);
              return new RecordLocation(segmentName, recordInfo._docId, recordInfo._timestamp, validDocIds);
            } else {
              return currentRecordLocation;
            }
          } else {
            // The current record location is pointing to a different segment

            // Update the record location when getting a newer timestamp, or the timestamp is the same as the current
            // timestamp, but the segment has a larger sequence number (the segment is newer than the current segment).
            if (recordInfo._timestamp > currentRecordLocation.getTimestamp() || (
                recordInfo._timestamp == currentRecordLocation.getTimestamp()
                    && LLCSegmentName.getSequenceNumber(segmentName) > LLCSegmentName
                    .getSequenceNumber(currentRecordLocation.getSegmentName()))) {
              currentRecordLocation.getValidDocIds().remove(currentRecordLocation.getDocId());
              validDocIds.add(recordInfo._docId);
              return new RecordLocation(segmentName, recordInfo._docId, recordInfo._timestamp, validDocIds);
            } else {
              return currentRecordLocation;
            }
          }
        } else {
          // New primary key
          validDocIds.add(recordInfo._docId);
          return new RecordLocation(segmentName, recordInfo._docId, recordInfo._timestamp, validDocIds);
        }
      });
    }
    // Update metrics
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        _primaryKeyToRecordLocationMap.size());
    return validDocIds;
  }

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
  public void updateRecord(String segmentName, RecordInfo recordInfo, ThreadSafeMutableRoaringBitmap validDocIds) {
    _primaryKeyToRecordLocationMap.compute(recordInfo._primaryKey, (primaryKey, currentRecordLocation) -> {
      if (currentRecordLocation != null) {
        // Existing primary key

        // Update the record location when the new timestamp is greater than or equal to the current timestamp. Update
        // the record location when there is a tie to keep the newer record.
        if (recordInfo._timestamp >= currentRecordLocation.getTimestamp()) {
          currentRecordLocation.getValidDocIds().remove(currentRecordLocation.getDocId());
          validDocIds.add(recordInfo._docId);
          return new RecordLocation(segmentName, recordInfo._docId, recordInfo._timestamp, validDocIds);
        } else {
          return currentRecordLocation;
        }
      } else {
        // New primary key
        validDocIds.add(recordInfo._docId);
        return new RecordLocation(segmentName, recordInfo._docId, recordInfo._timestamp, validDocIds);
      }
    });
    // Update metrics
    _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
        _primaryKeyToRecordLocationMap.size());
  }

  /**
   * Removes the upsert metadata for the given immutable segment. No need to remove the upsert metadata for the
   * consuming segment because it should be replaced by the committed segment.
   */
  public void removeSegment(String segmentName, ThreadSafeMutableRoaringBitmap validDocIds) {
    LOGGER.info("Removing upsert metadata for segment: {}", segmentName);

    if (!validDocIds.getMutableRoaringBitmap().isEmpty()) {
      // Remove all the record locations that point to the valid doc ids of the removed segment.
      _primaryKeyToRecordLocationMap.forEach((primaryKey, recordLocation) -> {
        if (recordLocation.getValidDocIds() == validDocIds) {
          // Check and remove to prevent removing the key that is just updated.
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
