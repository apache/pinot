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
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the upsert metadata per partition.
 * <p>For multiple records with the same timestamp, there is no guarantee on which record to be preserved.
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
 *     When replacing an existing segment, the updates applied to the new segment won't be reflected to the replaced
 *     segment.
 *   </li>
 * </ul>
 */
@ThreadSafe
public class PartitionUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertMetadataManager.class);

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

            if (validDocIds == currentRecordLocation.getValidDocIds()) {
              // The current record location is pointing to the new segment being loaded

              // Update the record location when getting a newer timestamp
              if (recordInfo._timestamp > currentRecordLocation.getTimestamp()) {
                validDocIds.remove(currentRecordLocation.getDocId());
                validDocIds.add(recordInfo._docId);
                return new RecordLocation(segmentName, recordInfo._docId, recordInfo._timestamp, validDocIds);
              }
            } else {
              // The current record location is pointing to the old segment being replaced. This could happen when
              // committing a consuming segment, or reloading a completed segment.

              // Update the record location when the new timestamp is greater than or equal to the current timestamp.
              // Update the record location when there is a tie because the record locations should point to the new
              // segment instead of the old segment being replaced. Also, do not update the valid doc ids for the old
              // segment because it has not been replaced yet.
              if (recordInfo._timestamp >= currentRecordLocation.getTimestamp()) {
                validDocIds.add(recordInfo._docId);
                return new RecordLocation(segmentName, recordInfo._docId, recordInfo._timestamp, validDocIds);
              }
            }
            return currentRecordLocation;
          }

          // Update the record location when getting a newer timestamp
          if (recordInfo._timestamp > currentRecordLocation.getTimestamp()) {
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
    }
    return validDocIds;
  }

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
  public synchronized void updateRecord(String segmentName, RecordInfo recordInfo,
      ThreadSafeMutableRoaringBitmap validDocIds) {
    _primaryKeyToRecordLocationMap.compute(recordInfo._primaryKey, (primaryKey, currentRecordLocation) -> {
      if (currentRecordLocation != null) {
        // Existing primary key

        // Update the record location when getting a newer timestamp
        if (recordInfo._timestamp > currentRecordLocation.getTimestamp()) {
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
  }

  /**
   * Removes the upsert metadata for the given immutable segment. No need to remove the upsert metadata for the
   * consuming segment because it should be replaced by the committed segment.
   */
  public synchronized void removeSegment(String segmentName, ThreadSafeMutableRoaringBitmap validDocIds) {
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
