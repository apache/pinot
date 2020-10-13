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

import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the upsert metadata per partition.
 */
@ThreadSafe
public class PartitionUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertMetadataManager.class);

  // TODO(upset): consider an off-heap KV store to persist this index to improve the recovery speed.
  private final ConcurrentHashMap<PrimaryKey, RecordLocation> _primaryKeyToRecordLocationMap =
      new ConcurrentHashMap<>();
  // the mapping between the (sealed) segment and its validDocuments
  private final ConcurrentHashMap<String, ThreadSafeMutableRoaringBitmap> _segmentToValidDocIdsMap =
      new ConcurrentHashMap<>();

  /**
   * Creates the valid doc ids for the given (immutable) segment.
   */
  public ThreadSafeMutableRoaringBitmap createValidDocIds(String segmentName) {
    LOGGER.info("Creating valid doc ids for segment: {}", segmentName);
    ThreadSafeMutableRoaringBitmap validDocIds = new ThreadSafeMutableRoaringBitmap();
    if (_segmentToValidDocIdsMap.put(segmentName, validDocIds) != null) {
      LOGGER.warn("Valid doc ids exist for segment: {}, replacing it", segmentName);
    }
    return validDocIds;
  }

  /**
   * Returns the valid doc ids for the given (immutable) segment.
   */
  public ThreadSafeMutableRoaringBitmap getValidDocIds(String segmentName) {
    return _segmentToValidDocIdsMap.computeIfAbsent(segmentName, k -> new ThreadSafeMutableRoaringBitmap());
  }

  /**
   * Updates the record location of the given primary key if the given record location is newer than the current record
   * location. Also updates the valid doc ids accordingly if the record location is updated.
   */
  public void updateRecordLocation(PrimaryKey primaryKey, RecordLocation recordLocation,
      ThreadSafeMutableRoaringBitmap validDocIds) {
    _primaryKeyToRecordLocationMap.compute(primaryKey, (k, v) -> {
      if (v != null) {
        // Existing primary key

        if (recordLocation.getTimestamp() >= v.getTimestamp()) {
          // Update the record location
          // NOTE: Update the record location when there is a tie on the timestamp because during the segment
          //       commitment, when loading the committed segment, it should replace the old record locations in case
          //       the order of records changed.

          // Remove the doc from the valid doc ids of the previous location
          if (v.isConsuming()) {
            // Previous location is a consuming segment, whose valid doc ids are maintained locally. Only update the
            // valid doc ids when the update is from the same segment.
            if (recordLocation.isConsuming() && recordLocation.getSegmentName().equals(v.getSegmentName())) {
              validDocIds.remove(v.getDocId());
            }
          } else {
            ThreadSafeMutableRoaringBitmap validDocIdsForPreviousLocation =
                _segmentToValidDocIdsMap.get(v.getSegmentName());
            if (validDocIdsForPreviousLocation != null) {
              validDocIdsForPreviousLocation.remove(v.getDocId());
            } else {
              LOGGER.warn("Failed to find valid doc ids for previous location: {}", v.getSegmentName());
            }
          }

          validDocIds.checkAndAdd(recordLocation.getDocId());
          return recordLocation;
        } else {
          // No need to update
          return v;
        }
      } else {
        // New primary key
        validDocIds.checkAndAdd(recordLocation.getDocId());
        return recordLocation;
      }
    });
  }

  /**
   * Removes the upsert metadata for the given segment.
   */
  public void removeSegment(String segmentName) {
    LOGGER.info("Removing upsert metadata for segment: {}", segmentName);
    _primaryKeyToRecordLocationMap.forEach((k, v) -> {
      if (v.getSegmentName().equals(segmentName)) {
        // NOTE: Check and remove to prevent removing the key that is just updated.
        _primaryKeyToRecordLocationMap.remove(k, v);
      }
    });
    _segmentToValidDocIdsMap.remove(segmentName);
  }
}
