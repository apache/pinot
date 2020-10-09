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

import java.util.HashSet;
import java.util.Map;
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

  private final int _partitionId;

  private final ConcurrentHashMap<PrimaryKey, RecordLocation> _primaryKeyIndex = new ConcurrentHashMap<>();
  // the mapping between the (sealed) segment and its validDocuments
  private final ConcurrentHashMap<String, ThreadSafeMutableRoaringBitmap> _segmentToValidDocIndexMap = new ConcurrentHashMap<>();

  public PartitionUpsertMetadataManager(int partitionId) {
    _partitionId = partitionId;
  }

  public void removeRecordLocation(PrimaryKey primaryKey) {
    _primaryKeyIndex.remove(primaryKey);
  }

  public boolean containsKey(PrimaryKey primaryKey) {
    return _primaryKeyIndex.containsKey(primaryKey);
  }

  public RecordLocation getRecordLocation(PrimaryKey primaryKey) {
    return _primaryKeyIndex.get(primaryKey);
  }

  public ThreadSafeMutableRoaringBitmap getValidDocIndex(String segmentName) {
    return _segmentToValidDocIndexMap.get(segmentName);
  }

  private ThreadSafeMutableRoaringBitmap getOrCreateValidDocIndex(String segmentName) {
    return _segmentToValidDocIndexMap.computeIfAbsent(segmentName, k->new ThreadSafeMutableRoaringBitmap());
  }

  public synchronized void removeUpsertMetadata(String segmentName) {
    _segmentToValidDocIndexMap.remove(segmentName);
    for (Map.Entry<PrimaryKey, RecordLocation> entry : new HashSet<>(_primaryKeyIndex.entrySet())) {
      if (entry.getValue().getSegmentName().equals(segmentName)) {
        _primaryKeyIndex.remove(entry.getKey());
      }
    }
  }

  int getPartitionId() {
    return _partitionId;
  }

  public synchronized void handUpsert(PrimaryKey primaryKey, RecordLocation location, String segmentName) {
    if (containsKey(primaryKey)) {
      RecordLocation prevLocation = getRecordLocation(primaryKey);
      // upsert
      if (location.getTimestamp() >= prevLocation.getTimestamp()) {
        removeRecordLocation(primaryKey);
        _primaryKeyIndex.put(primaryKey, location);
        getValidDocIndex(prevLocation.getSegmentName())
            .remove(prevLocation.getDocId());
        getOrCreateValidDocIndex(segmentName).checkAndAdd(location.getDocId());
        LOGGER.debug(String
            .format("upsert: replace old doc id %d with %d for key: %s, hash: %d", prevLocation.getDocId(),
                location.getDocId(), primaryKey, primaryKey.hashCode()));
      } else {
        LOGGER.debug(
            String.format("upsert: ignore a late-arrived record: %s, hash: %d", primaryKey, primaryKey.hashCode()));
      }
    } else { // append
      _primaryKeyIndex.put(primaryKey, location);
      getOrCreateValidDocIndex(segmentName).checkAndAdd(location.getDocId());
    }
  }
}
