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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.PrimaryKey;


class UpsertMetadataPartitionManager {

  private final int _partitionId;

  private final Map<PrimaryKey, RecordLocation> _primaryKeyIndex = new ConcurrentHashMap();
  // the mapping between the (sealed) segment and its validDocuments
  // TODO(upsert) concurrency protection
  private final Map<String, ThreadSafeMutableRoaringBitmap> _segmentToValidDocIndexMap = new ConcurrentHashMap();

  UpsertMetadataPartitionManager(int partitionId) {
    _partitionId = partitionId;
  }

  void removeRecordLocation(PrimaryKey primaryKey) {
    _primaryKeyIndex.remove(primaryKey);
  }

  boolean containsKey(PrimaryKey primaryKey) {
    return _primaryKeyIndex.containsKey(primaryKey);
  }

  RecordLocation getRecordLocation(PrimaryKey primaryKey) {
    return _primaryKeyIndex.get(primaryKey);
  }

  synchronized void updateRecordLocation(PrimaryKey primaryKey, RecordLocation recordLocation) {
    _primaryKeyIndex.put(primaryKey, recordLocation);
  }

  ThreadSafeMutableRoaringBitmap getValidDocIndex(String segmentName) {
    // TODO(upsert) check existence of the validDocIndex of the given segment, rebuild it if not available
    return _segmentToValidDocIndexMap.get(segmentName);
  }

  void putUpsertMetadata(String segmentName, Map<PrimaryKey, RecordLocation> primaryKeyIndex,
      ThreadSafeMutableRoaringBitmap validDocIndex) {
    //TODO(upsert) do we need to make a backup before update?
    _primaryKeyIndex.putAll(primaryKeyIndex);
    _segmentToValidDocIndexMap.put(segmentName, validDocIndex);
  }

  int getPartitionId() {
    return _partitionId;
  }
}
