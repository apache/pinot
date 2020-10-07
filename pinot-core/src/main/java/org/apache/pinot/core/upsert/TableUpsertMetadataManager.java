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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.PrimaryKey;

/**
 * The manager of the upsert metadata of a table.
 */
@ThreadSafe
public class TableUpsertMetadataManager {
  private final Map<Integer, PartitionUpsertMetadataManager> _partitionMetadataManagerMap = new ConcurrentHashMap<>();

  public TableUpsertMetadataManager() {
  }

  private synchronized PartitionUpsertMetadataManager getOrCreatePartitionManager(int partitionId) {
    if(!_partitionMetadataManagerMap.containsKey(partitionId)) {
      _partitionMetadataManagerMap.put(partitionId, new PartitionUpsertMetadataManager(partitionId));
    }
    return _partitionMetadataManagerMap.get(partitionId);
  }

  public boolean isEmpty() {
    return _partitionMetadataManagerMap.isEmpty();
  }

  public void removeRecordLocation(int partitionId, PrimaryKey primaryKey) {
    getOrCreatePartitionManager(partitionId).removeRecordLocation(primaryKey);
  }

  public boolean containsKey(int partitionId, PrimaryKey primaryKey) {
    return getOrCreatePartitionManager(partitionId).containsKey(primaryKey);
  }

  public RecordLocation getRecordLocation(int partitionId, PrimaryKey primaryKey) {
    return getOrCreatePartitionManager(partitionId).getRecordLocation(primaryKey);
  }

  public ThreadSafeMutableRoaringBitmap getValidDocIndex(int partitionId, String segmentName) {
    // TODO(upsert) check existence of the validDocIndex of the given segment, rebuild it if not available
    return getOrCreatePartitionManager(partitionId).getValidDocIndex(segmentName);
  }

  public synchronized void putUpsertMetadataOfPartition(int partitionId, String segmentName, Map<PrimaryKey, RecordLocation> primaryKeyIndex,
      ThreadSafeMutableRoaringBitmap validDocIndex) {
    getOrCreatePartitionManager(partitionId).putUpsertMetadata(segmentName, primaryKeyIndex, validDocIndex);
  }

  public void removeUpsertMetadataOfSegment(int partitionId, String segmentName) {
    getOrCreatePartitionManager(partitionId).removeUpsertMetadata(segmentName);
  }
}
