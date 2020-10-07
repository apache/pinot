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
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class UpsertProcessorUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertProcessorUtil.class);

  private UpsertProcessorUtil() {
  }

  public static void handleUpsert(PrimaryKey primaryKey, long timestamp, String segmentName, int docId, int partitionId,
      Map<PrimaryKey, RecordLocation> primaryKeyIndex, ThreadSafeMutableRoaringBitmap validDocIndex,
      TableUpsertMetadataManager upsertMetadataTableManager) {
    RecordLocation location = new RecordLocation(segmentName, docId, timestamp);
    // check local primary key index first
    if (primaryKeyIndex.containsKey(primaryKey)) {
      RecordLocation prevLocation = primaryKeyIndex.get(primaryKey);
      if (location.getTimestamp() >= prevLocation.getTimestamp()) {
        primaryKeyIndex.put(primaryKey, location);
        // update validDocIndex
        validDocIndex.remove(prevLocation.getDocId());
        validDocIndex.checkAndAdd(location.getDocId());
        LOGGER.debug(String
            .format("upsert: replace old doc id %d with %d for key: %s, hash: %d", prevLocation.getDocId(),
                location.getDocId(), primaryKey, primaryKey.hashCode()));
      } else {
        LOGGER.debug(
            String.format("upsert: ignore a late-arrived record: %s, hash: %d", primaryKey, primaryKey.hashCode()));
      }
    } else if (upsertMetadataTableManager.containsKey(partitionId, primaryKey)) {
      RecordLocation prevLocation = upsertMetadataTableManager.getRecordLocation(partitionId, primaryKey);
      if (location.getTimestamp() >= prevLocation.getTimestamp()) {
        upsertMetadataTableManager.removeRecordLocation(partitionId, primaryKey);
        primaryKeyIndex.put(primaryKey, location);

        // update validDocIndex
        upsertMetadataTableManager.getValidDocIndex(partitionId, prevLocation.getSegmentName())
            .remove(prevLocation.getDocId());
        validDocIndex.checkAndAdd(location.getDocId());
      }
    } else {
      primaryKeyIndex.put(primaryKey, location);
      validDocIndex.checkAndAdd(location.getDocId());
    }
  }
}
