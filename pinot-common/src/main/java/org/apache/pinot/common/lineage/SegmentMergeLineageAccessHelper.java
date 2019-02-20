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
package org.apache.pinot.common.lineage;

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;


/**
 * Class to help to read, write segment merge lineage
 */
public class SegmentMergeLineageAccessHelper {

  /**
   * Read the segment merge lineage ZNRecord from the property store
   *
   * @param propertyStore a property store
   * @param tableNameWithType a table name with type
   * @return a ZNRecord of segment merge lineage, return null if znode does not exist
   */
  public static ZNRecord getSegmentMergeLineageZNRecord(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String tableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForSegmentMergeLineage(tableNameWithType);
    Stat stat = new Stat();
    ZNRecord segmentMergeLineageZNRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (segmentMergeLineageZNRecord != null) {
      segmentMergeLineageZNRecord.setVersion(stat.getVersion());
    }
    return segmentMergeLineageZNRecord;
  }

  /**
   * Read the segment merge lineage from the property store
   *
   * @param propertyStore  a property store
   * @param tableNameWithType a table name with type
   * @return a segment merge lineage, return null if znode does not exist
   */
  public static SegmentMergeLineage getSegmentMergeLineage(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String tableNameWithType) {
    ZNRecord znRecord = getSegmentMergeLineageZNRecord(propertyStore, tableNameWithType);
    SegmentMergeLineage segmentMergeLineage = null;
    if (znRecord != null) {
      segmentMergeLineage = SegmentMergeLineage.fromZNRecord(znRecord);
    }
    return segmentMergeLineage;
  }

  /**
   * Write the segment merge lineage to the property store
   *
   * @param propertyStore a property store
   * @param segmentMergeLineage a segment merge lineage
   * @return true if update is successful. false otherwise.
   */
  public static boolean writeSegmentMergeLineage(ZkHelixPropertyStore<ZNRecord> propertyStore,
      SegmentMergeLineage segmentMergeLineage, int expectedVersion) {
    String tableNameWithType = segmentMergeLineage.getTableName();
    String path = ZKMetadataProvider.constructPropertyStorePathForSegmentMergeLineage(tableNameWithType);
    return propertyStore.set(path, segmentMergeLineage.toZNRecord(), expectedVersion, AccessOption.PERSISTENT);
  }
}
