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

import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;


/**
 * Class to help to read, write segment lineage metadata
 */
public class SegmentLineageAccessHelper {
  private SegmentLineageAccessHelper() {
  }

  /**
   * Read the segment lineage ZNRecord from the property store
   *
   * @param propertyStore a property store
   * @param tableNameWithType a table name with type
   * @return a ZNRecord of segment merge lineage, return null if znode does not exist
   */
  public static ZNRecord getSegmentLineageZNRecord(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForSegmentLineage(tableNameWithType);
    Stat stat = new Stat();
    ZNRecord segmentLineageZNRecord = propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (segmentLineageZNRecord != null) {
      segmentLineageZNRecord.setVersion(stat.getVersion());
    }
    return segmentLineageZNRecord;
  }

  /**
   * Read the segment lineage from the property store
   *
   * @param propertyStore  a property store
   * @param tableNameWithType a table name with type
   * @return a segment lineage, return null if znode does not exist
   */
  public static SegmentLineage getSegmentLineage(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType) {
    ZNRecord znRecord = getSegmentLineageZNRecord(propertyStore, tableNameWithType);
    SegmentLineage segmentMergeLineage = null;
    if (znRecord != null) {
      segmentMergeLineage = SegmentLineage.fromZNRecord(znRecord);
    }
    return segmentMergeLineage;
  }

  /**
   * Write the segment lineage to the property store
   *
   * @param propertyStore a property store
   * @param segmentLineage a segment lineage
   * @param expectedVersion expected version of ZNRecord. -1 for indicating to match any version.
   * @return true if update is successful. false otherwise.
   */
  public static boolean writeSegmentLineage(ZkHelixPropertyStore<ZNRecord> propertyStore, SegmentLineage segmentLineage,
      int expectedVersion) {
    String tableNameWithType = segmentLineage.getTableNameWithType();
    String path = ZKMetadataProvider.constructPropertyStorePathForSegmentLineage(tableNameWithType);
    try {
      return propertyStore.set(path, segmentLineage.toZNRecord(), expectedVersion, AccessOption.PERSISTENT);
    } catch (ZkBadVersionException e) {
      return false;
    }
  }

  /**
   * Delete the segment lineage from the property store
   *
   * @param propertyStore a property store
   * @param tableNameWithType a table name with type
   * @return true if delete is successful. false otherwise.
   */
  public static boolean deleteSegmentLineage(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType) {
    String path = ZKMetadataProvider.constructPropertyStorePathForSegmentLineage(tableNameWithType);
    return propertyStore.remove(path, AccessOption.PERSISTENT);
  }
}
