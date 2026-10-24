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
package org.apache.pinot.core.data.manager;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.TableSnapshot;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.core.util.PeerServerSegmentFinder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/// Existing Helix metadata access for table managers. Callers pass the current protected manager/store fields so
/// legacy subclasses keep their original behavior. Segment values retain every legacy field without conversion.
final class HelixTableDataManagerMetadata {
  private HelixTableDataManagerMetadata() {
  }

  static TableSnapshot fetchTable(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType) {
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(propertyStore, tableNameWithType);
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", tableNameWithType);
    Schema schema = ZKMetadataProvider.getTableSchema(propertyStore, tableNameWithType);
    Preconditions.checkState(schema != null, "Failed to find schema for table: %s", tableNameWithType);
    return new TableSnapshot(tableConfig, schema);
  }

  @Nullable
  static SegmentZKMetadata fetchSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, String tableNameWithType,
      String segmentName) {
    return ZKMetadataProvider.getSegmentZKMetadata(propertyStore, tableNameWithType, segmentName);
  }

  static List<URI> peerUris(HelixManager helixManager, String tableNameWithType, String segmentName, String scheme) {
    return PeerServerSegmentFinder.getPeerServerURIs(helixManager, tableNameWithType, segmentName, scheme);
  }
}
