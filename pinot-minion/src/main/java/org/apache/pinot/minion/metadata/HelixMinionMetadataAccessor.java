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
package org.apache.pinot.minion.metadata;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/// Preserves the existing Helix table, schema and segment lookup semantics for concurrent minion readers.
public final class HelixMinionMetadataAccessor implements MinionMetadataAccessor {
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public HelixMinionMetadataAccessor(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = Objects.requireNonNull(propertyStore);
  }

  @Override
  @Nullable
  public TableConfig getTableConfig(String tableNameWithType) {
    return ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
  }

  @Override
  @Nullable
  public Schema getTableSchema(String tableName) {
    return ZKMetadataProvider.getTableSchema(_propertyStore, tableName);
  }

  @Override
  @Nullable
  public SegmentZKMetadata getSegmentMetadata(String tableNameWithType, String segmentName) {
    return ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, tableNameWithType, segmentName);
  }
}
