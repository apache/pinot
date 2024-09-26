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
package org.apache.pinot.segment.local.dedup;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public abstract class BaseTableDedupMetadataManager implements TableDedupMetadataManager {
  protected final Map<Integer, PartitionDedupMetadataManager> _partitionMetadataManagerMap = new ConcurrentHashMap<>();
  protected String _tableNameWithType;
  protected DedupContext _dedupContext;

  @Override
  public void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager,
      ServerMetrics serverMetrics) {
    _tableNameWithType = tableConfig.getTableName();

    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(primaryKeyColumns),
        "Primary key columns must be configured for dedup enabled table: %s", _tableNameWithType);

    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    Preconditions.checkArgument(dedupConfig != null, "Dedup must be enabled for table: %s", _tableNameWithType);
    double metadataTTL = dedupConfig.getMetadataTTL();
    String dedupTimeColumn = dedupConfig.getDedupTimeColumn();
    if (dedupTimeColumn == null) {
      dedupTimeColumn = tableConfig.getValidationConfig().getTimeColumnName();
    }
    if (metadataTTL > 0) {
      Preconditions.checkArgument(dedupTimeColumn != null,
          "When metadataTTL is configured, metadata time column or time column must be configured for "
              + "dedup enabled table: %s", _tableNameWithType);
    }

    DedupContext.Builder dedupContextBuider = new DedupContext.Builder();
    dedupContextBuider
        .setTableConfig(tableConfig)
        .setSchema(schema)
        .setPrimaryKeyColumns(primaryKeyColumns)
        .setHashFunction(dedupConfig.getHashFunction())
        .setMetadataTTL(metadataTTL)
        .setDedupTimeColumn(dedupTimeColumn)
        .setTableIndexDir(tableDataManager.getTableDataDir())
        .setTableDataManager(tableDataManager)
        .setServerMetrics(serverMetrics);
    _dedupContext = dedupContextBuider.build();

    initCustomVariables();
  }

  public PartitionDedupMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap.computeIfAbsent(partitionId, this::createPartitionDedupMetadataManager);
  }

  /**
   * Create PartitionDedupMetadataManager for given partition id.
   */
  abstract protected PartitionDedupMetadataManager createPartitionDedupMetadataManager(Integer partitionId);

  /**
   * Can be overridden to initialize custom variables after other variables are set
   */
  protected void initCustomVariables() {
  }

  @Override
  public void stop() {
    for (PartitionDedupMetadataManager metadataManager : _partitionMetadataManagerMap.values()) {
      metadataManager.stop();
    }
  }

  @Override
  public void close()
      throws IOException {
    for (PartitionDedupMetadataManager metadataManager : _partitionMetadataManagerMap.values()) {
      metadataManager.close();
    }
  }
}
