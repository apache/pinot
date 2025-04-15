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
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server.Dedup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseTableDedupMetadataManager implements TableDedupMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTableDedupMetadataManager.class);

  protected final Map<Integer, PartitionDedupMetadataManager> _partitionMetadataManagerMap = new ConcurrentHashMap<>();
  protected String _tableNameWithType;
  protected DedupContext _context;

  @Override
  public void init(PinotConfiguration instanceDedupConfig, TableConfig tableConfig, Schema schema,
      TableDataManager tableDataManager) {
    _tableNameWithType = tableConfig.getTableName();

    Preconditions.checkArgument(tableConfig.isDedupEnabled(), "Dedup must be enabled for table: %s",
        _tableNameWithType);
    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    assert dedupConfig != null;

    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(primaryKeyColumns),
        "Primary key columns must be configured for dedup enabled table: %s", _tableNameWithType);

    double metadataTTL = dedupConfig.getMetadataTTL();
    String dedupTimeColumn = dedupConfig.getDedupTimeColumn();
    if (dedupTimeColumn == null) {
      dedupTimeColumn = tableConfig.getValidationConfig().getTimeColumnName();
    }
    if (metadataTTL > 0) {
      Preconditions.checkArgument(dedupTimeColumn != null,
          "When metadataTTL is configured, metadata time column or time column must be configured for dedup enabled "
              + "table: %s", _tableNameWithType);
    }

    boolean enablePreload =
        dedupConfig.getPreload().isEnabled(() -> instanceDedupConfig.getProperty(Dedup.DEFAULT_ENABLE_PRELOAD, false));
    if (enablePreload) {
      if (tableDataManager.getSegmentPreloadExecutor() == null) {
        LOGGER.warn("Preload cannot be enabled without segment preload executor for table: {}", _tableNameWithType);
        enablePreload = false;
      }
    }

    // NOTE: This field doesn't follow enablement override, and always enabled if enabled at instance level.
    boolean allowDedupConsumptionDuringCommit = dedupConfig.isAllowDedupConsumptionDuringCommit();
    if (!allowDedupConsumptionDuringCommit) {
      allowDedupConsumptionDuringCommit =
          instanceDedupConfig.getProperty(Dedup.DEFAULT_ALLOW_DEDUP_CONSUMPTION_DURING_COMMIT, false);
    }

    _context = new DedupContext.Builder()
        .setTableConfig(tableConfig)
        .setSchema(schema)
        .setTableDataManager(tableDataManager)
        .setPrimaryKeyColumns(primaryKeyColumns)
        .setHashFunction(dedupConfig.getHashFunction())
        .setMetadataTTL(metadataTTL)
        .setDedupTimeColumn(dedupTimeColumn)
        .setEnablePreload(enablePreload)
        .setMetadataManagerConfigs(dedupConfig.getMetadataManagerConfigs())
        .setAllowDedupConsumptionDuringCommit(allowDedupConsumptionDuringCommit)
        .build();
    LOGGER.info("Initialized {} for table: {} with: {}", getClass().getSimpleName(), _tableNameWithType, _context);

    initCustomVariables();
  }

  /**
   * Can be overridden to initialize custom variables after other variables are set
   */
  protected void initCustomVariables() {
  }

  @Override
  public PartitionDedupMetadataManager getOrCreatePartitionManager(int partitionId) {
    return _partitionMetadataManagerMap.computeIfAbsent(partitionId, this::createPartitionDedupMetadataManager);
  }

  /**
   * Create PartitionDedupMetadataManager for given partition id.
   */
  protected abstract PartitionDedupMetadataManager createPartitionDedupMetadataManager(Integer partitionId);

  @Override
  public DedupContext getContext() {
    return _context;
  }

  @Deprecated
  @Override
  public boolean isEnablePreload() {
    return _context.isPreloadEnabled();
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
