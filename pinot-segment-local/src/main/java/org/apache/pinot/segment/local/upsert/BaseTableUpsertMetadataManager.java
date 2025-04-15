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
package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseTableUpsertMetadataManager implements TableUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTableUpsertMetadataManager.class);

  protected String _tableNameWithType;
  protected UpsertContext _context;

  @Override
  public void init(PinotConfiguration instanceUpsertConfig, TableConfig tableConfig, Schema schema,
      TableDataManager tableDataManager) {
    _tableNameWithType = tableConfig.getTableName();

    Preconditions.checkArgument(tableConfig.isUpsertEnabled(),
        "Upsert must be enabled for table: %s", _tableNameWithType);
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    assert upsertConfig != null;

    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(primaryKeyColumns),
        "Primary key columns must be configured for upsert enabled table: %s", _tableNameWithType);

    List<String> comparisonColumns = upsertConfig.getComparisonColumns();
    if (comparisonColumns == null) {
      comparisonColumns = List.of(tableConfig.getValidationConfig().getTimeColumnName());
    }

    PartialUpsertHandler partialUpsertHandler = null;
    if (upsertConfig.getMode() == UpsertConfig.Mode.PARTIAL) {
      partialUpsertHandler = new PartialUpsertHandler(schema, comparisonColumns, upsertConfig);
    }

    boolean enableSnapshot = upsertConfig.getSnapshot()
        .isEnabled(() -> instanceUpsertConfig.getProperty(Upsert.DEFAULT_ENABLE_SNAPSHOT, false));
    boolean enablePreload = upsertConfig.getPreload()
        .isEnabled(() -> instanceUpsertConfig.getProperty(Upsert.DEFAULT_ENABLE_PRELOAD, false));
    if (enablePreload) {
      if (!enableSnapshot) {
        LOGGER.warn("Preload cannot be enabled without snapshot for table: {}", _tableNameWithType);
        enablePreload = false;
      }
      if (tableDataManager.getSegmentPreloadExecutor() == null) {
        LOGGER.warn("Preload cannot be enabled without segment preload executor for table: {}", _tableNameWithType);
        enablePreload = false;
      }
    }

    double metadataTTL = upsertConfig.getMetadataTTL();
    double deletedKeysTTL = upsertConfig.getDeletedKeysTTL();
    boolean enableDeletedKeysCompactionConsistency = upsertConfig.isEnableDeletedKeysCompactionConsistency();
    if (enableDeletedKeysCompactionConsistency) {
      if (!enableSnapshot) {
        LOGGER.warn("Deleted keys compaction consistency cannot be enabled without snapshot for table: {}",
            _tableNameWithType);
        enableDeletedKeysCompactionConsistency = false;
      }
      if (enablePreload) {
        LOGGER.warn("Deleted keys compaction consistency cannot be enabled with preload for table: {}",
            _tableNameWithType);
        enableDeletedKeysCompactionConsistency = false;
      }
      if (metadataTTL > 0) {
        LOGGER.warn("Deleted keys compaction consistency cannot be enabled with metadata TTL for table: {}",
            _tableNameWithType);
        enableDeletedKeysCompactionConsistency = false;
      }
      if (deletedKeysTTL <= 0) {
        LOGGER.warn("Deleted keys compaction consistency cannot be enabled without deleted keys TTL for table: {}",
            _tableNameWithType);
        enableDeletedKeysCompactionConsistency = false;
      }
    }

    // NOTE: This field doesn't follow enablement override, and always take instance config if set to true.
    boolean allowPartialUpsertConsumptionDuringCommit = upsertConfig.isAllowPartialUpsertConsumptionDuringCommit();
    if (!allowPartialUpsertConsumptionDuringCommit) {
      allowPartialUpsertConsumptionDuringCommit =
          instanceUpsertConfig.getProperty(Upsert.DEFAULT_ALLOW_PARTIAL_UPSERT_CONSUMPTION_DURING_COMMIT, false);
    }

    _context = new UpsertContext.Builder()
        .setTableConfig(tableConfig)
        .setSchema(schema)
        .setTableDataManager(tableDataManager)
        .setPrimaryKeyColumns(primaryKeyColumns)
        .setHashFunction(upsertConfig.getHashFunction())
        .setComparisonColumns(comparisonColumns)
        .setPartialUpsertHandler(partialUpsertHandler)
        .setDeleteRecordColumn(upsertConfig.getDeleteRecordColumn())
        .setDropOutOfOrderRecord(upsertConfig.isDropOutOfOrderRecord())
        .setOutOfOrderRecordColumn(upsertConfig.getOutOfOrderRecordColumn())
        .setEnableSnapshot(enableSnapshot)
        .setEnablePreload(enablePreload)
        .setMetadataTTL(metadataTTL)
        .setDeletedKeysTTL(deletedKeysTTL)
        .setEnableDeletedKeysCompactionConsistency(enableDeletedKeysCompactionConsistency)
        .setConsistencyMode(upsertConfig.getConsistencyMode())
        .setUpsertViewRefreshIntervalMs(upsertConfig.getUpsertViewRefreshIntervalMs())
        .setNewSegmentTrackingTimeMs(upsertConfig.getNewSegmentTrackingTimeMs())
        .setMetadataManagerConfigs(upsertConfig.getMetadataManagerConfigs())
        .setAllowPartialUpsertConsumptionDuringCommit(allowPartialUpsertConsumptionDuringCommit)
        .build();
    LOGGER.info("Initialized {} for table: {} with: {}", getClass().getSimpleName(), _tableNameWithType, _context);

    initCustomVariables();
  }

  /**
   * Can be overridden to initialize custom variables after other variables are set but before preload starts. This is
   * needed because preload will load segments which might require these custom variables.
   */
  protected void initCustomVariables() {
  }

  @Override
  public UpsertContext getContext() {
    return _context;
  }

  @Deprecated
  @Override
  public UpsertConfig.Mode getUpsertMode() {
    return _context.getUpsertMode();
  }

  @Deprecated
  @Override
  public UpsertConfig.ConsistencyMode getUpsertConsistencyMode() {
    return _context.getConsistencyMode();
  }

  @Deprecated
  @Override
  public boolean isEnablePreload() {
    return _context.isPreloadEnabled();
  }
}
