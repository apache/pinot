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
import java.io.File;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ThreadSafe
public abstract class BaseTableUpsertMetadataManager implements TableUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTableUpsertMetadataManager.class);

  protected String _tableNameWithType;
  protected UpsertContext _context;
  protected UpsertConfig.ConsistencyMode _consistencyMode;
  protected boolean _enablePreload;
  protected boolean _enableDeletedKeysCompactionConsistency;

  @Override
  public void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager) {
    _tableNameWithType = tableConfig.getTableName();

    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    Preconditions.checkArgument(upsertConfig != null && upsertConfig.getMode() != UpsertConfig.Mode.NONE,
        "Upsert must be enabled for table: %s", _tableNameWithType);

    List<String> primaryKeyColumns = schema.getPrimaryKeyColumns();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(primaryKeyColumns),
        "Primary key columns must be configured for upsert enabled table: %s", _tableNameWithType);

    List<String> comparisonColumns = upsertConfig.getComparisonColumns();
    if (comparisonColumns == null) {
      comparisonColumns = Collections.singletonList(tableConfig.getValidationConfig().getTimeColumnName());
    }

    PartialUpsertHandler partialUpsertHandler = null;
    if (upsertConfig.getMode() == UpsertConfig.Mode.PARTIAL) {
      partialUpsertHandler = new PartialUpsertHandler(schema, comparisonColumns, upsertConfig);
    }

    String deleteRecordColumn = upsertConfig.getDeleteRecordColumn();
    HashFunction hashFunction = upsertConfig.getHashFunction();
    boolean enableSnapshot = upsertConfig.isEnableSnapshot();
    _enablePreload =
        enableSnapshot && upsertConfig.isEnablePreload() && tableDataManager.getSegmentPreloadExecutor() != null;
    double metadataTTL = upsertConfig.getMetadataTTL();
    double deletedKeysTTL = upsertConfig.getDeletedKeysTTL();
    _enableDeletedKeysCompactionConsistency = upsertConfig.isEnableDeletedKeysCompactionConsistency();
    _consistencyMode = upsertConfig.getConsistencyMode();
    if (_consistencyMode == null) {
      _consistencyMode = UpsertConfig.ConsistencyMode.NONE;
    }
    long upsertViewRefreshIntervalMs = upsertConfig.getUpsertViewRefreshIntervalMs();
    long newSegmentTrackingTimeMs = upsertConfig.getNewSegmentTrackingTimeMs();
    File tableIndexDir = tableDataManager.getTableDataDir();
    _context = new UpsertContext.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setPrimaryKeyColumns(primaryKeyColumns).setComparisonColumns(comparisonColumns)
        .setDeleteRecordColumn(deleteRecordColumn).setHashFunction(hashFunction)
        .setPartialUpsertHandler(partialUpsertHandler).setEnableSnapshot(enableSnapshot)
        .setEnablePreload(_enablePreload).setMetadataTTL(metadataTTL).setDeletedKeysTTL(deletedKeysTTL)
        .setConsistencyMode(_consistencyMode).setUpsertViewRefreshIntervalMs(upsertViewRefreshIntervalMs)
        .setNewSegmentTrackingTimeMs(newSegmentTrackingTimeMs).setTableIndexDir(tableIndexDir)
        .setDropOutOfOrderRecord(upsertConfig.isDropOutOfOrderRecord())
        .setEnableDeletedKeysCompactionConsistency(_enableDeletedKeysCompactionConsistency)
        .setTableDataManager(tableDataManager).build();
    LOGGER.info(
        "Initialized {} for table: {} with primary key columns: {}, comparison columns: {}, delete record column: {},"
            + " hash function: {}, upsert mode: {}, enable snapshot: {}, enable preload: {}, metadata TTL: {},"
            + " deleted Keys TTL: {}, consistency mode: {}, upsert view refresh interval: {}ms, new segment tracking"
            + " time: {}ms, table index dir: {}", getClass().getSimpleName(), _tableNameWithType, primaryKeyColumns,
        comparisonColumns, deleteRecordColumn, hashFunction, upsertConfig.getMode(), enableSnapshot, _enablePreload,
        metadataTTL, deletedKeysTTL, _consistencyMode, upsertViewRefreshIntervalMs, newSegmentTrackingTimeMs,
        tableIndexDir);

    initCustomVariables();
  }

  /**
   * Can be overridden to initialize custom variables after other variables are set but before preload starts. This is
   * needed because preload will load segments which might require these custom variables.
   */
  protected void initCustomVariables() {
  }

  @Override
  public UpsertConfig.Mode getUpsertMode() {
    return _context.getPartialUpsertHandler() == null ? UpsertConfig.Mode.FULL : UpsertConfig.Mode.PARTIAL;
  }

  @Override
  public UpsertConfig.ConsistencyMode getUpsertConsistencyMode() {
    return _consistencyMode;
  }

  @Override
  public boolean isEnablePreload() {
    return _enablePreload;
  }
}
