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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.HelixManager;
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
  protected TableDataManager _tableDataManager;
  protected HelixManager _helixManager;
  protected ExecutorService _segmentPreloadExecutor;
  protected UpsertContext _context;

  @Override
  public void init(TableConfig tableConfig, Schema schema, TableDataManager tableDataManager, HelixManager helixManager,
      @Nullable ExecutorService segmentPreloadExecutor) {
    _tableNameWithType = tableConfig.getTableName();
    _tableDataManager = tableDataManager;
    _helixManager = helixManager;
    _segmentPreloadExecutor = segmentPreloadExecutor;

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
      Map<String, UpsertConfig.Strategy> partialUpsertStrategies = upsertConfig.getPartialUpsertStrategies();
      Preconditions.checkArgument(partialUpsertStrategies != null,
          "Partial-upsert strategies must be configured for partial-upsert enabled table: %s", _tableNameWithType);
      partialUpsertHandler =
          new PartialUpsertHandler(schema, partialUpsertStrategies, upsertConfig.getDefaultPartialUpsertStrategy(),
              comparisonColumns);
    }

    String deleteRecordColumn = upsertConfig.getDeleteRecordColumn();
    HashFunction hashFunction = upsertConfig.getHashFunction();
    boolean enableSnapshot = upsertConfig.isEnableSnapshot();
    boolean enablePreload = upsertConfig.isEnablePreload();
    double metadataTTL = upsertConfig.getMetadataTTL();
    double deletedKeysTTL = upsertConfig.getDeletedKeysTTL();
    File tableIndexDir = tableDataManager.getTableDataDir();
    _context = new UpsertContext.Builder().setTableConfig(tableConfig).setSchema(schema)
        .setPrimaryKeyColumns(primaryKeyColumns).setComparisonColumns(comparisonColumns)
        .setDeleteRecordColumn(deleteRecordColumn).setHashFunction(hashFunction)
        .setPartialUpsertHandler(partialUpsertHandler).setEnableSnapshot(enableSnapshot).setEnablePreload(enablePreload)
        .setMetadataTTL(metadataTTL).setDeletedKeysTTL(deletedKeysTTL).setTableIndexDir(tableIndexDir).build();
    LOGGER.info(
        "Initialized {} for table: {} with primary key columns: {}, comparison columns: {}, delete record column: {},"
            + " hash function: {}, upsert mode: {}, enable snapshot: {}, enable preload: {}, metadata TTL: {},"
            + " deleted Keys TTL: {}, table index dir: {}", getClass().getSimpleName(), _tableNameWithType,
        primaryKeyColumns, comparisonColumns, deleteRecordColumn, hashFunction, upsertConfig.getMode(), enableSnapshot,
        enablePreload, metadataTTL, deletedKeysTTL, tableIndexDir);

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
}
