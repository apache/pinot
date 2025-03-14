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
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableDedupMetadataManagerFactory {
  private TableDedupMetadataManagerFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableDedupMetadataManagerFactory.class);
  public static final String DEDUP_DEFAULT_METADATA_MANAGER_CLASS = "default.metadata.manager.class";
  public static final String DEDUP_DEFAULT_ENABLE_PRELOAD = "default.enable.preload";

  public static final String DEDUP_DEFAULT_ALLOW_DEDUP_CONSUMPTION_DURING_COMMIT =
      "default.allow.dedup.consumption.during.commit";

  public static TableDedupMetadataManager create(TableConfig tableConfig, Schema schema,
      TableDataManager tableDataManager, ServerMetrics serverMetrics,
      @Nullable PinotConfiguration instanceDedupConfig) {
    String tableNameWithType = tableConfig.getTableName();
    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    Preconditions.checkArgument(dedupConfig != null, "Must provide dedup config for table: %s", tableNameWithType);

    TableDedupMetadataManager metadataManager;
    String metadataManagerClass = dedupConfig.getMetadataManagerClass();

    if (instanceDedupConfig != null) {
      if (metadataManagerClass == null) {
        metadataManagerClass = instanceDedupConfig.getProperty(DEDUP_DEFAULT_METADATA_MANAGER_CLASS);
      }

      // Server level config honoured only when table level config is not set to true
      if (!dedupConfig.isEnablePreload()) {
        dedupConfig.setEnablePreload(
            Boolean.parseBoolean(instanceDedupConfig.getProperty(DEDUP_DEFAULT_ENABLE_PRELOAD, "false")));
      }
    }
    if (StringUtils.isNotEmpty(metadataManagerClass)) {
      LOGGER.info("Creating TableDedupMetadataManager with class: {} for table: {}", metadataManagerClass,
          tableNameWithType);
      try {
        metadataManager =
            (TableDedupMetadataManager) Class.forName(metadataManagerClass).getConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Caught exception while constructing TableDedupMetadataManager with class: %s for table: %s",
                metadataManagerClass, tableNameWithType), e);
      }
    } else {
      LOGGER.info("Creating ConcurrentMapTableDedupMetadataManager for table: {}", tableNameWithType);
      metadataManager = new ConcurrentMapTableDedupMetadataManager();
    }

    // server level config honoured only when table level config is not set to true
    if (!dedupConfig.isAllowDedupConsumptionDuringCommit()) {
      dedupConfig.setAllowDedupConsumptionDuringCommit(Boolean.parseBoolean(
          instanceDedupConfig.getProperty(DEDUP_DEFAULT_ALLOW_DEDUP_CONSUMPTION_DURING_COMMIT, "false")));
    }

    metadataManager.init(tableConfig, schema, tableDataManager, serverMetrics);
    return metadataManager;
  }
}
