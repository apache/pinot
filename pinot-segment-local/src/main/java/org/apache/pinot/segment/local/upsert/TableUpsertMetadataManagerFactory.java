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
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableUpsertMetadataManagerFactory {
  private TableUpsertMetadataManagerFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableUpsertMetadataManagerFactory.class);
  public static final String UPSERT_DEFAULT_METADATA_MANAGER_CLASS = "default.metadata.manager.class";
  public static final String UPSERT_DEFAULT_ENABLE_SNAPSHOT = "default.enable.snapshot";
  public static final String UPSERT_DEFAULT_ENABLE_PRELOAD = "default.enable.preload";

  public static TableUpsertMetadataManager create(TableConfig tableConfig,
      @Nullable PinotConfiguration instanceUpsertConfigs) {
    String tableNameWithType = tableConfig.getTableName();
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    Preconditions.checkArgument(upsertConfig != null, "Must provide upsert config for table: %s", tableNameWithType);

    TableUpsertMetadataManager metadataManager;
    String tableMetadataManagerClass = upsertConfig.getMetadataManagerClass();

    String defaultMetadataManagerClass =
        instanceUpsertConfigs != null ? instanceUpsertConfigs.getProperty(UPSERT_DEFAULT_METADATA_MANAGER_CLASS) : null;
    // Use the default metadata manager class mentioned in the server config if the table config does not specify one.
    String metadataManagerClass = tableMetadataManagerClass != null ? tableMetadataManagerClass
        : defaultMetadataManagerClass;

    if (instanceUpsertConfigs != null) {
      // Server level config honoured only when table level config is not set to true
      if (!upsertConfig.isEnableSnapshot()) {
        upsertConfig.setEnableSnapshot(
            Boolean.parseBoolean(instanceUpsertConfigs.getProperty(UPSERT_DEFAULT_ENABLE_SNAPSHOT, "false")));
      }

      // Server level config honoured only when table level config is not set to true
      if (!upsertConfig.isEnablePreload()) {
        upsertConfig.setEnablePreload(
            Boolean.parseBoolean(instanceUpsertConfigs.getProperty(UPSERT_DEFAULT_ENABLE_PRELOAD, "false")));
      }
    }

    if (StringUtils.isNotEmpty(metadataManagerClass)) {
      LOGGER.info("Creating TableUpsertMetadataManager with class: {} for table: {}", metadataManagerClass,
          tableNameWithType);
      try {
        metadataManager =
            (TableUpsertMetadataManager) Class.forName(metadataManagerClass).newInstance();
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Caught exception while constructing TableUpsertMetadataManager with class: %s for table: %s",
                metadataManagerClass, tableNameWithType), e);
      }
    } else {
      LOGGER.info("Creating ConcurrentMapTableUpsertMetadataManager for table: {}", tableNameWithType);
      metadataManager = new ConcurrentMapTableUpsertMetadataManager();
    }

    return metadataManager;
  }
}
