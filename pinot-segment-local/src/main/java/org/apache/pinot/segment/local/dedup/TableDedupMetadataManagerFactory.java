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
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableDedupMetadataManagerFactory {
  private TableDedupMetadataManagerFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableDedupMetadataManagerFactory.class);

  public static TableDedupMetadataManager create(TableConfig tableConfig, Schema schema,
      TableDataManager tableDataManager, ServerMetrics serverMetrics) {
    String tableNameWithType = tableConfig.getTableName();
    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    Preconditions.checkArgument(dedupConfig != null, "Must provide dedup config for table: %s", tableNameWithType);

    TableDedupMetadataManager metadataManager;
    String metadataManagerClass = dedupConfig.getMetadataManagerClass();
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

    metadataManager.init(tableConfig, schema, tableDataManager, serverMetrics);
    return metadataManager;
  }
}
