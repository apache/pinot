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
package org.apache.pinot.connector.flink.http;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class PinotConnectionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotConnectionUtils.class);

  private PinotConnectionUtils() {
  }

  public static Schema getSchema(ControllerRequestClient client, String tableName) {
    try {
      return client.getSchema(tableName);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get table schema %s from Pinot controller", tableName), e);
    }
  }

  public static TableConfig getTableConfig(ControllerRequestClient client, String tableName, String tableType) {
    TableConfig tableConfig;
    try {
      tableConfig = client.getTableConfig(tableName, TableType.valueOf(tableType));
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get table config %s from Pinot controller", tableName), e);
    }

    LOGGER.info("fetched pinot config {}", tableConfig);

    Map<String, String> newBatchConfigMap = new HashMap<>();
    // append the batch config of controller URI
    String controllerBaseUrl = client.getControllerRequestURLBuilder().getBaseUrl();
    newBatchConfigMap.put("push.controllerUri", controllerBaseUrl);
    newBatchConfigMap.put("outputDirURI", "/tmp/pinotoutput");

    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig == null) {
      ingestionConfig = new IngestionConfig();
      ingestionConfig.setBatchIngestionConfig(
          new BatchIngestionConfig(Collections.singletonList(newBatchConfigMap), "APPEND", "HOURLY"));
      tableConfig.setIngestionConfig(ingestionConfig);
      return tableConfig;
    }

    BatchIngestionConfig batchIngestionConfig = ingestionConfig.getBatchIngestionConfig();
    if (batchIngestionConfig == null) {
      ingestionConfig.setBatchIngestionConfig(
          new BatchIngestionConfig(Collections.singletonList(newBatchConfigMap), "APPEND", "HOURLY"));
      return tableConfig;
    }

    List<Map<String, String>> batchConfigMaps = batchIngestionConfig.getBatchConfigMaps();
    if (batchConfigMaps == null) {
      batchIngestionConfig.setBatchConfigMaps(Collections.singletonList(newBatchConfigMap));
    } else {
      batchConfigMaps.add(newBatchConfigMap);
    }
    return tableConfig;
  }
}
