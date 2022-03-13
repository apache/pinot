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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MultivaluedHashMap;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class PinotConnectionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotConnectionUtils.class);

  private PinotConnectionUtils() {
  }

  public static Schema getSchema(PinotControllerClient client, String tableName) {
    try {
      String resp = client.getSchemaStrFromController(tableName, new MultivaluedHashMap<>());
      Schema schema = JsonUtils.stringToObject(resp, Schema.class);
      return schema;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get table schema %s from Pinot controller", tableName), e);
    }
  }

  public static TableConfig getTableConfig(PinotControllerClient client, String tableName, String tableType) {
    String resp = null;
    TableConfig tableConfig = null;
    try {
      resp = client.getPinotConfigStrFromController(tableName, PinotControllerClient.TableType.valueOfByType(tableType),
          new MultivaluedHashMap<>());
      tableConfig = JsonUtils.stringToObject(resp, TableConfig.class);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to get table config %s from Pinot controller", tableName), e);
    }

    LOGGER.info("fetched pinot config {}", resp);

    Map<String, String> newBatchConfigMaps = new HashMap<>();
    // append the batch config of controller URI
    List<String> addresses = client.getControllerInstances(new MultivaluedHashMap<>());
    // use the first address
    if (addresses.isEmpty()) {
      throw new IllegalStateException("Cannot find controller addresses");
    }
    newBatchConfigMaps.put("push.controllerUri", "http://" + addresses.get(0));
    newBatchConfigMaps.put("outputDirURI", "/tmp/pinotoutput");
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    if (ingestionConfig == null) {
      tableConfig.setIngestionConfig(
          new IngestionConfig(new BatchIngestionConfig(Lists.newArrayList(newBatchConfigMaps), "APPEND", "HOURLY"),
              null, null, null, null));
      return tableConfig;
    }
    if (ingestionConfig.getBatchIngestionConfig() == null) {
      tableConfig.setIngestionConfig(
          new IngestionConfig(new BatchIngestionConfig(Lists.newArrayList(newBatchConfigMaps), "APPEND", "HOURLY"),
              null, ingestionConfig.getFilterConfig(), ingestionConfig.getTransformConfigs(),
              ingestionConfig.getComplexTypeConfig()));
      return tableConfig;
    }

    List<Map<String, String>> batchConfigMaps =
        ingestionConfig.getBatchIngestionConfig().getBatchConfigMaps() == null ? new ArrayList<>()
            : ingestionConfig.getBatchIngestionConfig().getBatchConfigMaps();
    batchConfigMaps.add(newBatchConfigMaps);

    tableConfig.setIngestionConfig(new IngestionConfig(
        new BatchIngestionConfig(batchConfigMaps, ingestionConfig.getBatchIngestionConfig().getSegmentIngestionType(),
            ingestionConfig.getBatchIngestionConfig().getSegmentIngestionFrequency()), null,
        ingestionConfig.getFilterConfig(), ingestionConfig.getTransformConfigs(),
        ingestionConfig.getComplexTypeConfig()));

    return tableConfig;
  }
}
