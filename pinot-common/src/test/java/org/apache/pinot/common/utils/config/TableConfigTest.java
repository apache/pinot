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
package org.apache.pinot.common.utils.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class TableConfigTest {
  private static final String RAW_TABLE_NAME = "testTable";

  @DataProvider
  public Object[][] configs()
      throws IOException {
    try (Stream<Path> configs = Files.list(Paths.get("src/test/resources/testConfigs"))) {
      return configs.map(path -> {
        try {
          return Files.readAllBytes(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).map(config -> new Object[]{config}).toArray(Object[][]::new);
    }
  }

  @Test(dataProvider = "configs")
  public void testConfigNotRejected(byte[] config)
      throws IOException {
    TableConfig tableConfig = JsonUtils.DEFAULT_READER.forType(TableConfig.class).readValue(config);
    assertTrue(StringUtils.isNotBlank(tableConfig.getTableName()));
  }

  @Test
  public void testGetReplication() {
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNumReplicas(2).build();
    assertEquals(offlineTableConfig.getReplication(), 2);
    offlineTableConfig.getValidationConfig().setReplication("4");
    assertEquals(offlineTableConfig.getReplication(), 4);

    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(2).build();
    assertEquals(realtimeTableConfig.getReplication(), 2);
    realtimeTableConfig.getValidationConfig().setReplication("4");
    assertEquals(realtimeTableConfig.getReplication(), 4);
    realtimeTableConfig.getValidationConfig().setReplication("3");
    assertEquals(realtimeTableConfig.getReplication(), 3);
  }

  @Test
  public void testCopyConstructor() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setContinueOnError(true);
    ingestionConfig.setRetryOnSegmentBuildPrecheckFailure(true);
    ingestionConfig.setRowTimeValueCheck(true);
    ingestionConfig.setSegmentTimeValueCheck(false);

    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setHashFunction(HashFunction.MD5);

    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setAggregateMetrics(true)
        .setRetentionTimeValue("5")
        .setRetentionTimeUnit("DAYS")
        .setNumReplicas(2)
        .setIngestionConfig(ingestionConfig)
        .setDedupConfig(dedupConfig)
        .setQueryConfig(new QueryConfig(2000L, true, false, Collections.emptyMap(), 100_000L, 100_000L))
        .setTierConfigList(List.of(new TierConfig("name", "type", null, null, "storageType", null, null, null)))
        .build();

    TableConfig copy = new TableConfig(config);

    assertEquals(config, copy);
    assertEquals(config.toJsonString(), copy.toJsonString());
  }
}
