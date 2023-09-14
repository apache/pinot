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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class TableConfigTest {

  private static final String TEST_OFFLINE_TABLE_NAME = "testllc_OFFLINE";
  private static final String TEST_REALTIME_HLC_TABLE_NAME = "testhlc_REALTIME";
  private static final String TEST_REALTIME_LLC_TABLE_NAME = "testllc_REALTIME";

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
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TEST_OFFLINE_TABLE_NAME).setNumReplicas(2).build();
    assertEquals(2, offlineTableConfig.getReplication());

    offlineTableConfig.getValidationConfig().setReplication("4");
    assertEquals(4, offlineTableConfig.getReplication());

    offlineTableConfig.getValidationConfig().setReplicasPerPartition("3");
    assertEquals(4, offlineTableConfig.getReplication());

    TableConfig realtimeLLCTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TEST_REALTIME_LLC_TABLE_NAME)
            .setStreamConfigs(getStreamConfigMap("lowlevel")).setNumReplicas(2).build();

    assertEquals(2, realtimeLLCTableConfig.getReplication());

    realtimeLLCTableConfig.getValidationConfig().setReplication("4");
    assertEquals(2, realtimeLLCTableConfig.getReplication());

    realtimeLLCTableConfig.getValidationConfig().setReplicasPerPartition("3");
    assertEquals(3, realtimeLLCTableConfig.getReplication());
  }

  private Map<String, String> getStreamConfigMap(String consumerType) {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("streamType", "kafka");
    configMap.put("stream.kafka.consumer.type", consumerType);
    configMap.put("stream.kafka.topic.name", "test");
    configMap.put("stream.kafka.decoder.class.name", "test");
    return configMap;
  }
}
