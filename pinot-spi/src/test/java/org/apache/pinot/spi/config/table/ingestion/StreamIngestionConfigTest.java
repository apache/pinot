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
package org.apache.pinot.spi.config.table.ingestion;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests the opt-in consumption mode and its table configuration serialization.
public class StreamIngestionConfigTest {
  @Test
  public void testLegacyConfigurationDefaultsToIndependentConsumption()
      throws IOException {
    StreamIngestionConfig config = JsonUtils.stringToObject(
        "{\"streamConfigMaps\":[{\"streamType\":\"kafka\"}]}", StreamIngestionConfig.class);
    assertFalse(config.isReplicateMode());
  }

  @Test
  public void testReplicateModeTableConfigurationRoundTrip()
      throws IOException {
    StreamIngestionConfig streamConfig = new StreamIngestionConfig(List.of(Map.of("streamType", "kafka")));
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(streamConfig);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable")
        .setNumReplicas(3).setIngestionConfig(ingestionConfig).build();
    for (boolean replicateMode : List.of(false, true)) {
      streamConfig.setReplicateMode(replicateMode);
      TableConfig restored = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(IngestionConfigUtils.isReplicateModeEnabled(restored), replicateMode);
      assertEquals(restored.getReplication(), 3);
    }
  }

  @Test
  public void testMissingStreamConfigurationDefaultsToIndependentConsumption() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").build();
    assertFalse(IngestionConfigUtils.isReplicateModeEnabled(tableConfig));
    IngestionConfig ingestionConfig = new IngestionConfig();
    tableConfig.setIngestionConfig(ingestionConfig);
    assertFalse(IngestionConfigUtils.isReplicateModeEnabled(tableConfig));
    StreamIngestionConfig streamConfig = new StreamIngestionConfig(List.of());
    ingestionConfig.setStreamIngestionConfig(streamConfig);
    assertFalse(IngestionConfigUtils.isReplicateModeEnabled(tableConfig));
    streamConfig.setReplicateMode(true);
    assertTrue(IngestionConfigUtils.isReplicateModeEnabled(tableConfig));
  }
}
