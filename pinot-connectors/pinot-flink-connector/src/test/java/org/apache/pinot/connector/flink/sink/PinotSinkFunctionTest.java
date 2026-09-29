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
package org.apache.pinot.connector.flink.sink;

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class PinotSinkFunctionTest {
  @Test
  public void testPrepareTableConfigForSinkAddsControllerBatchConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();

    TableConfig result = PinotSinkFunction.prepareTableConfigForSink(tableConfig, "http://localhost:9000");

    assertNotNull(result.getIngestionConfig());
    BatchIngestionConfig batchIngestionConfig = result.getIngestionConfig().getBatchIngestionConfig();
    assertNotNull(batchIngestionConfig);
    Map<String, String> batchConfigMap = batchIngestionConfig.getBatchConfigMaps().get(0);
    assertEquals(batchConfigMap.get(BatchConfigProperties.PUSH_CONTROLLER_URI), "http://localhost:9000");
    assertEquals(batchConfigMap.get(BatchConfigProperties.OUTPUT_DIR_URI),
        PinotSinkFunction.DEFAULT_OUTPUT_DIR_URI);
  }

  @Test
  public void testPrepareTableConfigForSinkMergesOverridesIntoExistingBatchConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig
        .setBatchIngestionConfig(new BatchIngestionConfig(List.of(Map.of("existing", "value")), "APPEND", "DAILY"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable")
        .setIngestionConfig(ingestionConfig)
        .build();

    TableConfig result = PinotSinkFunction.prepareTableConfigForSink(tableConfig, "http://localhost:9000");

    List<Map<String, String>> batchConfigMaps =
        result.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps();
    assertEquals(batchConfigMaps.size(), 1);
    Map<String, String> mergedBatchConfigMap = batchConfigMaps.get(0);
    assertEquals(mergedBatchConfigMap.get("existing"), "value");
    assertEquals(mergedBatchConfigMap.get(BatchConfigProperties.PUSH_CONTROLLER_URI), "http://localhost:9000");
    assertEquals(mergedBatchConfigMap.get(BatchConfigProperties.OUTPUT_DIR_URI),
        PinotSinkFunction.DEFAULT_OUTPUT_DIR_URI);
  }

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*requires exactly 1 batchConfigMap.*")
  public void testPrepareTableConfigForSinkRejectsMultipleBatchConfigs() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(List.of(Map.of("first", "value"), Map.of("second", "value")), "APPEND", "DAILY"));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable")
        .setIngestionConfig(ingestionConfig)
        .build();

    PinotSinkFunction.prepareTableConfigForSink(tableConfig, "http://localhost:9000");
  }
}
