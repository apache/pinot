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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.net.URL;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.controller.api.resources.TableAndSchemaConfig;
import org.apache.pinot.spi.config.table.ColumnStats;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PinotUpsertCapacityEstimationRestletResourceTest {
  private static final String TABLE_NAME = "restletTable_UPSERT";

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
  }

  @Test
  public void testEstimateHeapUsage()
      throws Exception {

    File schemaFile = readFile("memory_estimation/schema-for-upsert.json");
    File tableConfigFile = readFile("memory_estimation/table-config-for-upsert.json");
    Schema schema = JsonUtils.fileToObject(schemaFile, Schema.class);
    TableConfig tableConfig = JsonUtils.fileToObject(tableConfigFile, TableConfig.class);

    TableAndSchemaConfig tableAndSchemaConfig = new TableAndSchemaConfig(tableConfig, schema);
    String columnStats = new ColumnStats(10000, 48, 16, 8).toString();

    String estimateHeapUsageUrl =
        ControllerTestUtils.getControllerRequestURLBuilder().forUpsertTableHeapEstimation(columnStats);
    JsonNode result = JsonUtils.stringToJsonNode(
        ControllerTestUtils.sendPostRequest(estimateHeapUsageUrl, tableAndSchemaConfig.toJsonString()));
    assertEquals(result.get("tableName"), "restletTable_UPSERT");
    assertEquals(result.get("bytesPerKey"), 48);
    assertEquals(result.get("bytesPerValue"), 68);
    assertEquals(result.get("totalKeySpace(bytes)"), 480000);
    assertEquals(result.get("totalValueSpace(bytes)"), 680000);
    assertEquals(result.get("totalSpace(bytes)"), "1160000");
    assertEquals(result.get("numPartitions"), 8);
    assertEquals(result.get("replicasPerPartition"), 3);
    assertEquals(result.get("memoryPerHost"), 435000);
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }

  private File readFile(String fileName)
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(fileName);
    return new File(resource.toURI());
  }
}
