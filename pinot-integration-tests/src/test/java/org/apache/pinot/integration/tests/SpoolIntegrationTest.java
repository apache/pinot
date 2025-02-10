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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SpoolIntegrationTest extends BaseClusterIntegrationTest
    implements ExplainIntegrationTestTrait {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(2);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    String property = CommonConstants.MultiStageQueryRunner.KEY_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN;
    brokerConf.setProperty(property, "true");
  }

  @BeforeMethod
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(true);
  }

  // Test that intermediate stages can be spooled.
  // In this case Stage 4 is an intermediate stage whose single child is stage 5.
  // Stage 4 is spooled and sends data to stages 3 and 7
  @Test
  public void intermediateSpool()
      throws Exception {
    JsonNode jsonNode = postQuery("SET useSpools = true;\n"
        + "WITH group_and_sum AS (\n"
        + "  SELECT ArrTimeBlk,\n"
        + "    Dest,\n"
        + "    SUM(ArrTime) AS ArrTime\n"
        + "  FROM mytable\n"
        + "  GROUP BY ArrTimeBlk,\n"
        + "    Dest\n"
        + "  limit 1000\n"
        + "),\n"
        + "aggregated_data AS (\n"
        + "  SELECT\n"
        + "    Dest,\n"
        + "    SUM(ArrTime) AS ArrTime\n"
        + "  FROM group_and_sum\n"
        + "  GROUP BY\n"
        + "    Dest\n"
        + "),\n"
        + "joined AS (\n"
        + "  SELECT\n"
        + "    s.Dest,\n"
        + "    s.ArrTime,\n"
        + "    (o.ArrTime) AS ArrTime2\n"
        + "  FROM group_and_sum s\n"
        + "  JOIN aggregated_data o\n"
        + "  ON s.Dest = o.Dest\n"
        + ")\n"
        + "SELECT *\n"
        + "FROM joined\n"
        + "LIMIT 1");
    JsonNode stats = jsonNode.get("stageStats");
    DocumentContext parsed = JsonPath.parse(stats.toString());
    List<Map<String, Object>> stage4On3 = parsed.read("$..[?(@.stage == 3)]..[?(@.stage == 4)]");
    Assert.assertNotNull(stage4On3, "Stage 4 should be a descendant of stage 3");
    Assert.assertEquals(stage4On3.size(), 1, "Stage 4 should only be descended from stage 3 once");

    List<Map<String, Object>> stage4On7 = parsed.read("$..[?(@.stage == 7)]..[?(@.stage == 4)]");
    Assert.assertNotNull(stage4On7, "Stage 4 should be a descendant of stage 7");
    Assert.assertEquals(stage4On7.size(), 1, "Stage 4 should only be descended from stage 7 once");

    Assert.assertEquals(stage4On3, stage4On7, "Stage 4 should be the same in both stage 3 and stage 7");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
