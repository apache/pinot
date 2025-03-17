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
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class QueryThreadContextIntegrationTest extends BaseClusterIntegrationTest
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

  @Test(dataProvider = "useBothQueryEngines")
  public void testReqIdOnServer(boolean useMse)
      throws Exception {
    setUseMultiStageQueryEngine(useMse);
    JsonNode jsonNode = postQuery("SELECT reqId(Dest), count(*) "
        + "FROM mytable "
        + "GROUP BY 1");
    DocumentContext parsed = JsonPath.parse(jsonNode.toString());
    Assert.assertEquals(parsed.read("$.numRowsResultSet", Integer.class), 1, "Unexpected number of rows");
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][1]", Integer.class), 115545, "Unexpected count");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testReqIdOnBroker(boolean useMse)
      throws Exception {
    setUseMultiStageQueryEngine(useMse);
    JsonNode jsonNode = postQuery("SELECT reqId('cte'), count(*) "
        + "FROM mytable "
        + "GROUP BY 1");
    DocumentContext parsed = JsonPath.parse(jsonNode.toString());
    Assert.assertEquals(parsed.read("$.numRowsResultSet", Integer.class), 1, "Unexpected number of rows");
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][1]", Integer.class), 115545, "Unexpected count");
    String requestId = parsed.read("$.resultTable.rows[0][0]", String.class);
    Assert.assertNotNull(requestId, "Request id should not be null");
    Assert.assertTrue(Long.parseLong(requestId) > 0, "Request id should be positive");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testReqIdAfterGroup(boolean useMse)
      throws Exception {
    setUseMultiStageQueryEngine(useMse);
    String varcharType = useMse ? "VARCHAR" : "String";
    JsonNode jsonNode = postQuery("SELECT reqId(Dest), reqId(CAST (SUM(ArrTime) AS " + varcharType + ")) "
        + "FROM mytable "
        + "GROUP BY 1");
    DocumentContext parsed = JsonPath.parse(jsonNode.toString());
    Assert.assertEquals(parsed.read("$.numRowsResultSet", Integer.class), 1, "Unexpected number of rows");
    Long serverReq = parsed.read("$.resultTable.rows[0][0]", Long.class);
    Long postGroupReq = parsed.read("$.resultTable.rows[0][1]", Long.class);
    Assert.assertEquals(postGroupReq, serverReq, "Unexpected req id post GROUP BY");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCidOnServer(boolean useMse)
      throws Exception {
    setUseMultiStageQueryEngine(useMse);
    String clientRequestId = "testCid";
    JsonNode jsonNode = postQuery("SET clientQueryId='" + clientRequestId + "'; "
        + "SELECT cid(Dest), count(*) "
        + "FROM mytable "
        + "GROUP BY 1");
    DocumentContext parsed = JsonPath.parse(jsonNode.toString());
    Assert.assertEquals(parsed.read("$.numRowsResultSet", Integer.class), 1, "Unexpected number of rows");
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][0]", String.class), clientRequestId, "Unexpected cid");
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][1]", Integer.class), 115545, "Unexpected count");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCidOnBroker(boolean useMse)
      throws Exception {
    setUseMultiStageQueryEngine(useMse);
    String clientRequestId = "testCid";
    JsonNode jsonNode = postQuery("SET clientQueryId='" + clientRequestId + "'; "
        + "SELECT cid('cte'), count(*) "
        + "FROM mytable "
        + "GROUP BY 1");
    DocumentContext parsed = JsonPath.parse(jsonNode.toString());
    Assert.assertEquals(parsed.read("$.numRowsResultSet", Integer.class), 1, "Unexpected number of rows");
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][0]", String.class), clientRequestId, "Unexpected cid");
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][1]", Integer.class), 115545, "Unexpected count");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCidIdAfterGroup(boolean useMse)
      throws Exception {
    setUseMultiStageQueryEngine(useMse);
    String varcharType = useMse ? "VARCHAR" : "String";
    String clientRequestId = "testCid";
    JsonNode jsonNode = postQuery("SET clientQueryId='" + clientRequestId + "'; "
        + "SELECT cid(Dest), cid(CAST (SUM(ArrTime) AS " + varcharType + ")) "
        + "FROM mytable "
        + "GROUP BY 1");
    DocumentContext parsed = JsonPath.parse(jsonNode.toString());
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][0]", String.class), clientRequestId, "Unexpected cid");
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][1]", String.class), clientRequestId,
        "Unexpected cid post GROUP BY");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStageIdOnServer(boolean useMse)
      throws Exception {
    setUseMultiStageQueryEngine(useMse);
    JsonNode jsonNode = postQuery("SELECT stageId(Dest) "
        + "FROM mytable "
        + "GROUP BY 1");
    DocumentContext parsed = JsonPath.parse(jsonNode.toString());
    Assert.assertEquals(parsed.read("$.numRowsResultSet", Integer.class), 1, "Unexpected number of rows");

    // In SSE stageId always returns -1
    int stageId = useMse ? 2 : -1;
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][0]", Integer.class), stageId, "Unexpected stageId");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStageIdOnBroker(boolean useMse)
      throws Exception {
    setUseMultiStageQueryEngine(useMse);
    JsonNode jsonNode = postQuery("SELECT stageId('cte'), count(*) "
        + "FROM mytable "
        + "GROUP BY 1");
    DocumentContext parsed = JsonPath.parse(jsonNode.toString());
    Assert.assertEquals(parsed.read("$.numRowsResultSet", Integer.class), 1, "Unexpected number of rows");
    // When stageId is simplified in broker, stageId always returns -1
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][0]", Integer.class), -1, "Unexpected stageId");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStageIdIdAfterGroup(boolean useMse)
      throws Exception {
    setUseMultiStageQueryEngine(useMse);
    String varcharType = useMse ? "VARCHAR" : "String";
    JsonNode jsonNode = postQuery("SELECT stageId(Dest), stageId(CAST (SUM(ArrTime) AS " + varcharType + ")) "
        + "FROM mytable "
        + "GROUP BY 1");
    DocumentContext parsed = JsonPath.parse(jsonNode.toString());

    // In SSE stageId always returns -1
    int preGroupByStage = useMse ? 2 : -1;
    // In SSE stageId always returns -1
    int postGroupByStage = useMse ? 1 : -1;
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][0]", Integer.class), preGroupByStage,
        "Unexpected stageId");
    Assert.assertEquals(parsed.read("$.resultTable.rows[0][1]", Integer.class), postGroupByStage,
        "Unexpected stageId post GROUP BY");
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
