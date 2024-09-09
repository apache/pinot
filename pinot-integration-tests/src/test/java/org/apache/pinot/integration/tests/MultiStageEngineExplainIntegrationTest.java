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
import java.io.File;
import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.intellij.lang.annotations.Language;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MultiStageEngineExplainIntegrationTest extends BaseClusterIntegrationTest {

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
    brokerConf.setProperty(CommonConstants.MultiStageQueryRunner.ASK_SERVERS_FOR_EXPLAIN_PLAN, "true");
  }

  @BeforeMethod
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(true);
  }

  @Test
  public void simpleQuery() {
    explain("SELECT 1 FROM mytable",
        //@formatter:off
        "Execution Plan\n"
            + "PinotLogicalExchange(distribution=[broadcast])\n"
            + "  LeafStageCombineOperator\n"
            + "    StreamingInstanceResponse\n"
            + "      StreamingCombineSelect(repeatedOnSegments=[12])\n"
            + "        SelectStreaming(table=[mytable], totalDocs=[115545])\n"
            + "          Transform(expressions=[['1']])\n"
            + "            Project(columns=[[]])\n"
            + "              DocIdSet(maxDocs=[120000])\n"
            + "                FilterMatchEntireSegment(numDocs=[115545])\n");
        //@formatter:on
  }

  @Test
  public void simpleQueryVerbose() {
    explainVerbose("SELECT 1 FROM mytable",
        //@formatter:off
        "Execution Plan\n"
            + "IntermediateCombine\n"
            + "  Alternative(servers=[1])\n"
            + "    PinotLogicalExchange(distribution=[broadcast])\n"
            + "      LeafStageCombineOperator\n"
            + "        StreamingInstanceResponse\n"
            + "          StreamingCombineSelect\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "  Alternative(servers=[1])\n"
            + "    PinotLogicalExchange(distribution=[broadcast])\n"
            + "      LeafStageCombineOperator\n"
            + "        StreamingInstanceResponse\n"
            + "          StreamingCombineSelect\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n"
            + "            SelectStreaming(segment=[any], table=[mytable], totalDocs=[any])\n"
            + "              Transform(expressions=[['1']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[10000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[any])\n");
    //@formatter:on
  }

  @Test
  public void simpleQueryLogical() {
    explainLogical("SELECT 1 FROM mytable",
        //@formatter:off
        "Execution Plan\n"
            + "LogicalProject(EXPR$0=[1])\n"
            + "  LogicalTableScan(table=[[default, mytable]])\n");
    //@formatter:on
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

  private void explainVerbose(@Language("sql") String query, String expected) {
    try {
      JsonNode jsonNode = postQuery("set explainPlanVerbose=true; explain plan for " + query);
      JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

      String actual = plan.asText()
          .replaceAll("numDocs=\\[[^\\]]*]", "numDocs=[any]")
          .replaceAll("segment=\\[[^\\]]*]", "segment=[any]")
          .replaceAll("totalDocs=\\[[^\\]]*]", "totalDocs=[any]");


      Assert.assertEquals(actual, expected);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void explain(@Language("sql") String query, String expected) {
    try {
      JsonNode jsonNode = postQuery("explain plan for " + query);
      JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

      Assert.assertEquals(plan.asText(), expected);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void explainLogical(@Language("sql") String query, String expected) {
    try {
      JsonNode jsonNode = postQuery("set explainAskingServers=false; explain plan for " + query);
      JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

      Assert.assertEquals(plan.asText(), expected);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
