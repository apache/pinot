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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.ClusterIntegrationTestUtils.getBrokerQueryApiUrl;


// similar to GroupByOptionsIntegrationTest but this test verifies that default enable group trim option works even
// if hint is not set in the query
public class GroupByEnableTrimOptionIntegrationTest extends BaseClusterIntegrationTestSet {

  static final int FILES_NO = 4;
  static final int RECORDS_NO = 20;
  static final String I_COL = "i";
  static final String J_COL = "j";
  static final int SERVERS_NO = 2;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startServers(SERVERS_NO);
    startBroker();

    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_SCHEMA_NAME)
        .addSingleValueDimension(I_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(J_COL, FieldSpec.DataType.LONG)
        .build();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    List<File> avroFiles = GroupByOptionsIntegrationTest.createAvroFile(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    // Wait for all documents loaded
    TestUtils.waitForCondition(() -> getCurrentCountStarResult(DEFAULT_TABLE_NAME) == FILES_NO * RECORDS_NO, 100L,
        60_000,
        "Failed to load  documents", true, Duration.ofMillis(60_000 / 10));

    setUseMultiStageQueryEngine(true);

    Map<String, List<String>> map = getTableServersToSegmentsMap(getTableName(), TableType.OFFLINE);

    // make sure segments are split between multiple servers
    Assert.assertEquals(map.size(), SERVERS_NO);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    super.overrideBrokerConf(brokerConf);

    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_MSE_ENABLE_GROUP_TRIM, "true");
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    super.overrideServerConf(serverConf);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_MSE_MIN_GROUP_TRIM_SIZE, "3");
  }

  @Test
  public void testOrderByKeysIsPushedToFinalAggregationStageWhenGroupTrimIsEnabledByDefault()
      throws Exception {
    final String trimEnabledPlan = "Execution Plan\n"
        + "LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], offset=[0], fetch=[3])\n"
        + "  PinotLogicalSortExchange(distribution=[hash], collation=[[0, 1]], isSortOnSender=[false], "
        + "isSortOnReceiver=[true])\n"
        + "    LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], fetch=[3])\n"
        // 'collations' below is the important bit
        + "      PinotLogicalAggregate(group=[{0, 1}], agg#0=[COUNT($2)], aggType=[FINAL], collations=[[0, "
        + "1]], limit=[3])\n"
        + "        PinotLogicalExchange(distribution=[hash[0, 1]])\n"
        + "          LeafStageCombineOperator(table=[mytable])\n"
        + "            StreamingInstanceResponse\n"
        + "              CombineGroupBy\n"
        + "                GroupBy(groupKeys=[[i, j]], aggregations=[[count(*)]])\n"
        + "                  Project(columns=[[i, j]])\n"
        + "                    DocIdSet(maxDocs=[40000])\n"
        + "                      FilterMatchEntireSegment(numDocs=[80])\n";

    assertResultAndPlan(
        // group_trim_size should sort and limit v2 aggregate output if order by and limit is propagated
        " ",
        " select i, j, count(*) as cnt "
            + " from " + getTableName()
            + " group by i, j "
            + " order by i asc, j asc "
            + " limit 3",
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"cnt\"[\"LONG\"]\n"
            + "0,\t0,\t2\n"
            + "0,\t1,\t2\n"
            + "0,\t2,\t2",
        trimEnabledPlan);

    assertResultAndPlan(
        " ",
        " select /*+  aggOptions(bogus_hint='false') */  i, j, count(*) as cnt "
            + " from " + getTableName()
            + " group by i, j "
            + " order by i asc, j asc "
            + " limit 3",
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"cnt\"[\"LONG\"]\n"
            + "0,\t0,\t2\n"
            + "0,\t1,\t2\n"
            + "0,\t2,\t2",
        trimEnabledPlan);
  }

  @Test
  public void testOrderByKeysIsNotPushedToFinalAggregationStageWhenGroupTrimIsDisabledInHint()
      throws Exception {
    assertResultAndPlan(
        " ",
        " select /*+  aggOptions(is_enable_group_trim='false') */ i, j, count(*) as cnt "
            + " from " + getTableName()
            + " group by i, j "
            + " order by i asc, j asc "
            + " limit 3",
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"cnt\"[\"LONG\"]\n"
            + "0,\t0,\t2\n"
            + "0,\t1,\t2\n"
            + "0,\t2,\t2",
        "Execution Plan\n"
            + "LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], offset=[0], fetch=[3])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[0, 1]], isSortOnSender=[false], "
            + "isSortOnReceiver=[true])\n"
            + "    LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], fetch=[3])\n"
            // lack of 'collations' below is the important bit
            + "      PinotLogicalAggregate(group=[{0, 1}], agg#0=[COUNT($2)], aggType=[FINAL])\n"
            + "        PinotLogicalExchange(distribution=[hash[0, 1]])\n"
            + "          LeafStageCombineOperator(table=[mytable])\n"
            + "            StreamingInstanceResponse\n"
            + "              CombineGroupBy\n"
            + "                GroupBy(groupKeys=[[i, j]], aggregations=[[count(*)]])\n"
            + "                  Project(columns=[[i, j]])\n"
            + "                    DocIdSet(maxDocs=[40000])\n"
            + "                      FilterMatchEntireSegment(numDocs=[80])\n");
  }

  protected TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setNumReplicas(getNumReplicas())
        .setBrokerTenant(getBrokerTenant())
        .build();
  }

  // for debug only
  protected Properties getPinotConnectionProperties() {
    Properties properties = new Properties();
    properties.put("timeoutMs", "3600000");
    properties.put("brokerReadTimeoutMs", "3600000");
    properties.put("brokerConnectTimeoutMs", "3600000");
    properties.putAll(getExtraQueryProperties());
    return properties;
  }

  public void assertResultAndPlan(String option, String query, String expectedResult, String expectedPlan)
      throws Exception {
    String sql = option
        //disable timeout in debug
        + "set timeoutMs=3600000; set brokerReadTimeoutMs=3600000; set brokerConnectTimeoutMs=3600000; "
        + query;

    JsonNode result = postV2Query(sql);
    JsonNode plan = postV2Query(option + " set explainAskingServers=true; explain plan for " + query);

    Assert.assertEquals(ITestUtils.toResultStr(result), expectedResult);
    Assert.assertEquals(ITestUtils.toExplainStr(plan), expectedPlan);
  }

  private JsonNode postV2Query(String query)
      throws Exception {
    return postQuery(query, getBrokerQueryApiUrl(getBrokerBaseApiUrl(), true), null,
        getExtraQueryProperties());
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
