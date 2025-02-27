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
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.scalar.TestFunctions;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.ClusterIntegrationTestUtils.getBrokerQueryApiUrl;


/*
 * This test verifies that cancel is being called on queries sent by the broker if exception occurs in any operator.
 */
public class QueryFailureIntegrationTest extends BaseClusterIntegrationTestSet implements ExplainIntegrationTestTrait {

  static final int FILES_NO = 2;
  static final int RECORDS_NO = 5;
  static final String I_COL = "i";
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
        .build();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    List<File> avroFiles = createAvroFile(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    setUseMultiStageQueryEngine(true);
    TestUtils.waitForCondition(() -> getCurrentCountStarResult(DEFAULT_TABLE_NAME) == FILES_NO * RECORDS_NO, 100L,
        60_000,
        "Failed to load  documents", true, Duration.ofMillis(60_000 / 10));

    Map<String, List<String>> map = getTableServersToSegmentsMap(getTableName(), TableType.OFFLINE);
    // make sure segments are split between multiple servers
    org.testng.Assert.assertEquals(map.size(), SERVERS_NO);

    TestFunctions.enable();
  }

  @Test
  public void testCancellationOnMaxRowsInJoinError()
      throws Exception {
    // query should fail in subquery on maxRowsInJoin limit and the other 'sleepy' query should get cancelled
    JsonNode result = postQuery("set maxRowsInJoin=2; "
        + "select * from "
        + "(select sum(t1.i+t2.i) "
        + "from mytable t1 "
        + "join mytable t2 on t1.i!=t2.i "
        + "group by t1.i, t2.i) "
        + "union all "
        + "select sum(sleep(i+1000)) "
        + "from mytable ");
    Assert.assertTrue(
        ITestUtils.toResultStr(result).contains(
            "245=Cannot build in memory right table for join operator"));
  }

  @Test
  public void testCancellationOnNumGroupsLimitError()
      throws Exception {

    // query should fail in subquery on num groups limit and the other 'sleepy' query should get cancelled
    JsonNode result = postQuery(
        "set errorOnNumGroupsLimit=true; set numGroupsLimit=1; "
            + "select * from "
            + "(select count(*) "
            + "from mytable "
            + "group by i) "
            + "union all "
            + "select sum(sleep(i+1000)) "
            + "from mytable ");

    String resultStr = ITestUtils.toResultStr(result);
    Assert.assertTrue(resultStr, resultStr.contains("NUM_GROUPS_LIMIT has been reached at AggregateOperator"));
  }

  @Test
  public void testCancellationOnFunctionCallError()
      throws Exception {
    // trigger exception only in thread processing of first segment
    String query =
        "select i, sum(case when i = 0 then throwError(i) else sleep(i+1000) end) "
            + " from mytable "
            + " group by i";

    JsonNode result = postQuery(query);

    Assert.assertTrue(
        ITestUtils.toResultStr(result)
            .contains(
                "Caught exception while invoking method: public long org.apache.pinot.common.function.scalar"
                    + ".TestFunctions.throwError(int) with arguments: [0]"));
  }

  public JsonNode postQuery(String query)
      throws Exception {
    return postQuery(query, getBrokerQueryApiUrl(getBrokerBaseApiUrl(), true), null,
        getExtraQueryProperties());
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

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);

    TestFunctions.disable();
  }

  public static List<File> createAvroFile(File tempDir)
      throws IOException {

    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(I_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null)));

    List<File> files = new ArrayList<>();
    for (int file = 0; file < FILES_NO; file++) {
      File avroFile = new File(tempDir, "data_" + file + ".avro");
      try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);

        for (int docId = 0; docId < RECORDS_NO; docId++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(I_COL, file);
          fileWriter.append(record);
        }
        files.add(avroFile);
      }
    }
    return files;
  }
}
