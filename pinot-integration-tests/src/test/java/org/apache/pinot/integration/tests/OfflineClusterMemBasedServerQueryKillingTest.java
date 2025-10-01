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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for heap size based server query killing, this works only for xmx4G.
 */
public class OfflineClusterMemBasedServerQueryKillingTest extends BaseClusterIntegrationTestSet {
  private static final String STRING_DIM_SV1 = "stringDimSV1";
  private static final String STRING_DIM_SV2 = "stringDimSV2";
  private static final String INT_DIM_SV1 = "intDimSV1";
  private static final String LONG_DIM_SV1 = "longDimSV1";
  private static final String DOUBLE_DIM_SV1 = "doubleDimSV1";
  private static final String BOOLEAN_DIM_SV1 = "booleanDimSV1";
  private static final int NUM_SEGMENTS = 3;
  private static final int NUM_DOCS_PER_SEGMENT = 3_000_000;

  private static final String OOM_QUERY =
      "SELECT DISTINCT stringDimSV2 FROM mytable ORDER BY stringDimSV2 LIMIT 3000000";

  private static final String OOM_QUERY_2 =
      "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14), stringDimSV2 FROM mytable GROUP BY 2 ORDER BY 1 LIMIT 3000000";

  /// SafeTrim sort-aggregation case, pair-wise combine
  private static final String OOM_QUERY_3 =
      "SET sortAggregateSingleThreadedNumSegmentsThreshold=1; SET sortAggregateLimitThreshold=3000001; "
          + "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14), stringDimSV2 FROM mytable GROUP BY 2 ORDER BY 2 LIMIT 3000000";

  /// SafeTrim sort-aggregation case, sequential combine
  private static final String OOM_QUERY_4 =
      "SET sortAggregateSingleThreadedNumSegmentsThreshold=10000; SET sortAggregateLimitThreshold=3000001; "
          + "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14), stringDimSV2 FROM mytable GROUP BY 2 ORDER BY 2 LIMIT 3000000";

  private static final String AGGREGATE_QUERY = "SELECT DISTINCT_COUNT_HLL(intDimSV1, 14) FROM mytable";
  private static final String SELECT_STAR_QUERY = "SELECT * FROM mytable LIMIT 5";
  private static final String OOM_SELECT_STAR_QUERY = "SELECT * FROM mytable LIMIT 3000000";

  /**
   * Keeps track of metadata of queries that have been terminated due to OOM.
   * This is used to verify that the query was killed and the method used to kill the query.
   */
  public static class TestAccountantFactory implements ThreadAccountantFactory {

    @Override
    public TestAccountant init(PinotConfiguration config, String instanceId, InstanceType instanceType) {
      return new TestAccountant(config, instanceId, instanceType);
    }

    public static class TestAccountant extends PerQueryCPUMemResourceUsageAccountant {
      private QueryResourceTracker _queryResourceTracker = null;

      public TestAccountant(PinotConfiguration config, String instanceId, InstanceType instanceType) {
        super(config, instanceId, instanceType);
      }

      @Override
      public void logTerminatedQuery(QueryResourceTracker queryTracker, long totalHeapUsage) {
        super.logTerminatedQuery(queryTracker, totalHeapUsage);
        _queryResourceTracker = queryTracker;
      }

      public void reset() {
        _queryResourceTracker = null;
      }

      public QueryResourceTracker getQueryResourceTracker() {
        return _queryResourceTracker;
      }
    }
  }

  private TestAccountantFactory.TestAccountant _testAccountant;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
    _testAccountant =
        (TestAccountantFactory.TestAccountant) _serverStarters.get(0).getServerInstance().getThreadAccountant();

    // Create and upload the schema and table config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_TABLE_NAME)
        .addSingleValueDimension(STRING_DIM_SV1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_DIM_SV2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(INT_DIM_SV1, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_DIM_SV1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DOUBLE_DIM_SV1, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(BOOLEAN_DIM_SV1, FieldSpec.DataType.BOOLEAN)
        .build();
    addSchema(schema);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(DEFAULT_TABLE_NAME).build();
    addTableConfig(tableConfig);

    List<File> avroFiles = createAvroFiles();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterMethod
  public void resetTestAccountant() {
    _testAccountant.reset();
  }

  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
    serverConf.setProperty(Server.CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT, Integer.MAX_VALUE);

    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_FACTORY_NAME, TestAccountantFactory.class.getName());
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0f);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.15f);
  }

  protected long getCountStarResult() {
    return NUM_SEGMENTS * NUM_DOCS_PER_SEGMENT;
  }

  @Test
  public void testMemoryAllocationStats()
      throws Exception {
    JsonNode queryResponse = postQuery(SELECT_STAR_QUERY);
    long offlineThreadMemAllocatedBytes = queryResponse.get("offlineThreadMemAllocatedBytes").asLong();
    long offlineResponseSerMemAllocatedBytes = queryResponse.get("offlineResponseSerMemAllocatedBytes").asLong();
    long offlineTotalMemAllocatedBytes = queryResponse.get("offlineTotalMemAllocatedBytes").asLong();

    assertTrue(offlineThreadMemAllocatedBytes > 0);
    assertTrue(offlineResponseSerMemAllocatedBytes > 0);
    assertEquals(offlineThreadMemAllocatedBytes + offlineResponseSerMemAllocatedBytes, offlineTotalMemAllocatedBytes);
  }

  @DataProvider
  public String[] oomQueries() {
    return new String[]{OOM_QUERY, OOM_QUERY_2, OOM_QUERY_3, OOM_QUERY_4, OOM_SELECT_STAR_QUERY};
  }

  @Test(dataProvider = "oomQueries")
  public void testOomSse(String query)
      throws Exception {
    testOom(query);
    setUseMultiStageQueryEngine(true);
    testOom(query);
  }

  private void testOom(String query)
      throws Exception {
    JsonNode queryResponse = postQuery(query);
    JsonNode exceptionsNode = queryResponse.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + query);
    assertEquals(exceptionsNode.size(), 1, "Expected 1 exception for query: " + query + ", but got: " + exceptionsNode);
    JsonNode exceptionNode = exceptionsNode.get(0);
    JsonNode errorCodeNode = exceptionNode.get("errorCode");
    assertNotNull(errorCodeNode, "Missing errorCode from exception: " + exceptionNode);
    int errorCode = errorCodeNode.asInt();
    assertEquals(errorCode, QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.getId(),
        "Unexpected error code: " + errorCode + " from exception: " + exceptionNode);
    JsonNode messageNode = exceptionNode.get("message");
    assertNotNull(messageNode, "Missing message from exception: " + exceptionNode);
    String message = messageNode.asText();
    assertTrue(message.contains("OOM killed on SERVER"),
        "Unexpected exception message: " + message + " from exception: " + exceptionNode);
    assertEquals(queryResponse.get("requestId").asLong(),
        _testAccountant.getQueryResourceTracker().getExecutionContext().getRequestId());
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testOomMultipleQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    AtomicReference<JsonNode> queryResponse1 = new AtomicReference<>();
    AtomicReference<JsonNode> queryResponse2 = new AtomicReference<>();
    AtomicReference<JsonNode> queryResponse3 = new AtomicReference<>();
    CountDownLatch countDownLatch = new CountDownLatch(3);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    executor.submit(() -> {
      queryResponse1.set(postQuery(OOM_QUERY));
      countDownLatch.countDown();
      return null;
    });
    executor.submit(() -> {
      queryResponse2.set(postQuery(AGGREGATE_QUERY));
      countDownLatch.countDown();
      return null;
    });
    executor.submit(() -> {
      queryResponse3.set(postQuery(SELECT_STAR_QUERY));
      countDownLatch.countDown();
      return null;
    });
    executor.shutdown();
    countDownLatch.await();

    JsonNode queryResponse = queryResponse1.get();
    JsonNode exceptionsNode = queryResponse.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + OOM_QUERY);
    assertEquals(exceptionsNode.size(), 1,
        "Expected 1 exception for query: " + OOM_QUERY + ", but got: " + exceptionsNode);
    JsonNode exceptionNode = exceptionsNode.get(0);
    JsonNode errorCodeNode = exceptionNode.get("errorCode");
    assertNotNull(errorCodeNode, "Missing errorCode from exception: " + exceptionNode);
    int errorCode = errorCodeNode.asInt();
    assertEquals(errorCode, QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.getId(),
        "Unexpected error code: " + errorCode + " from exception: " + exceptionNode);
    JsonNode messageNode = exceptionNode.get("message");
    assertNotNull(messageNode, "Missing message from exception: " + exceptionNode);
    String message = messageNode.asText();
    assertTrue(message.contains("OOM killed on SERVER"),
        "Unexpected exception message: " + message + " from exception: " + exceptionNode);
    assertEquals(queryResponse.get("requestId").asLong(),
        _testAccountant.getQueryResourceTracker().getExecutionContext().getRequestId());

    queryResponse = queryResponse2.get();
    exceptionsNode = queryResponse.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + AGGREGATE_QUERY);
    assertTrue(exceptionsNode.isEmpty(),
        "Expected no exceptions for query: " + AGGREGATE_QUERY + ", but got: " + exceptionsNode);

    queryResponse = queryResponse3.get();
    exceptionsNode = queryResponse.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + SELECT_STAR_QUERY);
    assertTrue(exceptionsNode.isEmpty(),
        "Expected no exceptions for query: " + SELECT_STAR_QUERY + ", but got: " + exceptionsNode);
  }

  private List<File> createAvroFiles()
      throws IOException {
    // Create Avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(new Field(STRING_DIM_SV1, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(STRING_DIM_SV2, org.apache.avro.Schema.create(Type.STRING), null, null),
        new Field(INT_DIM_SV1, org.apache.avro.Schema.create(Type.INT), null, null),
        new Field(LONG_DIM_SV1, org.apache.avro.Schema.create(Type.LONG), null, null),
        new Field(DOUBLE_DIM_SV1, org.apache.avro.Schema.create(Type.DOUBLE), null, null),
        new Field(BOOLEAN_DIM_SV1, org.apache.avro.Schema.create(Type.BOOLEAN), null, null)));

    // Create Avro files
    List<File> ret = new ArrayList<>();
    for (int segmentId = 0; segmentId < NUM_SEGMENTS; segmentId++) {
      File avroFile = new File(_tempDir, "data_" + segmentId + ".avro");
      try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);

        int randBound = NUM_DOCS_PER_SEGMENT / 2;
        Random random = new Random(0);
        for (int docId = 0; docId < NUM_DOCS_PER_SEGMENT; docId++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(STRING_DIM_SV1, "test query killing");
          record.put(STRING_DIM_SV2, "test query killing" + docId);
          record.put(INT_DIM_SV1, random.nextInt(randBound));
          record.put(LONG_DIM_SV1, random.nextLong());
          record.put(DOUBLE_DIM_SV1, random.nextDouble());
          record.put(BOOLEAN_DIM_SV1, true);
          fileWriter.append(record);
        }
        ret.add(avroFile);
      }
    }
    return ret;
  }
}
