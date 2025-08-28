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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for heap size based server query killing, this works only for xmx4G.
 * <p>
 * Query killing isn't currently supported in the v2 multi-stage query engine so these tests only run on the v1 engine.
 */
public class OfflineClusterMemBasedServerQueryKillingTest extends BaseClusterIntegrationTestSet {
  public static final String STRING_DIM_SV1 = "stringDimSV1";
  public static final String STRING_DIM_SV2 = "stringDimSV2";
  public static final String INT_DIM_SV1 = "intDimSV1";
  public static final String LONG_DIM_SV1 = "longDimSV1";
  public static final String DOUBLE_DIM_SV1 = "doubleDimSV1";
  public static final String BOOLEAN_DIM_SV1 = "booleanDimSV1";
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;
  private static final int NUM_DOCS = 3_000_000;

  private static final String OOM_QUERY =
      "SELECT PERCENTILETDigest(doubleDimSV1, 50) AS digest, intDimSV1 FROM mytable GROUP BY intDimSV1"
          + " ORDER BY digest LIMIT 30000";

  private static final String OOM_QUERY_2 =
      "SELECT stringDimSV2 FROM mytable GROUP BY stringDimSV2"
          + " ORDER BY stringDimSV2 LIMIT 3000000";

  /// SafeTrim sort-aggregation case, pair-wise combine
  private static final String OOM_QUERY_3 =
      "SET sortAggregateSingleThreadedNumSegmentsThreshold=1;"
          + "SET sortAggregateLimitThreshold=3000001;"
          + " SELECT stringDimSV2 FROM mytable GROUP BY stringDimSV2"
          + " ORDER BY stringDimSV2 LIMIT 3000000";

  /// SafeTrim sort-aggregation case, sequential combine
  private static final String OOM_QUERY_4 =
      "SET sortAggregateSingleThreadedNumSegmentsThreshold=10000;"
          + "SET sortAggregateLimitThreshold=3000001;"
          + " SELECT stringDimSV2 FROM mytable GROUP BY stringDimSV2"
          + " ORDER BY stringDimSV2 LIMIT 3000000";

  private static final String DIGEST_QUERY_1 =
      "SELECT PERCENTILETDigest(doubleDimSV1, 50) AS digest FROM mytable";
  private static final String COUNT_STAR_QUERY =
      "SELECT * FROM mytable LIMIT 5";
  private static final String OOM_QUERY_SELECTION_ONLY = "SELECT * FROM mytable LIMIT 9000000";

  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(20, r -> {
    Thread thread = new Thread(r);
    thread.setDaemon(false);
    return thread;
  });

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  /**
   * Keeps track of metadata of queries that have been terminated due to OOM.
   * This is used to verify that the query was killed and the method used to kill the query.
   */
  public static class TestAccountantFactory extends PerQueryCPUMemAccountantFactory {
    @Override
    public PerQueryCPUMemResourceUsageAccountant init(PinotConfiguration config, String instanceId,
        InstanceType instanceType) {
      return new TestAccountant(config, instanceId, instanceType);
    }

    public static class TestAccountant extends PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant {
      private QueryResourceTracker _queryResourceTracker = null;
      private long _totalHeapMemoryUsage = 0L;
      private boolean _hasCallback = false;

      public TestAccountant(PinotConfiguration config, String instanceId, InstanceType instanceType) {
        super(config, instanceId, instanceType);
      }

      @Override
      public void logTerminatedQuery(QueryResourceTracker queryResourceTracker, long totalHeapMemoryUsage,
          boolean hasCallback) {
        super.logTerminatedQuery(queryResourceTracker, totalHeapMemoryUsage, hasCallback);
        _queryResourceTracker = queryResourceTracker;
        _totalHeapMemoryUsage = totalHeapMemoryUsage;
        _hasCallback = hasCallback;
      }

      public void reset() {
        _queryResourceTracker = null;
        _totalHeapMemoryUsage = 0L;
        _hasCallback = false;
      }

      public QueryResourceTracker getQueryResourceTracker() {
        return _queryResourceTracker;
      }

      public long getTotalHeapMemoryUsage() {
        return _totalHeapMemoryUsage;
      }

      public boolean hasCallback() {
        return _hasCallback;
      }
    }
  }

  private TestAccountantFactory.TestAccountant _testAccountant = null;

  @BeforeClass
  public void setUp()
      throws Exception {
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);

    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startServers();
    TestUtils.waitForCondition(aVoid -> Tracing.isAccountantRegistered(), 100L, 60_000L,
        "Waiting for accountant to be registered");
    _testAccountant = (TestAccountantFactory.TestAccountant) Tracing.getThreadAccountant();
    startBrokers();


    // Create and upload the schema and table config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_SCHEMA_NAME)
        .addSingleValueDimension(STRING_DIM_SV1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_DIM_SV2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(INT_DIM_SV1, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_DIM_SV1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DOUBLE_DIM_SV1, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(BOOLEAN_DIM_SV1, FieldSpec.DataType.BOOLEAN)
        .build();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    List<File> avroFiles = createAvroFile();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    //Wait for all documents loaded
    waitForAllDocsLoaded(10_000L);
  }

  protected void startBrokers()
      throws Exception {
    startBrokers(getNumBrokers());
  }

  protected void startServers()
      throws Exception {
    startServers(getNumServers());
  }

  @AfterMethod
  public void resetTestAccountant() {
    if (_testAccountant != null) {
      _testAccountant.reset();
    }
  }

  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.0f);
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.15f);
    serverConf.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        TestAccountantFactory.class.getName());
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
            + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.0f);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.60f);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO, 1.1f);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
            + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        TestAccountantFactory.class.getName());
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }


  protected long getCountStarResult() {
    return NUM_DOCS * 3;
  }

  protected String getTimeColumnName() {
    return null;
  }

  protected TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName()).setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig()).setNullHandlingEnabled(getNullHandlingEnabled())
        .setSegmentPartitionConfig(getSegmentPartitionConfig())
        .build();
  }

  @Test
  public void testDigestOOM()
      throws Exception {
    JsonNode queryResponse = postQuery(OOM_QUERY);
    String exceptions = queryResponse.get("exceptions").toString();
    assertTrue(exceptions.contains("\"errorCode\":" + QueryErrorCode.QUERY_CANCELLATION.getId()), exceptions);
    assertTrue(exceptions.contains("got killed on SERVER"), exceptions);
  }

  @Test(enabled = false) // Disabled because this test is flaky in the multi-stage query engine
  public void testDigestOOMMSE()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    JsonNode queryResponse = postQuery(OOM_QUERY);
    String exceptionsNode = queryResponse.get("exceptions").toString();
    assertTrue(exceptionsNode.contains("\"errorCode\":" + QueryErrorCode.INTERNAL.getId()), exceptionsNode);
    assertTrue(exceptionsNode.contains("Received 1 error"), exceptionsNode);
    assertTrue(_testAccountant.hasCallback());
    assertEquals(queryResponse.get("requestId").asText(), _testAccountant.getQueryResourceTracker().getQueryId());
  }

  @Test
  public void testMemoryAllocationStats()
      throws Exception {
    JsonNode queryResponse = postQuery(COUNT_STAR_QUERY);
    long offlineThreadMemAllocatedBytes = queryResponse.get("offlineThreadMemAllocatedBytes").asLong();
    long offlineResponseSerMemAllocatedBytes = queryResponse.get("offlineResponseSerMemAllocatedBytes").asLong();
    long offlineTotalMemAllocatedBytes = queryResponse.get("offlineTotalMemAllocatedBytes").asLong();

    assertTrue(offlineThreadMemAllocatedBytes > 0);
    assertTrue(offlineResponseSerMemAllocatedBytes > 0);
    assertEquals(offlineThreadMemAllocatedBytes + offlineResponseSerMemAllocatedBytes, offlineTotalMemAllocatedBytes);
  }

  @Test
  public void testSelectionOnlyOOM()
      throws Exception {
    JsonNode queryResponse = postQuery(OOM_QUERY_SELECTION_ONLY);
    String exceptions = queryResponse.get("exceptions").toString();
    assertTrue(exceptions.contains("\"errorCode\":" + QueryErrorCode.QUERY_CANCELLATION.getId()), exceptions);
    assertTrue(exceptions.contains("got killed on SERVER"), exceptions);
  }

  @Test(enabled = false) // Disabled because this test is flaky in the multi-stage query engine
  public void testSelectionOnlyOOMMSE()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    JsonNode queryResponse = postQuery(OOM_QUERY_SELECTION_ONLY);

    String exceptionsNode = queryResponse.get("exceptions").toString();
    assertTrue(exceptionsNode.contains("\"errorCode\":" + QueryErrorCode.INTERNAL.getId()), exceptionsNode);
    assertTrue(exceptionsNode.contains("Received 1 error"), exceptionsNode);
    assertTrue(_testAccountant.hasCallback());
    assertEquals(queryResponse.get("requestId").asText(), _testAccountant.getQueryResourceTracker().getQueryId());
  }

  @Test
  public void testDigestOOM2()
      throws Exception {
    JsonNode queryResponse = postQuery(OOM_QUERY_2);
    String exceptions = queryResponse.get("exceptions").toString();
    assertTrue(exceptions.contains("\"errorCode\":" + QueryErrorCode.QUERY_CANCELLATION.getId()), exceptions);
    assertTrue(exceptions.contains("got killed on SERVER"), exceptions);
  }

  @Test(enabled = false) // Disabled because this test is flaky in the multi-stage query engine
  public void testDigestOOM2MSE()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    JsonNode queryResponse = postQuery(OOM_QUERY_2);
    String exceptionsNode = queryResponse.get("exceptions").toString();
    assertTrue(exceptionsNode.contains("\"errorCode\":" + QueryErrorCode.INTERNAL.getId()), exceptionsNode);
    assertTrue(exceptionsNode.contains("Received 1 error"), exceptionsNode);
    assertTrue(_testAccountant.hasCallback());
    assertEquals(queryResponse.get("requestId").asText(), _testAccountant.getQueryResourceTracker().getQueryId());
  }

  /// SafeTrim sort-aggregation case, pair-wise combine
  @Test
  public void testDigestOOM3()
      throws Exception {
    JsonNode queryResponse = postQuery(OOM_QUERY_3);
    String exceptions = queryResponse.get("exceptions").toString();
    assertTrue(exceptions.contains("\"errorCode\":" + QueryErrorCode.QUERY_CANCELLATION.getId()), exceptions);
    assertTrue(exceptions.contains("got killed on SERVER"), exceptions);
  }

  @Test
  public void testDigestOOM3MSE()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    JsonNode queryResponse = postQuery(OOM_QUERY_3);
    String exceptionsNode = queryResponse.get("exceptions").toString();
    assertTrue(exceptionsNode.contains("\"errorCode\":" + QueryErrorCode.INTERNAL.getId()), exceptionsNode);
    assertTrue(exceptionsNode.contains("Received 1 error from stage 1"), exceptionsNode);
    assertTrue(_testAccountant.hasCallback());
    assertEquals(queryResponse.get("requestId").asText(), _testAccountant.getQueryResourceTracker().getQueryId());
  }

  /// SafeTrim sort-aggregation case, sequential combine
  @Test
  public void testDigestOOM4()
      throws Exception {
    JsonNode queryResponse = postQuery(OOM_QUERY_4);
    String exceptions = queryResponse.get("exceptions").toString();
    assertTrue(exceptions.contains("\"errorCode\":" + QueryErrorCode.QUERY_CANCELLATION.getId()), exceptions);
    assertTrue(exceptions.contains("got killed on SERVER"), exceptions);
  }

  @Test
  public void testDigestOOM4MSE()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    JsonNode queryResponse = postQuery(OOM_QUERY_4);
    String exceptionsNode = queryResponse.get("exceptions").toString();
    assertTrue(exceptionsNode.contains("\"errorCode\":" + QueryErrorCode.INTERNAL.getId()), exceptionsNode);
    assertTrue(exceptionsNode.contains("Received 1 error from stage 1"), exceptionsNode);
    assertTrue(_testAccountant.hasCallback());
    assertEquals(queryResponse.get("requestId").asText(), _testAccountant.getQueryResourceTracker().getQueryId());
  }

  @Test
  public void testDigestOOMMultipleQueries()
      throws Exception {
    AtomicReference<JsonNode> queryResponse1 = new AtomicReference<>();
    AtomicReference<JsonNode> queryResponse2 = new AtomicReference<>();
    AtomicReference<JsonNode> queryResponse3 = new AtomicReference<>();

    CountDownLatch countDownLatch = new CountDownLatch(3);

    EXECUTOR_SERVICE.submit(
        () -> {
          try {
            queryResponse1.set(postQuery(OOM_QUERY));
            countDownLatch.countDown();
          } catch (Exception ignored) {
          }
        }
    );
    EXECUTOR_SERVICE.submit(
        () -> {
          try {
            queryResponse2.set(postQuery(DIGEST_QUERY_1));
            countDownLatch.countDown();
          } catch (Exception ignored) {
          }
        }
    );
    EXECUTOR_SERVICE.submit(
        () -> {
          try {
            queryResponse3.set(postQuery(COUNT_STAR_QUERY));
            countDownLatch.countDown();
          } catch (Exception ignored) {
          }
        }
    );
    countDownLatch.await();
    String exceptionsNode = queryResponse1.get().get("exceptions").toString();
    assertTrue(exceptionsNode.contains("\"errorCode\":503"), exceptionsNode);
    assertTrue(exceptionsNode.contains("got killed"), exceptionsNode);
    assertFalse(StringUtils.isEmpty(queryResponse2.get().get("exceptions").toString()), exceptionsNode);
    assertFalse(StringUtils.isEmpty(queryResponse3.get().get("exceptions").toString()), exceptionsNode);
  }

  private List<File> createAvroFile()
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(STRING_DIM_SV1,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(STRING_DIM_SV2,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(INT_DIM_SV1,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(LONG_DIM_SV1,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null),
        new org.apache.avro.Schema.Field(DOUBLE_DIM_SV1,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(BOOLEAN_DIM_SV1,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN), null, null)));

    List<File> ret = new ArrayList<>();
    for (int file = 0; file < 3; file++) {
      // create avro file
      File avroFile = new File(_tempDir, "data_" + file + ".avro");
      try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);

        int numDocs = NUM_DOCS;
        int randBound = numDocs / 2;
        Random random = new Random(0);
        IntStream randomInt = random.ints(0, 100_000);
        for (int docId = 0; docId < numDocs; docId++) {
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
