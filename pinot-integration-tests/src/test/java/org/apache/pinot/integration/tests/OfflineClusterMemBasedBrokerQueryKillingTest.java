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
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactoryForTest;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for heap size based broker query killing, this works only for xmx4G
 */
public class OfflineClusterMemBasedBrokerQueryKillingTest extends BaseClusterIntegrationTestSet {

  public static final String STRING_DIM_SV1 = "stringDimSV1";
  public static final String STRING_DIM_SV2 = "stringDimSV2";
  public static final String INT_DIM_SV1 = "intDimSV1";
  public static final String LONG_DIM_SV1 = "longDimSV1";
  public static final String DOUBLE_DIM_SV1 = "doubleDimSV1";
  public static final String BOOLEAN_DIM_SV1 = "booleanDimSV1";
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 3;
  private static final String OOM_QUERY =
      "SELECT PERCENTILETDigest(doubleDimSV1, 50) AS digest, intDimSV1 FROM mytable WHERE intDimSV1 > 450000 GROUP BY"
          + " intDimSV1 ORDER BY digest LIMIT 15000";

  private static final String DIGEST_QUERY_1 =
      "SELECT PERCENTILETDigest(doubleDimSV1, 50) AS digest FROM mytable";
  private static final String COUNT_STAR_QUERY =
      "SELECT * FROM mytable LIMIT 5";

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

  @BeforeClass
  public void setUp()
      throws Exception {
    LogManager.getLogger(PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant.class)
        .setLevel(Level.ERROR);
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);

    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers();
    while (!Tracing.isAccountantRegistered()) {
      Thread.sleep(100L);
    }
    startServers();

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

    // Setup logging and resource accounting
    LogManager.getLogger(OfflineClusterMemBasedBrokerQueryKillingTest.class).setLevel(Level.INFO);
    LogManager.getLogger(PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant.class)
        .setLevel(Level.INFO);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.INFO);
    LogManager.getLogger(Tracing.class).setLevel(Level.INFO);
  }

  protected void startBrokers()
      throws Exception {
    startBrokers(getNumBrokers());
  }

  protected void startServers()
      throws Exception {
    startServers(getNumServers());
  }

  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.0f);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.40f);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO, 0.0025f);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_INSTANCE_TYPE, InstanceType.BROKER);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO, 1.1f);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
            + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        PerQueryCPUMemAccountantFactoryForTest.class.getCanonicalName());
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.0f);
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.60f);
    serverConf.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        PerQueryCPUMemAccountantFactoryForTest.class.getCanonicalName());
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
            + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  protected long getCountStarResult() {
    return 3_000_000;
  }

  protected String getTimeColumnName() {
    return null;
  }

  protected TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).setSchemaName(getSchemaName())
        .setTimeColumnName(getTimeColumnName()).setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig()).setNullHandlingEnabled(getNullHandlingEnabled())
        .setSegmentPartitionConfig(getSegmentPartitionConfig())
        .build();
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
    Assert.assertTrue(queryResponse1.get().get("exceptions").toString().contains(
        "Interrupted in broker reduce phase"));
    Assert.assertTrue(queryResponse1.get().get("exceptions").toString().contains("\"errorCode\":"
        + QueryException.QUERY_CANCELLATION_ERROR_CODE));
    Assert.assertTrue(queryResponse1.get().get("exceptions").toString().contains("got killed because"));
    Assert.assertFalse(StringUtils.isEmpty(queryResponse2.get().get("exceptions").toString()));
    Assert.assertFalse(StringUtils.isEmpty(queryResponse3.get().get("exceptions").toString()));
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

        int numDocs = 1_000_000;
        int randBound = numDocs / 2;
        Random random = new Random(0);
        IntStream randomInt = random.ints(0, 100_000);
        for (int docId = 0; docId < numDocs; docId++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(STRING_DIM_SV1, "test query killing");
          record.put(STRING_DIM_SV2, "test query killing");
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
