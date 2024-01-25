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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RealtimeConsumptionRateLimiterClusterIntegrationTest extends BaseRealtimeClusterIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(RealtimeConsumptionRateLimiterClusterIntegrationTest.class);

  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final double SERVER_RATE_LIMIT = 100;

  private final boolean _isDirectAlloc = RANDOM.nextBoolean();
  private final boolean _isConsumerDirConfigured = RANDOM.nextBoolean();
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();
  private List<File> _avroFiles;

  @Override
  protected String getLoadMode() {
    return ReadMode.mmap.name();
  }

  @Override
  public void startController()
      throws Exception {
    super.startController();
    enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, _isDirectAlloc);
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT, SERVER_RATE_LIMIT);
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(Collections.singletonList(getStreamConfigMap())));
    return ingestionConfig;
  }

  @Override
  protected long getCountStarResult() {
    // all the data that was ingested from Kafka also got uploaded via the controller's upload endpoint
    return super.getCountStarResult() * 2;
  }

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    // Remove the consumer directory
    FileUtils.deleteQuietly(new File(CONSUMER_DIRECTORY));

    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    _avroFiles = unpackAvroData(_tempDir);

    // Push data into Kafka
    pushAvroIntoKafka(_avroFiles);
  }

  @AfterClass
  @Override
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(new File(CONSUMER_DIRECTORY));
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testOneTableRateLimit()
      throws Exception {
    String tableName = getTableName();
    try {
      // Create and upload the schema and table config
      Schema schema = createSchema();
      addSchema(schema);
      long startTime = System.currentTimeMillis();
      TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
      addTableConfig(tableConfig);
      for (int i = 0; i < 60; i++) {
        if (!isTableLoaded(tableName)) {
          Thread.sleep(1000L);
        } else {
          break;
        }
      }
      PinotMeter realtimeRowConsumedMeter = ServerMetrics.get().getMeteredValue(ServerMeter.REALTIME_ROWS_CONSUMED);
      long startCount = getCurrentCountStarResult(tableName);
      for (int i = 1; i <= 10; i++) {
        Thread.sleep(1000L);
        long currentCount = getCurrentCountStarResult(tableName);
        double currentRate = (currentCount - startCount) / (double) (System.currentTimeMillis() - startTime) * 1000;
        LOGGER.info("Second = " + i + ", realtimeRowConsumedMeter = " + realtimeRowConsumedMeter.oneMinuteRate()
            + ", currentCount = " + currentCount + ", currentRate = " + currentRate);
        Assert.assertTrue(realtimeRowConsumedMeter.oneMinuteRate() < SERVER_RATE_LIMIT,
            "Rate should be less than " + SERVER_RATE_LIMIT);
        Assert.assertTrue(currentRate < SERVER_RATE_LIMIT * 1.5, // Put some leeway for the rate calculation
            "Rate should be less than " + SERVER_RATE_LIMIT);
      }
    } finally {
      dropRealtimeTable(tableName);
      waitForTableDataManagerRemoved(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    }
  }

  @Test
  public void testTwoTableRateLimit()
      throws Exception {
    String tableName1 = "testTable1";
    String tableName2 = "testTable2";

    try {
      // Create and upload the schema and table config
      Schema schema1 = createSchema();
      schema1.setSchemaName("testTable1");
      addSchema(schema1);
      Schema schema2 = createSchema();
      schema2.setSchemaName("testTable2");
      addSchema(schema2);
      long startTime = System.currentTimeMillis();

      TableConfig tableConfig1 = createRealtimeTableConfig(tableName1);
      addTableConfig(tableConfig1);
      TableConfig tableConfig2 = createRealtimeTableConfig(tableName2);
      addTableConfig(tableConfig2);
      for (int i = 0; i < 60; i++) {
        if (!isTableLoaded(tableName1) || !isTableLoaded(tableName2)) {
          Thread.sleep(1000L);
        } else {
          break;
        }
      }

      PinotMeter serverRowConsumedMeter = ServerMetrics.get().getMeteredValue(ServerMeter.REALTIME_ROWS_CONSUMED);
      long startCount1 = getCurrentCountStarResult(tableName1);
      long startCount2 = getCurrentCountStarResult(tableName2);
      for (int i = 1; i <= 10; i++) {
        Thread.sleep(1000L);
        long currentCount1 = getCurrentCountStarResult(tableName1);
        long currentCount2 = getCurrentCountStarResult(tableName2);
        long currentServerCount = currentCount1 + currentCount2;
        long currentTimeMillis = System.currentTimeMillis();
        double currentRate1 = (currentCount1 - startCount1) / (double) (currentTimeMillis - startTime) * 1000;
        double currentRate2 = (currentCount2 - startCount2) / (double) (currentTimeMillis - startTime) * 1000;
        double currentServerRate = currentRate1 + currentRate2;
        LOGGER.info("Second = " + i + ", serverRowConsumedMeter = " + serverRowConsumedMeter.oneMinuteRate()
            + ", currentCount1 = " + currentCount1 + ", currentRate1 = " + currentRate1
            + ", currentCount2 = " + currentCount2 + ", currentRate2 = " + currentRate2
            + ", currentServerCount = " + currentServerCount + ", currentServerRate = " + currentServerRate
        );

        Assert.assertTrue(serverRowConsumedMeter.oneMinuteRate() < SERVER_RATE_LIMIT,
            "Rate should be less than " + SERVER_RATE_LIMIT + ", serverOneMinuteRate = " + serverRowConsumedMeter
                .oneMinuteRate());
        Assert.assertTrue(currentServerRate < SERVER_RATE_LIMIT * 1.5,
            // Put some leeway for the rate calculation
            "Whole table ingestion rate should be less than " + SERVER_RATE_LIMIT + ", currentRate1 = " + currentRate1
                + ", currentRate2 = " + currentRate2 + ", currentServerRate = " + currentServerRate);
      }
    } finally {
      dropRealtimeTable(tableName1);
      waitForTableDataManagerRemoved(TableNameBuilder.REALTIME.tableNameWithType(tableName1));
      dropRealtimeTable(tableName2);
      waitForTableDataManagerRemoved(TableNameBuilder.REALTIME.tableNameWithType(tableName2));
    }
  }

  protected TableConfig createRealtimeTableConfig() {
    return createRealtimeTableConfig(getTableName());
  }

  protected TableConfig createRealtimeTableConfig(String tableName) {
    return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName)
        .setTimeColumnName(getTimeColumnName()).setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns()).setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns()).setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs()).setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode()).setTaskConfig(getTaskConfig()).setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant()).setIngestionConfig(getIngestionConfig()).setQueryConfig(getQueryConfig())
        .setStreamConfigs(getStreamConfigs()).setNullHandlingEnabled(getNullHandlingEnabled()).build();
  }

  private boolean isTableLoaded(String tableName) {
    try {
      return getCurrentCountStarResult(tableName) > 0;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  protected Map<String, String> getStreamConfigs() {
    return null;
  }

  @Test(enabled = false)
  public void testDictionaryBasedQueries(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testGeneratedQueries(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testHardcodedQueries(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testInstanceShutdown() {
    // Do nothing
  }

  @Test(enabled = false)
  public void testQueriesFromQueryFile(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testQueryExceptions(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testHardcodedServerPartitionedSqlQueries() {
    // Do nothing
  }
}
