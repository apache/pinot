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
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager;
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
  private static final String SHARED_TABLE_NAME = "realtime_consumption_rate_limiter";
  private static final String SHARED_KAFKA_TOPIC = "realtime_consumption_rate_limiter";
  private static final String DEFAULT_EXTRA_TABLE_NAME_1 = "testTable1";
  private static final String DEFAULT_EXTRA_TABLE_NAME_2 = "testTable2";
  private static final String SHARED_EXTRA_TABLE_NAME_1 = "realtime_consumption_rate_limiter_1";
  private static final String SHARED_EXTRA_TABLE_NAME_2 = "realtime_consumption_rate_limiter_2";
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final double SERVER_RATE_LIMIT = 100;

  private final boolean _isDirectAlloc = RANDOM.nextBoolean();
  private final boolean _isConsumerDirConfigured = RANDOM.nextBoolean();
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();
  private File _classTempDir;
  private List<File> _avroFiles;
  private String _previousServerConsumptionRateLimit;
  private String _previousServerConsumptionByteRateLimit;
  private boolean _serverConsumptionRateLimitApplied;

  @Override
  protected String getLoadMode() {
    return ReadMode.mmap.name();
  }

  @Override
  public void startController()
      throws Exception {
    super.startController();
    if (!isSharedRichClusterEnabled()) {
      enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
    }
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, _isDirectAlloc);
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, getConsumerDirectory());
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

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC : super.getKafkaTopic();
  }

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    // Remove the consumer directory
    FileUtils.deleteQuietly(getConsumerDirectoryFile());

    _classTempDir = getClassTempDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafkaWithoutTopic();

    if (isSharedRichClusterEnabled()) {
      applyServerConsumptionRateLimit();
    }

    cleanRealtimeTablesAndSchemas();
    resetKafkaTopic();

    // Unpack the Avro files
    _avroFiles = unpackAvroData(_classTempDir);

    // Push data into Kafka
    pushAvroIntoKafka(_avroFiles);
  }

  @AfterClass(alwaysRun = true)
  @Override
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanRealtimeTablesAndSchemas);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::restoreServerConsumptionRateLimit);
    exception = runCleanup(exception, () -> FileUtils.deleteDirectory(getConsumerDirectoryFile()));
    exception = runCleanup(exception, this::stopServerIfStarted);
    exception = runCleanup(exception, this::stopBrokerIfStarted);
    exception = runCleanup(exception, this::stopControllerIfStarted);
    exception = runCleanup(exception, this::stopKafkaIfStarted);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
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
        LOGGER.info("Second = {}, realtimeRowConsumedMeter = {}, currentCount = {}, currentRate = {}", i,
            realtimeRowConsumedMeter.oneMinuteRate(), currentCount, currentRate);
        Assert.assertTrue(realtimeRowConsumedMeter.oneMinuteRate() < SERVER_RATE_LIMIT,
            "Rate should be less than " + SERVER_RATE_LIMIT);
        Assert.assertTrue(currentRate < SERVER_RATE_LIMIT * 1.5, // Put some leeway for the rate calculation
            "Rate should be less than " + SERVER_RATE_LIMIT);
      }
    } finally {
      cleanRealtimeTableAndSchema(tableName);
    }
  }

  @Test
  public void testTwoTableRateLimit()
      throws Exception {
    String tableName1 = getExtraTableName1();
    String tableName2 = getExtraTableName2();

    try {
      // Create and upload the schema and table config
      Schema schema1 = createSchema();
      schema1.setSchemaName(tableName1);
      addSchema(schema1);
      Schema schema2 = createSchema();
      schema2.setSchemaName(tableName2);
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
        LOGGER.info(
            "Second = {}, serverRowConsumedMeter = {}, currentCount1 = {}, currentRate1 = {}, currentCount2 = {}, "
                + "currentRate2 = {}, currentServerCount = {}, currentServerRate = {}",
            i, serverRowConsumedMeter.oneMinuteRate(), currentCount1, currentRate1, currentCount2, currentRate2,
            currentServerCount, currentServerRate);

        Assert.assertTrue(serverRowConsumedMeter.oneMinuteRate() < SERVER_RATE_LIMIT,
            "Rate should be less than " + SERVER_RATE_LIMIT + ", serverOneMinuteRate = " + serverRowConsumedMeter
                .oneMinuteRate());
        Assert.assertTrue(currentServerRate < SERVER_RATE_LIMIT * 1.5,
            // Put some leeway for the rate calculation
            "Whole table ingestion rate should be less than " + SERVER_RATE_LIMIT + ", currentRate1 = " + currentRate1
                + ", currentRate2 = " + currentRate2 + ", currentServerRate = " + currentServerRate);
      }
    } finally {
      cleanRealtimeTablesAndSchemas(tableName1, tableName2);
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

  private String getExtraTableName1() {
    return isSharedRichClusterEnabled() ? SHARED_EXTRA_TABLE_NAME_1 : DEFAULT_EXTRA_TABLE_NAME_1;
  }

  private String getExtraTableName2() {
    return isSharedRichClusterEnabled() ? SHARED_EXTRA_TABLE_NAME_2 : DEFAULT_EXTRA_TABLE_NAME_2;
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-shared")
        : _tempDir;
  }

  private String getConsumerDirectory() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-consumer").getAbsolutePath()
        : CONSUMER_DIRECTORY;
  }

  private File getConsumerDirectoryFile() {
    return new File(getConsumerDirectory());
  }

  private void cleanRealtimeTablesAndSchemas()
      throws Exception {
    cleanRealtimeTablesAndSchemas(getTableName(), getExtraTableName1(), getExtraTableName2());
  }

  private void cleanRealtimeTablesAndSchemas(String... tableNames)
      throws Exception {
    Exception exception = null;
    for (String tableName : tableNames) {
      exception = runCleanup(exception, () -> cleanRealtimeTableAndSchema(tableName));
    }
    if (exception != null) {
      throw exception;
    }
  }

  private void cleanRealtimeTableAndSchema(String tableName)
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(tableName)) {
      dropRealtimeTable(tableName);
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void resetKafkaTopic() {
    deleteKafkaTopicIfPresent();
    createKafkaTopic(getKafkaTopic());
  }

  private void deleteKafkaTopicIfPresent() {
    if (isKafkaTopicPresent()) {
      deleteKafkaTopic(getKafkaTopic());
    }
  }

  private boolean isKafkaTopicPresent() {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return false;
    }
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(getKafkaTopic());
    } catch (Exception e) {
      return false;
    }
  }

  private void applyServerConsumptionRateLimit()
      throws Exception {
    if (_serverConsumptionRateLimitApplied) {
      return;
    }

    _previousServerConsumptionRateLimit =
        getClusterConfig(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT);
    _previousServerConsumptionByteRateLimit =
        getClusterConfig(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES);
    _serverConsumptionRateLimitApplied = true;
    if (_previousServerConsumptionByteRateLimit != null) {
      deleteClusterConfig(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES);
    }
    updateClusterConfig(
        Map.of(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT, Double.toString(SERVER_RATE_LIMIT)));
    updateServerRateLimiter(SERVER_RATE_LIMIT, 0.0);
  }

  private void restoreServerConsumptionRateLimit()
      throws Exception {
    if (!_serverConsumptionRateLimitApplied) {
      return;
    }

    restoreClusterConfig(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT,
        _previousServerConsumptionRateLimit);
    restoreClusterConfig(CommonConstants.Server.CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT_BYTES,
        _previousServerConsumptionByteRateLimit);
    updateServerRateLimiter(parseRateLimit(_previousServerConsumptionRateLimit),
        parseRateLimit(_previousServerConsumptionByteRateLimit));
    _serverConsumptionRateLimitApplied = false;
  }

  private String getClusterConfig(String key) {
    HelixConfigScope scope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
        .forCluster(getHelixClusterName()).build();
    return _helixManager.getConfigAccessor().get(scope, key);
  }

  private void restoreClusterConfig(String key, String value)
      throws Exception {
    if (value == null) {
      deleteClusterConfig(key);
    } else {
      updateClusterConfig(Map.of(key, value));
    }
  }

  private void updateServerRateLimiter(double messageRateLimit, double byteRateLimit) {
    RealtimeConsumptionRateManager.getInstance().updateServerRateLimiter(
        RealtimeConsumptionRateManager.resolveServerRateLimit(messageRateLimit, byteRateLimit), ServerMetrics.get());
  }

  private double parseRateLimit(String rateLimit) {
    if (rateLimit == null) {
      return CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT;
    }
    try {
      return Double.parseDouble(rateLimit);
    } catch (NumberFormatException e) {
      return CommonConstants.Server.DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT;
    }
  }

  private void stopServerIfStarted() {
    if (!_serverStarters.isEmpty()) {
      stopServer();
    }
  }

  private void stopBrokerIfStarted() {
    if (!_brokerStarters.isEmpty()) {
      stopBroker();
    }
  }

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void stopKafkaIfStarted() {
    if (_kafkaStarters != null && !_kafkaStarters.isEmpty()) {
      stopKafka();
    }
  }

  private void deleteClassTempDir()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
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
