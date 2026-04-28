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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration tests for {@link PinotLLCRealtimeSegmentManager}.
 */
public class PinotLLCRealtimeSegmentManagerIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME = "pinot_llc_realtime_segment_manager";
  private static final String SHARED_KAFKA_TOPIC = "pinot_llc_realtime_segment_manager";
  private static final String MAX_SEGMENT_COMPLETION_TIME_CONFIG_KEY =
      PinotLLCRealtimeSegmentManager.MAX_SEGMENT_COMPLETION_TIME_MILLIS_KEY;

  private final String _resourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);

  private File _classTempDir;
  private String _previousMaxSegmentCompletionTimeMillis;
  private boolean _maxSegmentCompletionTimeConfigRemembered;

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
  }

  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    if (isSharedRichClusterEnabled()) {
      TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir);
    } else {
      TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    }

    startZk();
    startKafka();
    startController();
    rememberMaxSegmentCompletionTimeClusterConfig();
    deleteMaxSegmentCompletionTimeClusterConfigIfPresent();
    waitForMaxSegmentCompletionTime(
        PinotLLCRealtimeSegmentManager.DEFAULT_MAX_SEGMENT_COMPLETION_TIME_MILLIS,
        "Max segment completion time was not reset to default before test setup");
    startBroker();
    startServer();

    cleanRealtimeTableAndSchema();
    resetKafkaTopic();

    List<File> avroFiles = unpackAvroData(_classTempDir);
    pushAvroIntoKafka(avroFiles);

    Schema schema = createSchema();
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);

    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanRealtimeTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::restoreMaxSegmentCompletionTimeClusterConfig);
    if (!isSharedRichClusterEnabled()) {
      exception = runCleanup(exception, this::stopServerIfStarted);
      exception = runCleanup(exception, this::stopBrokerIfStarted);
      exception = runCleanup(exception, this::stopControllerIfStarted);
      exception = runCleanup(exception, this::stopKafkaIfStarted);
      exception = runCleanup(exception, this::stopZk);
    }
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME + "_" + _resourceSuffix : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC + "_" + _resourceSuffix : super.getKafkaTopic();
  }

  @Test
  public void testMaxSegmentCompletionTimeClusterConfigChange()
      throws Exception {
    PinotLLCRealtimeSegmentManager segmentManager = _helixResourceManager.getRealtimeSegmentManager();

    // Verify default value
    assertEquals(segmentManager.getMaxSegmentCompletionTimeMillis(),
        PinotLLCRealtimeSegmentManager.DEFAULT_MAX_SEGMENT_COMPLETION_TIME_MILLIS);

    // Update via cluster config and verify propagation through ZK change listener
    updateClusterConfig(Map.of(MAX_SEGMENT_COMPLETION_TIME_CONFIG_KEY, "600000"));
    TestUtils.waitForCondition(
        aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis() == 600_000L,
        1000, 10000,
        "Max segment completion time was not updated to 600000ms");

    // Update to another value
    updateClusterConfig(Map.of(MAX_SEGMENT_COMPLETION_TIME_CONFIG_KEY, "900000"));
    TestUtils.waitForCondition(
        aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis() == 900_000L,
        1000, 10000,
        "Max segment completion time was not updated to 900000ms");

    // Delete the config and verify revert to default
    deleteClusterConfig(MAX_SEGMENT_COMPLETION_TIME_CONFIG_KEY);
    TestUtils.waitForCondition(
        aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis()
            == PinotLLCRealtimeSegmentManager.DEFAULT_MAX_SEGMENT_COMPLETION_TIME_MILLIS,
        1000, 10000,
        "Max segment completion time was not reverted to default after config deletion");
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + _resourceSuffix)
        : _tempDir;
  }

  private void cleanRealtimeTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
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

  private void rememberMaxSegmentCompletionTimeClusterConfig() {
    if (_maxSegmentCompletionTimeConfigRemembered) {
      return;
    }
    _previousMaxSegmentCompletionTimeMillis = getClusterConfig(MAX_SEGMENT_COMPLETION_TIME_CONFIG_KEY);
    _maxSegmentCompletionTimeConfigRemembered = true;
  }

  private void restoreMaxSegmentCompletionTimeClusterConfig()
      throws Exception {
    if (!_maxSegmentCompletionTimeConfigRemembered) {
      return;
    }
    if (_previousMaxSegmentCompletionTimeMillis == null) {
      deleteMaxSegmentCompletionTimeClusterConfigIfPresent();
    } else {
      updateClusterConfig(Map.of(MAX_SEGMENT_COMPLETION_TIME_CONFIG_KEY, _previousMaxSegmentCompletionTimeMillis));
    }
    waitForMaxSegmentCompletionTime(
        _previousMaxSegmentCompletionTimeMillis == null
            ? PinotLLCRealtimeSegmentManager.DEFAULT_MAX_SEGMENT_COMPLETION_TIME_MILLIS
            : parseMaxSegmentCompletionTimeMillis(_previousMaxSegmentCompletionTimeMillis),
        "Max segment completion time was not restored after test");
    _maxSegmentCompletionTimeConfigRemembered = false;
  }

  private void deleteMaxSegmentCompletionTimeClusterConfigIfPresent()
      throws Exception {
    if (getClusterConfig(MAX_SEGMENT_COMPLETION_TIME_CONFIG_KEY) != null) {
      deleteClusterConfig(MAX_SEGMENT_COMPLETION_TIME_CONFIG_KEY);
    }
  }

  private String getClusterConfig(String key) {
    return _helixManager.getConfigAccessor().get(getClusterConfigScope(), key);
  }

  private long parseMaxSegmentCompletionTimeMillis(String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return PinotLLCRealtimeSegmentManager.DEFAULT_MAX_SEGMENT_COMPLETION_TIME_MILLIS;
    }
  }

  private HelixConfigScope getClusterConfigScope() {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
        .build();
  }

  private void waitForMaxSegmentCompletionTime(long expectedValue, String errorMessage) {
    PinotLLCRealtimeSegmentManager segmentManager = _helixResourceManager.getRealtimeSegmentManager();
    TestUtils.waitForCondition(
        aVoid -> segmentManager.getMaxSegmentCompletionTimeMillis() == expectedValue,
        1000, 10000, errorMessage);
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

  private void deleteClassTempDir() {
    if (_classTempDir != null) {
      FileUtils.deleteQuietly(_classTempDir);
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
}
