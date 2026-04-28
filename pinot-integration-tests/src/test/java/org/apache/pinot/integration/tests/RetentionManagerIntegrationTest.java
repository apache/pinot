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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RetentionManagerIntegrationTest extends SharedRichClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetentionManagerIntegrationTest.class);
  private static final String SHARED_TABLE_NAME_PREFIX = "retention_manager";
  private static final String SHARED_KAFKA_TOPIC_PREFIX = "retention-manager";
  private static final String ORPHAN_SEGMENT_PREFIX = "orphan_segment";
  private static final String ENABLE_UNTRACKED_SEGMENT_DELETION_CONFIG =
      ControllerConf.ControllerPeriodicTasksConf.ENABLE_UNTRACKED_SEGMENT_DELETION;

  private final String _sharedResourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);
  protected List<File> _avroFiles;
  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;
  private String _previousUntrackedSegmentDeletionConfig;
  private boolean _untrackedSegmentDeletionConfigCaptured;

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS, 5000);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    try {
      LOGGER.info("Set segment.store.uri: {} for server with scheme: {}", _controllerConfig.getDataDir(),
          new URI(_controllerConfig.getDataDir()).getScheme());
      serverConf.setProperty("pinot.server.instance.segment.store.uri", "file:" + _controllerConfig.getDataDir());
      serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
          "true");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME_PREFIX + "_" + _sharedResourceSuffix
        : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC_PREFIX + "-" + _sharedResourceSuffix
        : super.getKafkaTopic();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    startZk();
    if (isSharedRichClusterEnabled()) {
      startKafkaWithoutTopic();
    } else {
      startKafka();
    }
    startController();
    captureUntrackedSegmentDeletionConfig();
    startBroker();
    startServer();

    if (isSharedRichClusterEnabled()) {
      cleanRealtimeTableAndSchema();
      resetKafkaTopic();
      deleteFakeSegments();
    }
    setupTable();
    waitForAllDocsLoaded(600_000L);
  }

  protected void setupTable()
      throws Exception {
    _avroFiles = unpackAvroData(_classTempDir);
    pushAvroIntoKafka(_avroFiles);

    Schema schema = createSchema();
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("2");
    addTableConfig(tableConfig);

    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::deleteFakeSegments);
    exception = runCleanup(exception, this::cleanRealtimeTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::restoreUntrackedSegmentDeletionConfig);
    exception = runCleanup(exception, this::stopServerIfDirectMode);
    exception = runCleanup(exception, this::stopBrokerIfDirectMode);
    exception = runCleanup(exception, this::stopControllerIfDirectMode);
    exception = runCleanup(exception, this::stopKafkaIfDirectMode);
    exception = runCleanup(exception, this::stopZkIfDirectMode);
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Test
  public void testClusterConfigChangeListener()
      throws IOException, URISyntaxException {
    // Disable orphan segment deletion to ensure orphan segments are not cleaned up
    updateClusterConfig(Map.of(ENABLE_UNTRACKED_SEGMENT_DELETION_CONFIG, "false"));
    waitForUntrackedSegmentDeletionEnabled(false);

    createFakeSegments(_controllerConfig.getDataDir(), getTableName(), ORPHAN_SEGMENT_PREFIX, 4);

    _controllerStarter.getRetentionManager().run();

    PinotFS pinotFS = PinotFSFactory.create(URIUtils.getUri(_controllerConfig.getDataDir()).getScheme());

    // Verify that 6 segments remain: 2 CONSUMING segments + 4 orphan segments
    TestUtils.waitForCondition((aVoid) -> {
      try {
        String[] fileList = pinotFS.listFiles(new URI(_controllerConfig.getDataDir() + "/" + getTableName()), false);
        return fileList.length == 6;
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }, 5000, 10000, "Expected 6 segments (2 CONSUMING + 4 orphan) but found different count");

    // Enable orphan segment deletion
    updateClusterConfig(Map.of(ENABLE_UNTRACKED_SEGMENT_DELETION_CONFIG, "true"));

    // Wait for the config change to take effect
    waitForUntrackedSegmentDeletionEnabled(true);

    _controllerStarter.getRetentionManager().run();

    // Verify that only 2 CONSUMING segments remain after orphan segment cleanup
    TestUtils.waitForCondition((aVoid) -> {
      try {
        String[] fileList = pinotFS.listFiles(new URI(_controllerConfig.getDataDir() + "/" + getTableName()), false);
        return fileList.length == 2;
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }, 5000, 100000, "Expected 2 CONSUMING segments after orphan cleanup but found different count");
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + _sharedResourceSuffix)
        : _tempDir;
  }

  private void captureUntrackedSegmentDeletionConfig() {
    if (!_untrackedSegmentDeletionConfigCaptured) {
      _previousUntrackedSegmentDeletionConfig = getClusterConfig(ENABLE_UNTRACKED_SEGMENT_DELETION_CONFIG);
      _untrackedSegmentDeletionConfigCaptured = true;
    }
  }

  private String getClusterConfig(String key) {
    HelixConfigScope scope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
        .forCluster(getHelixClusterName()).build();
    return _helixManager.getConfigAccessor().get(scope, key);
  }

  private void restoreUntrackedSegmentDeletionConfig()
      throws Exception {
    if (!_untrackedSegmentDeletionConfigCaptured || _controllerStarter == null) {
      return;
    }

    if (_previousUntrackedSegmentDeletionConfig == null) {
      deleteClusterConfig(ENABLE_UNTRACKED_SEGMENT_DELETION_CONFIG);
      waitForUntrackedSegmentDeletionEnabled(
          ControllerConf.ControllerPeriodicTasksConf.DEFAULT_ENABLE_UNTRACKED_SEGMENT_DELETION);
    } else {
      updateClusterConfig(Map.of(ENABLE_UNTRACKED_SEGMENT_DELETION_CONFIG, _previousUntrackedSegmentDeletionConfig));
      if ("true".equalsIgnoreCase(_previousUntrackedSegmentDeletionConfig)
          || "false".equalsIgnoreCase(_previousUntrackedSegmentDeletionConfig)) {
        waitForUntrackedSegmentDeletionEnabled(Boolean.parseBoolean(_previousUntrackedSegmentDeletionConfig));
      }
    }
    _untrackedSegmentDeletionConfigCaptured = false;
  }

  private void waitForUntrackedSegmentDeletionEnabled(boolean expectedValue) {
    TestUtils.waitForCondition(
        (aVoid) -> _controllerStarter.getRetentionManager().isUntrackedSegmentDeletionEnabled() == expectedValue,
        1000, 10000, "UntrackedSegmentDeletionEnabled did not become " + expectedValue);
  }

  private void cleanRealtimeTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(getTableName())) {
      dropRealtimeTable(getTableName());
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }
    if (_helixResourceManager.getSchema(getTableName()) != null) {
      deleteSchema(getTableName());
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

  private void deleteFakeSegments()
      throws IOException {
    if (_controllerConfig == null) {
      return;
    }

    URI tableDataUri = URIUtils.getUri(_controllerConfig.getDataDir(), getTableName());
    PinotFS pinotFS = PinotFSFactory.create(tableDataUri.getScheme());
    String[] fileList;
    try {
      fileList = pinotFS.listFiles(tableDataUri, false);
    } catch (Exception e) {
      return;
    }
    for (String filePath : fileList) {
      String segmentName = URIUtils.getLastPart(filePath);
      if (segmentName.startsWith(ORPHAN_SEGMENT_PREFIX)) {
        pinotFS.delete(URIUtils.getUri(filePath), false);
      }
    }
  }

  private void stopServerIfDirectMode() {
    if (!isSharedRichClusterEnabled() && !_serverStarters.isEmpty()) {
      stopServer();
    }
  }

  private void stopBrokerIfDirectMode() {
    if (!isSharedRichClusterEnabled() && !_brokerStarters.isEmpty()) {
      stopBroker();
    }
  }

  private void stopControllerIfDirectMode() {
    if (!isSharedRichClusterEnabled() && _controllerStarter != null) {
      stopController();
    }
  }

  private void stopKafkaIfDirectMode() {
    if (!isSharedRichClusterEnabled() && _kafkaStarters != null && !_kafkaStarters.isEmpty()) {
      stopKafka();
    }
  }

  private void stopZkIfDirectMode() {
    if (!isSharedRichClusterEnabled()) {
      stopZk();
    }
  }

  private void deleteClassTempDir()
      throws IOException {
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

  private void createFakeSegments(String dataDir, String tableName, String orphanSegmentPrefix,
      int numberOfOrphanSegment)
      throws URISyntaxException, IOException {
    PinotFS pinotFS = PinotFSFactory.create(URIUtils.getUri(dataDir).getScheme());
    for (int i = 0; i < numberOfOrphanSegment; i++) {
      String segmentPath = dataDir + "/" + tableName + "/" + orphanSegmentPrefix + "_" + i;
      pinotFS.touch(new URI(segmentPath));
      File file = new File(segmentPath);
      if (!file.setLastModified(DateTime.now().minusDays(100).getMillis())) {
        LOGGER.warn("Failed to set last modified time for file: {}", segmentPath);
      }
    }
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
