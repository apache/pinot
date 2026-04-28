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

import com.google.common.base.Function;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.server.starter.helix.SegmentOnlineOfflineStateModelFactory;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SegmentCompletionIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME_PREFIX = "segment_completion";
  private static final String SHARED_KAFKA_TOPIC_PREFIX = "segment-completion";
  private static final int NUM_KAFKA_PARTITIONS = 1;

  private final String _sharedResourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);

  private File _classTempDir;
  private String _serverInstance;
  private HelixManager _serverHelixManager;
  private volatile String _currentSegment;

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return true;
  }

  @Override
  protected int getSharedNumServers() {
    return 0;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
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

  @Override
  protected int getNumKafkaPartitions() {
    return NUM_KAFKA_PARTITIONS;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir);

    startZk();
    startController();
    startBroker();
    startFakeServer();
    startKafkaWithoutTopic();

    // Create and upload the schema and table config
    cleanRealtimeTableAndSchema();
    resetKafkaTopic();
    _currentSegment = null;
    addSchema(createSchema());
    addTableConfig(createRealtimeTableConfig(null));
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + _sharedResourceSuffix)
        : _tempDir;
  }

  /**
   * Helper method to start a fake server that only implements Helix part.
   *
   * @throws Exception
   */
  private void startFakeServer()
      throws Exception {
    int fakeServerPort = isSharedRichClusterEnabled() ? NetUtils.findOpenPort(_nextServerPort)
        : CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
    _nextServerPort = fakeServerPort + 1;
    _serverInstance =
        CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + NetUtils.getHostAddress() + "_" + fakeServerPort;

    // Create server instance with the fake server state model
    _serverHelixManager = HelixManagerFactory
        .getZKHelixManager(getHelixClusterName(), _serverInstance, InstanceType.PARTICIPANT, getZkUrl());
    _serverHelixManager.getStateMachineEngine()
        .registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(),
            new FakeServerSegmentStateModelFactory());
    _serverHelixManager.connect();

    // Add Helix tag to the server
    _serverHelixManager.getClusterManagmentTool().addInstanceTag(getHelixClusterName(), _serverInstance,
        TableNameBuilder.REALTIME.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));

    // Initialize controller leader locator
    ControllerLeaderLocator.create(_serverHelixManager);
  }

  /**
   * Test stop consuming and auto fix.
   *
   * @throws Exception
   */
  @Test
  public void testStopConsumingAndAutoFix()
      throws Exception {
    final String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    // Check if segment get into CONSUMING state
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          ExternalView externalView = _helixAdmin.getResourceExternalView(getHelixClusterName(), realtimeTableName);
          Map<String, String> stateMap = externalView.getStateMap(_currentSegment);
          return stateMap.get(_serverInstance)
              .equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE);
        } catch (Exception e) {
          return null;
        }
      }
    }, 60_000L, "Failed to reach CONSUMING state");

    // Now report to the controller that we had to stop consumption
    ServerSegmentCompletionProtocolHandler protocolHandler =
        new ServerSegmentCompletionProtocolHandler(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()),
            realtimeTableName);
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withStreamPartitionMsgOffset(new LongMsgOffset(45688L).toString()).withSegmentName(_currentSegment)
        .withReason("RandomReason").withInstanceId(_serverInstance);
    SegmentCompletionProtocol.Response response = protocolHandler.segmentStoppedConsuming(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED);

    // Check if segment get into OFFLINE state
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          ExternalView externalView = _helixAdmin.getResourceExternalView(getHelixClusterName(), realtimeTableName);
          Map<String, String> stateMap = externalView.getStateMap(_currentSegment);
          return stateMap.get(_serverInstance).equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.OFFLINE_STATE);
        } catch (Exception e) {
          return null;
        }
      }
    }, 60_000L, "Failed to reach OFFLINE state");

    final String oldSegment = _currentSegment;

    // Now call the validation manager, and the segment should fix itself
    RealtimeSegmentValidationManager validationManager = _controllerStarter.getRealtimeSegmentValidationManager();
    validationManager.start();
    validationManager.run();

    // Check if a new segment get into CONSUMING state
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          if (!_currentSegment.equals(oldSegment)) {
            ExternalView externalView = _helixAdmin.getResourceExternalView(getHelixClusterName(), realtimeTableName);
            Map<String, String> stateMap = externalView.getStateMap(_currentSegment);
            return
                stateMap.get(_serverInstance).equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE)
                    && (new LLCSegmentName(_currentSegment).getSequenceNumber())
                    == (new LLCSegmentName(oldSegment).getSequenceNumber()) + 1;
          }
          return false;
        } catch (Exception e) {
          return null;
        }
      }
    }, 60_000L, "Failed to get a new segment reaching CONSUMING state");
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanRealtimeTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::stopAndDropFakeServer);
    if (!isSharedRichClusterEnabled()) {
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

  private void stopAndDropFakeServer() {
    if (_serverHelixManager != null) {
      _serverHelixManager.disconnect();
      _serverHelixManager = null;
    }
    if (_serverInstance != null && _helixResourceManager != null && isFakeServerInstancePresent()) {
      TestUtils.waitForCondition(
          aVoid -> _helixResourceManager.dropInstance(_serverInstance).isSuccessful(),
          60_000L, "Failed to drop fake server instance: " + _serverInstance);
    }
    _serverInstance = null;
    _currentSegment = null;
  }

  private boolean isFakeServerInstancePresent() {
    try {
      return _helixAdmin != null && _helixAdmin.getInstancesInCluster(getHelixClusterName()).contains(_serverInstance);
    } catch (Exception e) {
      return false;
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

  private interface Cleanup {
    void run()
        throws Exception;
  }

  public class FakeServerSegmentStateModelFactory extends StateModelFactory<StateModel> {

    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      return new FakeSegmentStateModel();
    }

    @SuppressWarnings("unused")
    @StateModelInfo(states = "{'OFFLINE', 'ONLINE', 'CONSUMING', 'DROPPED'}", initialState = "OFFLINE")
    public class FakeSegmentStateModel extends StateModel {

      @Transition(from = "OFFLINE", to = "CONSUMING")
      public void onBecomeConsumingFromOffline(Message message, NotificationContext context) {
        _currentSegment = message.getPartitionName();
      }

      @Transition(from = "CONSUMING", to = "ONLINE")
      public void onBecomeOnlineFromConsuming(Message message, NotificationContext context) {
      }

      @Transition(from = "CONSUMING", to = "OFFLINE")
      public void onBecomeOfflineFromConsuming(Message message, NotificationContext context) {
      }

      @Transition(from = "OFFLINE", to = "ONLINE")
      public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      }

      @Transition(from = "OFFLINE", to = "DROPPED")
      public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      }

      @Transition(from = "ONLINE", to = "DROPPED")
      public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
      }

      @Transition(from = "ERROR", to = "OFFLINE")
      public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      }
    }
  }
}
