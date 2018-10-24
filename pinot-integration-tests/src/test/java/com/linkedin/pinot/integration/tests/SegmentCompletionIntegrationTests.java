/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.google.common.base.Function;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.config.TagNameUtils;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.controller.validation.ValidationManager;
import com.linkedin.pinot.server.realtime.ControllerLeaderLocator;
import com.linkedin.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import com.linkedin.pinot.server.starter.helix.SegmentOnlineOfflineStateModelFactory;
import com.linkedin.pinot.util.TestUtils;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.Map;
import javax.annotation.Nullable;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SegmentCompletionIntegrationTests extends LLCRealtimeClusterIntegrationTest {
  private static final int NUM_KAFKA_PARTITIONS = 1;

  private String _serverInstance;
  private HelixManager _serverHelixManager;
  private String _currentSegment;

  @Override
  protected int getNumKafkaPartitions() {
    return NUM_KAFKA_PARTITIONS;
  }

  @BeforeClass
  public void setUp() throws Exception {
    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startFakeServer();

    // Start Kafka
    startKafka();

    // Create Pinot table
    setUpTable(null);
  }

  /**
   * Helper method to start a fake server that only implements Helix part.
   *
   * @throws Exception
   */
  private void startFakeServer() throws Exception {
    _serverInstance = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + NetUtil.getHostAddress() + "_"
        + CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

    // Create server instance with the fake server state model
    _serverHelixManager = HelixManagerFactory.getZKHelixManager(_clusterName, _serverInstance, InstanceType.PARTICIPANT,
        ZkStarter.DEFAULT_ZK_STR);
    _serverHelixManager.getStateMachineEngine()
        .registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(),
            new FakeServerSegmentStateModelFactory());
    _serverHelixManager.connect();

    // Add Helix tag to the server
    _serverHelixManager.getClusterManagmentTool()
        .addInstanceTag(_clusterName, _serverInstance,
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
  public void testStopConsumingAndAutoFix() throws Exception {
    final String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    // Check if segment get into CONSUMING state
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          ExternalView externalView = _helixAdmin.getResourceExternalView(_clusterName, realtimeTableName);
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
        new ServerSegmentCompletionProtocolHandler(new ServerMetrics(new MetricsRegistry()));
    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withOffset(45688L).withSegmentName(_currentSegment).withReason("RandomReason").withInstanceId(_serverInstance);
    SegmentCompletionProtocol.Response response = protocolHandler.segmentStoppedConsuming(params);
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED);

    // Check if segment get into OFFLINE state
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          ExternalView externalView = _helixAdmin.getResourceExternalView(_clusterName, realtimeTableName);
          Map<String, String> stateMap = externalView.getStateMap(_currentSegment);
          return stateMap.get(_serverInstance).equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.OFFLINE_STATE);
        } catch (Exception e) {
          return null;
        }
      }
    }, 60_000L, "Failed to reach OFFLINE state");

    final String oldSegment = _currentSegment;

    // Now call the validation manager, and the segment should fix itself
    ValidationManager validationManager = _controllerStarter.getValidationManager();
    validationManager.init();
    validationManager.run();

    // Check if a new segment get into CONSUMING state
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          if (!_currentSegment.equals(oldSegment)) {
            ExternalView externalView = _helixAdmin.getResourceExternalView(_clusterName, realtimeTableName);
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

  @Test(enabled = false)
  @Override
  public void testQueriesFromQueryFile() throws Exception {
    // Skipped
  }

  @Test(enabled = false)
  @Override
  public void testGeneratedQueriesWithMultiValues() throws Exception {
    // Skipped
  }

  @Test(enabled = false)
  @Override
  public void testInstanceShutdown() throws Exception {
    // Skipped
  }

  @Test(enabled = false)
  @Override
  public void testSegmentFlushSize() throws Exception {
    // Skipped
  }

  @Test(enabled = false)
  @Override
  public void testDictionaryBasedQueries() throws Exception {
    // Skipped
  }

  @Test(enabled = false)
  @Override
  public void testQueryExceptions() throws Exception {
    // Skipped
  }

  @Test(enabled = false)
  public void testConsumerDirectoryExists() {
    // Skipped
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopFakeServer();
    stopBroker();
    stopController();
    stopZk();
  }

  private void stopFakeServer() {
    _serverHelixManager.disconnect();
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
