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
import java.io.IOException;
import java.util.Map;
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


@Test(groups = {"integration-suite-2"})
public class SegmentCompletionIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_KAFKA_PARTITIONS = 1;

  private String _serverInstance;
  private HelixManager _serverHelixManager;
  private String _currentSegment;

  @Override
  protected int getNumKafkaPartitions() {
    return NUM_KAFKA_PARTITIONS;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startFakeServer();

    // Start Kafka
    startKafka();

    // Create and upload the schema and table config
    addSchema(createSchema());
    addTableConfig(createRealtimeTableConfig(null));
  }

  /**
   * Helper method to start a fake server that only implements Helix part.
   *
   * @throws Exception
   */
  private void startFakeServer()
      throws Exception {
    _serverInstance = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + NetUtils.getHostAddress() + "_"
        + CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;

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

  @AfterClass
  public void tearDown()
      throws IOException {
    dropRealtimeTable(getTableName());
    stopFakeServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
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
