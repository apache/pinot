/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ControllerTenantNameBuilder;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.common.utils.ZkUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.server.realtime.ControllerLeaderLocator;
import com.linkedin.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import com.linkedin.pinot.server.starter.helix.SegmentOnlineOfflineStateModelFactory;


public class SegmentCompletionIntegrationTests extends RealtimeClusterIntegrationTest {
  private String _segmentName;
  private static long MAX_RUN_TIME_SECONDS = 60;
  private String _serverInstance;
  private HelixManager _helixManager;
  private final String _tableName = "mytable";
  private final String _clusterName = getHelixClusterName();
  private HelixAdmin _helixAdmin;

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startKafka();

    startController();
    startBroker();
    startFakeServer();

    addRealtimeTable();
  }

  @AfterClass
  public void tearDown() throws Exception {
    stopFakeServer();
    stopBroker();
    stopController();
    stopZk();
  }

  @Override
  protected void createKafkaTopic(String kafkaTopic, String zkStr) {
    KafkaStarterUtils.createTopic(kafkaTopic, zkStr, /*partitionCount=*/1);
  }

  @Override
  protected void setUpTable(String tableName, String timeColumnName, String timeColumnType, String kafkaZkUrl,
      String kafkaTopic, File schemaFile, File avroFile) throws Exception {
    Schema schema = Schema.fromFile(schemaFile);
    addSchema(schemaFile, schema.getSchemaName());
    // Call into the super class method
    addLLCRealtimeTable(tableName, timeColumnName, timeColumnType, -1, "", KafkaStarterUtils.DEFAULT_KAFKA_BROKER,
        kafkaTopic, schema.getSchemaName(), null, null, avroFile, ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH, "Carrier",
        Collections.<String>emptyList(), "mmap");
  }

  private void addRealtimeTable() throws Exception {
    File schemaFile = getSchemaFile();
    setUpTable(_tableName, "DaysSinceEpoch", "daysSinceEpoch", KafkaStarterUtils.DEFAULT_ZK_STR, KAFKA_TOPIC,
        schemaFile, null);
  }

  // Start a fake server that only implements helix part, does not consume any rows.
  private void startFakeServer() throws Exception {
    String hostName = NetUtil.getHostAddress();
    _serverInstance = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE  + hostName + "_" + CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
    _helixManager = HelixManagerFactory.getZKHelixManager(_clusterName, _serverInstance,
        InstanceType.PARTICIPANT, ZkStarter.DEFAULT_ZK_STR);
    final StateMachineEngine stateMachineEngine = _helixManager.getStateMachineEngine();
    _helixManager.connect();
    ZkHelixPropertyStore<ZNRecord> zkPropertyStore = ZkUtils.getZkPropertyStore(_helixManager, _clusterName);
    final StateModelFactory<?> stateModelFactory =
        new FakeServerSegmentStateModelFactory(_clusterName, _serverInstance, zkPropertyStore);
    stateMachineEngine.registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(),
        stateModelFactory);
    _helixAdmin = _helixManager.getClusterManagmentTool();

    _helixAdmin.addInstanceTag(_clusterName, _serverInstance,
        TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(ControllerTenantNameBuilder.DEFAULT_TENANT_NAME));
    ControllerLeaderLocator.create(_helixManager);
  }

  private void stopFakeServer() {
    _helixManager.disconnect();
  }

  // Test that if we send stoppedConsuming to the controller, the segment goes offline.
  @Test
  public void testStopConsumingToOfflineAndAutofix() throws  Exception {
    final String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(_tableName);
    long endTime = now() + MAX_RUN_TIME_SECONDS * 1000L;

    while (now() < endTime) {
      ExternalView ev = HelixHelper.getExternalViewForResource(_helixAdmin, _clusterName, realtimeTableName);
      if (ev != null) {
        Map<String, String> stateMap = ev.getStateMap(_segmentName);
        if (stateMap != null && stateMap.containsKey(_serverInstance)) {
          if (stateMap.get(_serverInstance).equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE)) {
            break;
          }
        }
      }
      Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
    }

    Assert.assertTrue(now() < endTime, "Failed trying to reach consuming state");

    // Now report to the controller that we had to stop consumption
    ServerSegmentCompletionProtocolHandler protocolHandler = new ServerSegmentCompletionProtocolHandler(_serverInstance);
    SegmentCompletionProtocol.Response response = protocolHandler.segmentStoppedConsuming(_segmentName, 45688L,
        "RandomReason");
    Assert.assertTrue(response.getStatus() == SegmentCompletionProtocol.ControllerResponseStatus.PROCESSED);

    while (now() < endTime) {
      ExternalView ev = HelixHelper.getExternalViewForResource(_helixAdmin, _clusterName, realtimeTableName);
      Map<String, String> stateMap = ev.getStateMap(_segmentName);
      if (stateMap.containsKey(_serverInstance)) {
        if (stateMap.get(_serverInstance).equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.OFFLINE_STATE)) {
          break;
        }
      }
      Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
    }

    Assert.assertTrue(now() < endTime, "Failed trying to reach offline state");

    // Now call the validation manager, and the segment should fix itself
    getControllerValidationManager().runValidation();

    // Now there should be a new segment in CONSUMING state in the IDEALSTATE.
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, realtimeTableName);
    Assert.assertEquals(idealState.getPartitionSet().size(), 2);
    for (String segmentId : idealState.getPartitionSet()) {
      if (!segmentId.equals(_segmentName)) {
        // This is a new segment. Verify that it is in CONSUMING state, and has a sequence number 1 more than prev one
        LLCSegmentName oldSegmentName = new LLCSegmentName(_segmentName);
        LLCSegmentName newSegmentName = new LLCSegmentName(segmentId);
        Assert.assertEquals(newSegmentName.getSequenceNumber(), oldSegmentName.getSequenceNumber() + 1);
        Map<String, String> instanceStateMap = idealState.getInstanceStateMap(segmentId);
        for (String state : instanceStateMap.values()) {
          Assert.assertTrue(state.equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE));
        }
      }
    }
    // We will assume that it eventually makes it to externalview
  }

  @Override
  public void testGeneratedQueriesWithMultiValues() throws Exception {
    // If we don't override this method to do nothing, we will starting running the super-class's method
    // with our fake server, which will not work
  }

  @Override
  public void testHardcodedQuerySet() throws Exception {
    // If we don't override this method to do nothing, we will starting running the super-class's method
    // with our fake server, which will not work
  }

  long now() {
    return System.currentTimeMillis();
  }

  public class FakeServerSegmentStateModelFactory extends StateModelFactory<StateModel> {
    private final String _helixClusterName;
    private final String _instanceId;
    public FakeServerSegmentStateModelFactory(String helixClusterName, String instanceId,
        ZkHelixPropertyStore<ZNRecord> zkPropertyStore) {
      _helixClusterName = helixClusterName;
      _instanceId = instanceId;
    }

    public StateModel createNewStateModel(String partitionName) {
      final FakeSegmentStateModel SegmentOnlineOfflineStateModel =
          new FakeSegmentStateModel(_helixClusterName, _instanceId);
      return SegmentOnlineOfflineStateModel;
    }

    @StateModelInfo(states = "{'OFFLINE','ONLINE', 'CONSUMING', 'DROPPED'}", initialState = "OFFLINE")
    public class FakeSegmentStateModel extends StateModel {
      public FakeSegmentStateModel(String helixClusterName, String instanceId) {
      }

      @Transition(from = "OFFLINE", to = "CONSUMING")
      public void onBecomeConsumingFromOffline(Message message, NotificationContext context) {
        final String realtimeTableName = message.getResourceName();
        final String segmentNameStr = message.getPartitionName();
        // We got the CONSUMING state transition, that is first success step.
        _segmentName = segmentNameStr;
      }

      @Transition(from = "CONSUMING", to = "ONLINE")
      public void onBecomeOnlineFromConsuming(Message message, NotificationContext context) {
        final String realtimeTableName = message.getResourceName();
        final String segmentNameStr = message.getPartitionName();
      }

      @Transition(from = "CONSUMING", to = "OFFLINE")
      public void onBecomeOfflineFromConsuming(Message message, NotificationContext context) {
        final String realtimeTableName = message.getResourceName();
        final String segmentNameStr = message.getPartitionName();
        // We got the offline transition so that is success step 2.
      }

      @Transition(from = "OFFLINE", to = "ONLINE")
      public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
        final String realtimeTableName = message.getResourceName();
        final String segmentNameStr = message.getPartitionName();

      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        final String realtimeTableName = message.getResourceName();
        final String segmentNameStr = message.getPartitionName();
      }

      @Transition(from = "OFFLINE", to = "DROPPED")
      public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        final String realtimeTableName = message.getResourceName();
        final String segmentNameStr = message.getPartitionName();
      }
      @Transition(from = "ONLINE", to = "DROPPED")
      public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
        final String realtimeTableName = message.getResourceName();
        final String segmentNameStr = message.getPartitionName();
      }
      @Transition(from = "ERROR", to = "OFFLINE")
      public void onBecomeOfflineFromError(Message message, NotificationContext context) {
        final String realtimeTableName = message.getResourceName();
        final String segmentNameStr = message.getPartitionName();
      }
    }
  }
}
