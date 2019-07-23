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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.TableRebalancer;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.server.starter.helix.SegmentOnlineOfflineStateModelFactory;
import org.apache.pinot.tools.PinotTableRebalancer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Class to test {@link TableRebalancer} using {@link PinotTableRebalancer}
 * as the entry point to test the end-to-end path from pinot-admin tool.
 */
public class TableRebalancerAdminToolClusterIntegrationTest extends BaseClusterIntegrationTestSet {

  private static final String ZKSTR = ZkStarter.DEFAULT_ZK_STR;
  private static final int NUM_INITIAL_SERVERS = 3;
  private final List<HelixManager> _helixManagers = new ArrayList<>();
  private final Set<String> servers = new HashSet<>();

  @Override
  protected boolean isUsingNewConfigFormat() {
    return true;
  }

  @BeforeClass
  public void setup()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startFakeServers(NUM_INITIAL_SERVERS, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT);
    addOfflineTable(getTableName(), null, null, null, null, getLoadMode(), SegmentVersion.v1, getInvertedIndexColumns(),
        getBloomFilterIndexColumns(), getTaskConfig(), null, null);
    completeTableConfiguration();
  }

  @AfterClass
  public void tearDown() {
    stopFakeServers();
    stopBroker();
    stopController();
    stopZk();
  }

  private void startFakeServers(final int numServers, final int basePort)
      throws Exception {
    for (int i = 0; i < numServers; i++) {
      final int nettyPort = basePort + i;
      final String instanceId =
          CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + NetUtil.getHostAddress() + "_" + nettyPort;
      if (servers.contains(instanceId)) {
        continue;
      }
      servers.add(instanceId);
      final HelixManager helixManager = HelixManagerFactory
          .getZKHelixManager(_clusterName, instanceId, InstanceType.PARTICIPANT, ZkStarter.DEFAULT_ZK_STR);
      helixManager.getStateMachineEngine()
          .registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(),
              new FakeServerSegmentStateModelFactory());
      helixManager.connect();
      helixManager.getClusterManagmentTool().addInstanceTag(_clusterName, instanceId,
          TableNameBuilder.OFFLINE.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
      _helixManagers.add(helixManager);
      ControllerLeaderLocator.create(helixManager);
    }
  }

  private void stopFakeServers() {
    for (HelixManager helixManager : _helixManagers) {
      helixManager.disconnect();
    }
  }

  /**
   * Test in no-downtime mode when there are sufficient
   * common hosts between current and target ideal states
   * to keep up the min number of serving replicas. In this
   * case, the algorithm in {@link TableRebalancer} that
   * makes changes to ideal state to get to a target ideal
   * state can do the transition in one go.
   *
   * Scenario:
   *
   * Current ideal state
   *
   * segment1: {host1:online, host2:online, host3:online}
   * segment2: {host1:online, host2:online, host3:online}
   *
   * We add 2 additional hosts: host4 and host5
   * The rebalancer will use the table config to get the
   * appropriate rebalance strategy (default strategy in this case)
   * and come up with the target ideal state
   *
   * segment1: {host1:online, host3:online, host5:online}
   * segment2: {host1:online, host2:online, host4:online}
   *
   * The test specifies the min replicas to keep up as 2.
   * With the above setup, the rebalancer can set to target
   * partition state for each segment with a direct update
   * (because of common hosts that still satisfy the min
   * replica requirement). Since this happens once for each
   * segment before we get to target ideal state, the
   * number of direct transitions are 2.
   *
   * @throws Exception
   */
  @Test
  public void testPinotTableRebalancerWithDirectTransitions()
      throws Exception {
    createIdealState(2, NUM_INITIAL_SERVERS);
    // add additional servers to trigger the rebalance strategy,
    // otherwise rebalance will be a NO-OP
    startFakeServers(2, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + NUM_INITIAL_SERVERS);
    final PinotTableRebalancer tableRebalancer = new PinotTableRebalancer(ZKSTR, _clusterName, false, true, false, 2);
    tableRebalancer.rebalance(getTableName(), "OFFLINE");
    TableRebalancer.RebalancerStats stats = tableRebalancer.getRebalancerStats();
    Assert.assertEquals(0, stats.getDryRun());
    Assert.assertEquals(2, stats.getDirectTransitions());
    Assert.assertEquals(0, stats.getIncrementalTransitions());
  }

  /**
   * Test in no-downtime mode when there are not sufficient
   * common hosts between current and target ideal states
   * to keep up the min number of serving replicas. In this
   * case, the algorithm in {@link TableRebalancer} that
   * makes changes to ideal state to get to a target ideal
   * state does the changes incrementally while satisfying
   * the min replica requirement
   *
   * Scenario:
   *
   * Current ideal state
   *
   * segment1: {host1:online, host2:online, host3:online}
   * segment2: {host1:online, host2:online, host3:online}
   * segment3: {host1:online, host2:online, host3:online}
   * segment4: {host1:online, host2:online, host3:online}
   *
   * We add 2 additional hosts: host5 and host6
   * The rebalancer will use the table config to get the
   * appropriate rebalance strategy (default strategy in this case)
   * and come up with the target ideal state
   *
   * segment1: {host1:online, host3:online, host5:online}
   * segment2: {host2:online, host4:online, host6:online}
   * segment3: {host1:online, host3:online, host5:online}
   * segment4: {host2:online, host4:online, host6:online}
   *
   * The test specifies the min replicas to keep up as 2.
   * With the above setup, the rebalancer can set to target
   * partition state for each segment with a direct update
   * (because of common hosts that still satisfy the min
   * replica requirement) only for segments 1 and 3.
   *
   * so far direct transitions 2
   *
   * for segments 2 and 4, we have to do one incremental
   * change each by removing a current server and adding
   * a new server while keeping up with requirement of
   * 2 min serving replicas
   *
   * so far incremental transitions 2
   *
   * Since we have looked at all segments once, the updated
   * ideal state is persisted in ZK, we wait for external view
   * to converge and next iteration of rebalancing begins by
   * checking if we have reached target.
   * We haven't so we go over each segment again.
   *
   * Now for each segment we can do direct update -- once for
   * each of the 4 segments
   *
   * So, direct transitions: 2+4 = 6, increment transitions = 2
   * @throws Exception
   */
  @Test
  public void testPinotTableRebalancerWithIncrementalTransitions()
      throws Exception {
    createIdealState(4, NUM_INITIAL_SERVERS);
    // add additional servers to trigger the rebalance strategy,
    // otherwise rebalance will be a NO-OP
    startFakeServers(3, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + NUM_INITIAL_SERVERS);
    final PinotTableRebalancer tableRebalancer = new PinotTableRebalancer(ZKSTR, _clusterName, false, true, false, 2);
    tableRebalancer.rebalance(getTableName(), "OFFLINE");
    TableRebalancer.RebalancerStats stats = tableRebalancer.getRebalancerStats();
    Assert.assertEquals(0, stats.getDryRun());
    Assert.assertEquals(6, stats.getDirectTransitions());
    Assert.assertEquals(2, stats.getIncrementalTransitions());
  }

  private void createIdealState(final int numSegments, final int numReplicas)
      throws Exception {
    final HelixDataAccessor dataAccessor = _helixManagers.get(0).getHelixDataAccessor();
    final String tableNameWithType = getTableName() + "_OFFLINE";
    final PropertyKey idealStateKey = dataAccessor.keyBuilder().idealStates(tableNameWithType);
    final IdealState idealState = dataAccessor.getProperty(idealStateKey);
    final IdealState idealStateCopy = HelixHelper.cloneIdealState(idealState);
    final String serverPrefix = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + NetUtil.getHostAddress() + "_";
    for (int i = 0; i < numSegments; i++) {
      final String segmentID = "segment" + i;
      if (idealStateCopy.getInstanceStateMap(segmentID) != null) {
        idealStateCopy.getInstanceStateMap(segmentID).clear();
      }
      for (int j = 0; j < numReplicas; j++) {
        final int port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + j;
        final String instanceID = serverPrefix + port;
        idealStateCopy.setPartitionState(segmentID, instanceID, "ONLINE");
      }
    }
    // this CAS serves no purpose in this single threaded execution test
    final ZkBaseDataAccessor zkBaseDataAccessor = (ZkBaseDataAccessor) dataAccessor.getBaseDataAccessor();
    zkBaseDataAccessor.set(idealStateKey.getPath(), idealStateCopy.getRecord(), idealState.getRecord().getVersion(),
        AccessOption.PERSISTENT);
  }

  public class FakeServerSegmentStateModelFactory extends StateModelFactory<StateModel> {
    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      return new FakeServerSegmentStateModelFactory.FakeSegmentStateModel();
    }

    @SuppressWarnings("unused")
    @StateModelInfo(states = "{'OFFLINE', 'ONLINE'}", initialState = "OFFLINE")
    public class FakeSegmentStateModel extends StateModel {

      @Transition(from = "OFFLINE", to = "ONLINE")
      public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      }
    }
  }
}
