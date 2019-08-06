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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.pinot.common.utils.SegmentName;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.TableRebalancer;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
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
  private final List<HelixManager> _helixManagers = new ArrayList<>();
  private static final int NUM_INITIAL_SERVERS = 3;
  private SegmentStateTransitionStats _segmentStateTransitionStats;
  private boolean _raiseErrorOnSegmentStateTransition = false;

  // todo: re-use fake server start/stop methods from ControllerTest
  @BeforeClass
  public void setup()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startKafka();
  }

  @AfterClass
  public void tearDown() {
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
  }

  private void startFakeServers(final int numServers, final int basePort, final boolean realtime)
      throws Exception {
    for (int i = 0; i < numServers; i++) {
      final int nettyPort = basePort + i;
      final String instanceId =
          CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + NetUtil.getHostAddress() + "_" + nettyPort;
      final HelixManager helixManager = HelixManagerFactory
          .getZKHelixManager(getHelixClusterName(), instanceId, InstanceType.PARTICIPANT, ZkStarter.DEFAULT_ZK_STR);
      helixManager.getStateMachineEngine()
          .registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(),
              new FakeServerSegmentStateModelFactory());
      helixManager.connect();
      if (realtime) {
        helixManager.getClusterManagmentTool().addInstanceTag(getHelixClusterName(), instanceId,
            TableNameBuilder.REALTIME.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
        _helixManagers.add(helixManager);
      } else {
        helixManager.getClusterManagmentTool().addInstanceTag(getHelixClusterName(), instanceId,
            TableNameBuilder.OFFLINE.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
        _helixManagers.add(helixManager);
      }
    }
  }

  private void stopFakeServers() {
    for (HelixManager helixManager : _helixManagers) {
      helixManager.disconnect();
    }
    _helixManagers.clear();
  }

  /**
   * Test in no-downtime mode when there are sufficient
   * common hosts between current and target ideal states
   * to keep up the min number of serving replicas. In this
   * case, the algorithm in {@link TableRebalancer} that
   * makes changes to ideal state to get to a target ideal
   * state can do the transition in one step by updating
   * the instance state map for each segment and finally
   * writing the new ideal state in ZK
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
   * number of direct updates to segment instance map are 2.
   *
   * Once the instance map of each segment is updated, we
   * write the ideal state in ZK -- this happens exactly once
   * as this ideal state is same as target and we finish.
   *
   * Total of 2 segment movements:
   * segment1 from host2 to host5
   * segment2 from host3 to host4
   *
   * @throws Exception
   */
  @Test
  public void testPinotTableRebalancerWithDirectTransitions()
      throws Exception {
    final int numSegments = 2;
    final int numReplicas = 3;
    final int numAdditionalServers = 2;
    final String tableName = getTableName() + "DIRECT";

    try {
      startFakeServers(NUM_INITIAL_SERVERS, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT, false);

      // add table
      addOfflineTable(tableName, null, null, null, null,
          getLoadMode(), SegmentVersion.v1, getInvertedIndexColumns(),
          getBloomFilterIndexColumns(), getTaskConfig(), null, null);
      completeTableConfiguration();

      // init stats
      _segmentStateTransitionStats = new SegmentStateTransitionStats();

      // write ideal state
      createIdealState(numSegments, numReplicas, tableName);

      // add additional servers to trigger the rebalance strategy,
      // otherwise rebalance will be a NO-OP
      startFakeServers(numAdditionalServers, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + NUM_INITIAL_SERVERS, false);

      // rebalance
      final PinotTableRebalancer tableRebalancer = new PinotTableRebalancer(ZKSTR, getHelixClusterName(), false, true, false, 2);
      tableRebalancer.rebalance(tableName, "OFFLINE");
      final TableRebalancer.RebalancerStats stats = tableRebalancer.getRebalancerStats();

      // check the algorithm stats on how we moved from current
      // to target ideal state
      Assert.assertEquals(stats.getDryRun(), 0);
      Assert.assertEquals(stats.getUpdatestoIdealStateInZK(), 1);
      Assert.assertEquals(stats.getDirectUpdatesToSegmentInstanceMap(), 2);
      Assert.assertEquals(stats.getIncrementalUpdatesToSegmentInstanceMap(), 0);
      Assert.assertEquals(stats.getNumSegmentMoves(), 2);

      // as part of rebalancing, host2 lost segment1 and host3
      // lost segment2 -- so 2 transitions from ON to OFF and
      // OFF to DROP
      Assert.assertEquals(_segmentStateTransitionStats.offFromOn.get(), 2);
      Assert.assertEquals(_segmentStateTransitionStats.dropFromOff.get(), 2);
    } finally {
      stopFakeServers();
    }
  }

  /**
   * Test in no-downtime mode when there are not sufficient
   * common hosts between current and target ideal states
   * to keep up the min number of serving replicas. In this
   * case, the algorithm in {@link TableRebalancer} that
   * makes changes to ideal state to get to a target ideal
   * state does the changes incrementally by making a single
   * update to instance state map of a segment
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
   * We add 3 additional hosts: host4, host5 and host6
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
   * so far direct updates to segment instance-state map: 2
   *
   * for segments 2 and 4, we have to do one incremental
   * change each by removing a current server and adding
   * a new server while keeping up with requirement of
   * 2 min serving replicas
   *
   * so far incremental updates to segment instance-state map: 2
   *
   * Since we have looked at all segments once, the updated
   * ideal state is persisted in ZK, we wait for external view
   * to converge and next iteration of rebalancing begins by
   * checking if we have reached target.
   *
   * We haven't so we go over each segment again.
   *
   * Now for each segment we can do direct update -- once for
   * each of the 4 segments while still satisfying
   * the min replica requirement. Again we write the final
   * ideal state in ZK and finish
   *
   * So, direct updates: 2+4 = 6, incremental updates = 2,
   * ideal state updates in ZK = 2
   *
   * total 6 segment movements:
   *
   * segment1 moved once to host5
   * segment2 moved from host1, host3 to host4, host6
   * segment3 moved once to host5
   * segment4 moved from host1, host3 to host4, host6
   *
   * @throws Exception
   */
  @Test
  public void testPinotTableRebalancerWithIncrementalTransitions()
      throws Exception {
    final int numSegments = 4;
    final int numReplicas = 3;
    final int numAdditionalServers = 3;
    final String tableName = getTableName() + "INCREMENTAL";

    try {
      startFakeServers(NUM_INITIAL_SERVERS, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT, false);

      // add table
      addOfflineTable(tableName, null, null,
          null, null, getLoadMode(),
          SegmentVersion.v1, getInvertedIndexColumns(), getBloomFilterIndexColumns(),
          getTaskConfig(), null, null);
      completeTableConfiguration();

      // init stats
      _segmentStateTransitionStats = new SegmentStateTransitionStats();

      // write ideal state
      createIdealState(numSegments, numReplicas, tableName);

      // add additional servers to trigger the rebalance strategy,
      // otherwise rebalance will be a NO-OP
      startFakeServers(numAdditionalServers, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + NUM_INITIAL_SERVERS, false);

      // rebalance
      final PinotTableRebalancer tableRebalancer = new PinotTableRebalancer(ZKSTR, getHelixClusterName(), false, true, false, 2);
      tableRebalancer.rebalance(tableName, "OFFLINE");
      TableRebalancer.RebalancerStats stats = tableRebalancer.getRebalancerStats();

      // verify the algorithm stats on how we moved from
      // current to target ideal state
      Assert.assertEquals(0, stats.getDryRun());
      Assert.assertEquals(stats.getUpdatestoIdealStateInZK(), 2);
      Assert.assertEquals(stats.getDirectUpdatesToSegmentInstanceMap(), 6);
      Assert.assertEquals(stats.getIncrementalUpdatesToSegmentInstanceMap(), 2);
      Assert.assertEquals(stats.getNumSegmentMoves(), 6);

      // as part of rebalancing, host2 lost segment1 and segment3,
      // host1 and host3 lost segment2 and segment4 -- so 6
      // transitions from ON to OFF and OFF to DROP
      Assert.assertEquals(_segmentStateTransitionStats.offFromOn.get(), 6);
      Assert.assertEquals(_segmentStateTransitionStats.dropFromOff.get(), 6);
    } finally {
      stopFakeServers();
    }
  }

  /**
   * Test in no-downtime mode by removing replica servers
   *
   * Scenario: 5 servers, 3 replicas per segment
   *
   * Current ideal state
   *
   * segment1: {host0:online, host2:online, host4:online}
   * segment2: {host1:online, host2:online, host3:online}
   * segment3: {host0:online, host3:online, host4:online}
   * segment4: {host1:online, host3:online, host4:online}
   *
   * We remove servers host3 and host4
   * The rebalancer will use the table config to get the
   * appropriate rebalance strategy (default strategy in this case)
   * and come up with the target ideal state
   *
   * segment1: {host0:online, host1:online, host2:online}
   * segment2: {host0:online, host1:online, host2:online}
   * segment3: {host0:online, host1:online, host2:online}
   * segment4: {host0:online, host1:online, host2:online}
   *
   * The test specifies the min replicas to keep up as 2.
   * With the above setup, the rebalancer can set to target
   * partition state for each segment with a direct update
   * (because of common hosts that still satisfy the min
   * replica requirement) only for segments 1 and 2.
   *
   * so far direct updates to segment instance-state map: 2
   *
   * for segments 3 and 4, we have to do one incremental
   * change each by removing a current server and adding
   * a new server while keeping up with requirement of
   * 2 min serving replicas
   *
   * so far incremental updates to segment instance-state map: 2
   *
   * Since we have looked at all segments once, the updated
   * ideal state is persisted in ZK, we wait for external view
   * to converge and next iteration of rebalancing begins by
   * checking if we have reached target.
   * We haven't so we go over each segment again.
   *
   * Now for each segment we can do direct update -- once for
   * each of the 4 segments while still satisfying
   * the min replica requirement. Again we write the final
   * ideal state in ZK and finish
   *
   * So, direct updates: 2+4 = 6, incremental updates = 2,
   * ideal state updates in ZK = 2
   *
   * total 6 segment movements
   *
   * @throws Exception
   */
  @Test
  public void testPinotTableRebalancerAfterRemovingServers()
      throws Exception {
    try {
      // basic cluster setup in this class is 3 servers (NUM_INITIAL_SERVERS).
      // for this test where we will remove servers to trigger rebalancing,
      // we will start with a cluster of 5 servers so add 2 more servers
      startFakeServers(NUM_INITIAL_SERVERS + 2, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT, false);

      // add table
      final String tableName = getTableName() + "REMOVE";
      addOfflineTable(tableName, null, null,
          null, null, getLoadMode(),
          SegmentVersion.v1, getInvertedIndexColumns(), getBloomFilterIndexColumns(),
          getTaskConfig(), null, null);
      completeTableConfiguration();

      // init stats
      _segmentStateTransitionStats = new SegmentStateTransitionStats();

      // create ideal state
      final HelixDataAccessor dataAccessor = _helixManagers.get(0).getHelixDataAccessor();
      final String tableNameWithType = tableName + "_OFFLINE";
      final PropertyKey idealStateKey = dataAccessor.keyBuilder().idealStates(tableNameWithType);
      final IdealState idealState = dataAccessor.getProperty(idealStateKey);
      final IdealState idealStateCopy = HelixHelper.cloneIdealState(idealState);

      idealStateCopy.setPartitionState("segment1", _helixManagers.get(0).getInstanceName(), "ONLINE");
      idealStateCopy.setPartitionState("segment1", _helixManagers.get(2).getInstanceName(), "ONLINE");
      idealStateCopy.setPartitionState("segment1", _helixManagers.get(4).getInstanceName(), "ONLINE");

      idealStateCopy.setPartitionState("segment2", _helixManagers.get(1).getInstanceName(), "ONLINE");
      idealStateCopy.setPartitionState("segment2", _helixManagers.get(2).getInstanceName(), "ONLINE");
      idealStateCopy.setPartitionState("segment2", _helixManagers.get(3).getInstanceName(), "ONLINE");

      idealStateCopy.setPartitionState("segment3", _helixManagers.get(0).getInstanceName(), "ONLINE");
      idealStateCopy.setPartitionState("segment3", _helixManagers.get(3).getInstanceName(), "ONLINE");
      idealStateCopy.setPartitionState("segment3", _helixManagers.get(4).getInstanceName(), "ONLINE");

      idealStateCopy.setPartitionState("segment4", _helixManagers.get(1).getInstanceName(), "ONLINE");
      idealStateCopy.setPartitionState("segment4", _helixManagers.get(3).getInstanceName(), "ONLINE");
      idealStateCopy.setPartitionState("segment4", _helixManagers.get(4).getInstanceName(), "ONLINE");

      // write ideal state for the setup of 4 segments, 5 servers and 3 replicas per segment
      final ZkBaseDataAccessor zkBaseDataAccessor = (ZkBaseDataAccessor) dataAccessor.getBaseDataAccessor();
      zkBaseDataAccessor.set(idealStateKey.getPath(), idealStateCopy.getRecord(), idealState.getRecord().getVersion(),
          AccessOption.PERSISTENT);

      // remove server3 and server4
      final HelixManager host3 = _helixManagers.get(3);
      final HelixManager host4 = _helixManagers.get(4);
      host3.getClusterManagmentTool().removeInstanceTag(getHelixClusterName(), host3.getInstanceName(),
          TableNameBuilder.OFFLINE.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
      host4.getClusterManagmentTool().removeInstanceTag(getHelixClusterName(), host4.getInstanceName(),
          TableNameBuilder.OFFLINE.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));

      // rebalance
      final PinotTableRebalancer tableRebalancer = new PinotTableRebalancer(ZKSTR, getHelixClusterName(), false, true, false, 2);
      tableRebalancer.rebalance(tableName, "OFFLINE");
      final TableRebalancer.RebalancerStats stats = tableRebalancer.getRebalancerStats();

      // check the algorithm stats on how we moved from current
      // to target ideal state
      Assert.assertEquals(stats.getDryRun(), 0);
      Assert.assertEquals(stats.getUpdatestoIdealStateInZK(), 2);
      Assert.assertEquals(stats.getDirectUpdatesToSegmentInstanceMap(), 6);
      Assert.assertEquals(stats.getIncrementalUpdatesToSegmentInstanceMap(), 2);
      Assert.assertEquals(stats.getNumSegmentMoves(), 6);

      // as part of rebalancing, host3 lost segment2, segment3,
      // and segment4. similarly, host4 lost segment1, segment3
      // and segment4 -- total 6 transitions from ONLINE to OFFLINE
      // and OFFLINE to DROPPED
      Assert.assertEquals(_segmentStateTransitionStats.offFromOn.get(), 6);
      Assert.assertEquals(_segmentStateTransitionStats.dropFromOff.get(), 6);
    } finally {
      stopFakeServers();
    }
  }

  /**
   * This tests the behavior of table rebalancer in the event
   * when external view reports error state for segment(s).
   *
   * {@link TableRebalancer} updates the ideal state in ZK
   * after making changes (complete or partial/incremental)
   * to state. It then waits for the external view to converge.
   * Meanwhile as part of the change it did in ideal state and
   * state transition was not properly handled by the servers,
   * external view will report ERROR state. Rebalancer checks if
   * for any segment the state is ERROR and bails out immediately
   *
   * To simulate the error scenario, we explicitly throw
   * exception for a particular segment when the callback is
   * invoked for state change from OFFLINE to ONLINE when
   */
  @Test
  public void testForErrorOnSegmentStateTransition() {
    try {
      final int numSegments = 2;
      final int numReplicas = 3;
      final int numAdditionalServers = 2;
      final String tableName = getTableName() + "FAILURE";

      startFakeServers(NUM_INITIAL_SERVERS, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT, false);

      // add table
      addOfflineTable(tableName, null, null, null, null,
          getLoadMode(), SegmentVersion.v1, getInvertedIndexColumns(),
          getBloomFilterIndexColumns(), getTaskConfig(), null, null);
      completeTableConfiguration();

      // write ideal state
      createIdealState(numSegments, numReplicas, tableName);

      // add additional servers to trigger the rebalance strategy,
      // otherwise rebalance will be a NO-OP
      startFakeServers(numAdditionalServers, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + NUM_INITIAL_SERVERS, false);

      // set the flag to raise error on segment state transition to true
      // this will ensure that as part of rebalancer when a particular
      // segment is moved from one server to another, the latter throws
      // exception on state transition from OFFLINE to ONLINE
      _raiseErrorOnSegmentStateTransition = true;

      // rebalance
      final PinotTableRebalancer tableRebalancer = new PinotTableRebalancer(ZKSTR, getHelixClusterName(), false, true, false, 2);
      tableRebalancer.rebalance(tableName, "OFFLINE");
      Assert.fail("Expecting exception");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("External view reports error state for segment segment0"));
    } finally {
      _raiseErrorOnSegmentStateTransition = false;
      stopFakeServers();
    }
  }

  /**
   * Test for realtime table in no downtime mode
   * with LLC consumer and rebalancer is asked
   * to consider segments in consuming state as well
   *
   * Scenario: 4 segments 3 replicas
   *
   * Current ideal state
   *
   * segment__0__0: {host1:online, host2:online, host3:online}
   * segment__0__1: {host1:consuming, host2:consuming, host3:consuming}
   * segment__1__0: {host1:online, host2:online, host3:online}
   * segment__1__1: {host1:consuming, host2:consuming, host3:consuming}
   *
   * We add 3 additional hosts: host4, host5 and host6
   * The rebalancer will use the table config to get the
   * appropriate rebalance strategy (default strategy in this case)
   * and come up with the target ideal state
   *
   * segment__0__0: {host1:online, host3:online, host5:online}
   * segment__0__1: {host1:consuming, host2:consuming, host6:consuming}
   * segment__1__0: {host2:online, host4:online, host6:online}
   * segment__1__1: {host3:consuming, host4:consuming, host5:consuming}
   *
   * total 6 segment moves
   *
   * segment00 : host2 -> host5
   * segment01 : host3 -> host6
   * segment10 : host1, host3 -> host4, host6
   * segment11 : host1, host2 -> host4, host5
   *
   * @throws Exception
   */
  @Test
  public void testLLCRealtimeTableIncludeConsuming() throws Exception {
    final int numSegments = 4;
    final int numReplicas = 3;
    final int numAdditionalServers = 3;
    final String tableName = getTableName() + "LLCONSUMING";

    try {
      startFakeServers(NUM_INITIAL_SERVERS, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT, true);
      setUpRealtimeTable(null, numReplicas, true, tableName);

      // init stats
      _segmentStateTransitionStats = new SegmentStateTransitionStats();

      // write ideal state
      createIdealStateForRealtime(numSegments, numReplicas, tableName, true);

      // add additional servers to trigger the rebalance strategy,
      // otherwise rebalance will be a NO-OP
      startFakeServers(numAdditionalServers, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + NUM_INITIAL_SERVERS, true);

      // rebalance
      final PinotTableRebalancer tableRebalancer = new PinotTableRebalancer(ZKSTR, getHelixClusterName(), false, true, true, 2);
      tableRebalancer.rebalance(tableName, "REALTIME");
      final TableRebalancer.RebalancerStats stats = tableRebalancer.getRebalancerStats();

      // verify the algorithm stats on how we moved from
      // current to target ideal state
      Assert.assertEquals(0, stats.getDryRun());
      Assert.assertEquals(stats.getUpdatestoIdealStateInZK(), 2);
      Assert.assertEquals(stats.getDirectUpdatesToSegmentInstanceMap(), 6);
      Assert.assertEquals(stats.getIncrementalUpdatesToSegmentInstanceMap(), 2);
      Assert.assertEquals(stats.getNumSegmentMoves(), 6);

      // as part of rebalancing, host2 lost segment1 and segment3,
      // host1 and host3 lost segment2 and segment4 -- so 6
      // transitions from ON to OFF and OFF to DROP
      Assert.assertEquals(_segmentStateTransitionStats.offFromOn.get(), 3);
      Assert.assertEquals(_segmentStateTransitionStats.dropFromOff.get(), 3);
    } finally {
      stopFakeServers();
    }
  }

  /**
   * Test for realtime table in no downtime mode
   * with LLC consumer and rebalancer is asked
   * not to consider segments in consuming state
   *
   * Scenario: 4 segments 3 replicas
   *
   * Current ideal state
   *
   * segment__0__0: {host1:online, host2:online, host3:online}
   * segment__0__1: {host1:consuming, host2:consuming, host3:consuming}
   * segment__1__0: {host1:online, host2:online, host3:online}
   * segment__1__1: {host1:consuming, host2:consuming, host3:consuming}
   *
   * We add 3 additional hosts: host4, host5 and host6
   * The rebalancer will use the table config to get the
   * appropriate rebalance strategy (default strategy in this case)
   * and come up with the target ideal state
   *
   * segment__0__0: {host1:online, host3:online, host5:online}
   * segment__0__1: {host1:consuming, host2:consuming, host3:consuming}
   * segment__1__0: {host2:online, host4:online, host6:online}
   * segment__1__1: {host1:consuming, host2:consuming, host3:consuming}
   *
   * consuming segments will not be moved so total 3 segment moves
   *
   * segment00 : host2 -> host5
   * segment10: host1, host3 - > host4, host6
   *
   * @throws Exception
   */
  @Test
  public void testLLCRealtimeTableNoIncludeConsuming() throws Exception {
    final int numSegments = 4;
    final int numReplicas = 3;
    final int numAdditionalServers = 3;
    final String tableName = getTableName() + "LLNOCONSUMING";

    try {
      startFakeServers(NUM_INITIAL_SERVERS, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT, true);
      setUpRealtimeTable(null, numReplicas, true, tableName);

      // init stats
      _segmentStateTransitionStats = new SegmentStateTransitionStats();

      // write ideal state
      createIdealStateForRealtime(numSegments, numReplicas, tableName, true);

      // add additional servers to trigger the rebalance strategy,
      // otherwise rebalance will be a NO-OP
      startFakeServers(numAdditionalServers, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + NUM_INITIAL_SERVERS, true);

      // rebalance
      final PinotTableRebalancer tableRebalancer = new PinotTableRebalancer(ZKSTR, getHelixClusterName(), false, true, false, 2);
      tableRebalancer.rebalance(tableName, "REALTIME");
      final TableRebalancer.RebalancerStats stats = tableRebalancer.getRebalancerStats();

      // verify the algorithm stats on how we moved from
      // current to target ideal state
      Assert.assertEquals(0, stats.getDryRun());
      Assert.assertEquals(stats.getUpdatestoIdealStateInZK(), 2);
      Assert.assertEquals(stats.getDirectUpdatesToSegmentInstanceMap(), 7);
      Assert.assertEquals(stats.getIncrementalUpdatesToSegmentInstanceMap(), 1);
      Assert.assertEquals(stats.getNumSegmentMoves(), 3);

      // as part of rebalancing, host2 lost segment1 and segment3,
      // host1 and host3 lost segment2 and segment4 -- so 6
      // transitions from ON to OFF and OFF to DROP
      Assert.assertEquals(_segmentStateTransitionStats.offFromOn.get(), 3);
      Assert.assertEquals(_segmentStateTransitionStats.dropFromOff.get(), 3);
    } finally {
      stopFakeServers();
    }
  }

  /**
   * Test for realtime table in no downtime mode
   * with HLC consumer
   *
   * Scenario: 4 segments 3 replicas
   *
   * Current ideal state
   *
   * segment__0__0: {host1:online, host2:online, host3:online}
   * segment__0__1: {host1:consuming, host2:consuming, host3:consuming}
   * segment__1__0: {host1:online, host2:online, host3:online}
   * segment__1__1: {host1:consuming, host2:consuming, host3:consuming}
   *
   * We add 3 additional hosts: host4, host5 and host6 and check that
   * rebalancer does not do anything for realtime table with
   * HLC consumer
   *
   * @throws Exception
   */
  @Test
  public void testHLCRealtimeTable() throws Exception {
    final int numSegments = 4;
    final int numReplicas = 3;
    final int numAdditionalServers = 3;
    final String tableName = getTableName() +"HLC";

    try {
      startFakeServers(NUM_INITIAL_SERVERS, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT, true);
      setUpRealtimeTable(null, numReplicas, false, tableName);

      // init stats
      _segmentStateTransitionStats = new SegmentStateTransitionStats();

      // write ideal state
      createIdealStateForRealtime(numSegments, numReplicas, tableName, false);

      // add additional servers to trigger the rebalance strategy,
      // otherwise rebalance will be a NO-OP
      startFakeServers(numAdditionalServers, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + NUM_INITIAL_SERVERS, true);

      // rebalance
      final PinotTableRebalancer tableRebalancer = new PinotTableRebalancer(ZKSTR, getHelixClusterName(), false, true, false, 2);
      tableRebalancer.rebalance(tableName, "REALTIME");
      final TableRebalancer.RebalancerStats stats = tableRebalancer.getRebalancerStats();

      // verify the algorithm stats on how we moved from
      // current to target ideal state
      Assert.assertEquals(0, stats.getDryRun());
      Assert.assertEquals(stats.getUpdatestoIdealStateInZK(), 0);
      Assert.assertEquals(stats.getDirectUpdatesToSegmentInstanceMap(), 0);
      Assert.assertEquals(stats.getIncrementalUpdatesToSegmentInstanceMap(), 0);
      Assert.assertEquals(stats.getNumSegmentMoves(), 0);

      Assert.assertEquals(_segmentStateTransitionStats.offFromOn.get(), 0);
      Assert.assertEquals(_segmentStateTransitionStats.dropFromOff.get(), 0);
    } finally {
      stopFakeServers();
    }
  }

  private void createIdealStateForRealtime(final int numSegments, final int numReplicas,
      final String tableName, boolean llc)
      throws Exception {
    final HelixDataAccessor dataAccessor = _helixManagers.get(0).getHelixDataAccessor();
    final String tableNameWithType = tableName + "_REALTIME";
    final PropertyKey idealStateKey = dataAccessor.keyBuilder().idealStates(tableNameWithType);
    final IdealState idealState = dataAccessor.getProperty(idealStateKey);
    final IdealState idealStateCopy = HelixHelper.cloneIdealState(idealState);
    final String serverPrefix = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + NetUtil.getHostAddress() + "_";
    Map<String, Map<String, String>> idealStateMap = idealStateCopy.getRecord().getMapFields();

    // the creation of LLC realtime table would have already created ideal state
    // with segments from 2 partitions in CONSUMING state.
    // make that state as ONLINE so next we can add
    // segments with higher sequence number for the same kafka partition
    // in CONSUMING state
    for (String segment : idealStateMap.keySet()) {
      if (idealStateCopy.getInstanceStateMap(segment) != null) {
        idealStateCopy.getInstanceStateMap(segment).clear();
      }
      for (int i = 0; i < numReplicas; i++) {
        final int port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + i;
        final String instanceID = serverPrefix + port;
        idealStateCopy.setPartitionState(segment, instanceID, "ONLINE");
      }
    }

    if (llc) {
      // write 2 latest segments for the same kafka partitions (0 and 1)
      // with higher sequence number and in CONSUMING state
      final String timestamp = idealStateMap.keySet().iterator().next().split(SegmentName.SEPARATOR)[3];
      final String segmentPrefix = tableName + "__";
      final String segmentSuffix = "__"+ timestamp;
      int sequenceNumber = 1;
      for (int kafkaPartition = 0; kafkaPartition < 2; kafkaPartition++) {
        final String segmentID = segmentPrefix + kafkaPartition + "__" + sequenceNumber + segmentSuffix;
        for (int j = 0; j < numReplicas; j++) {
          final int port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT + j;
          final String instanceID = serverPrefix + port;
          idealStateCopy.setPartitionState(segmentID, instanceID, "CONSUMING");
        }
      }
    }

    final ZkBaseDataAccessor zkBaseDataAccessor = (ZkBaseDataAccessor) dataAccessor.getBaseDataAccessor();
    zkBaseDataAccessor.set(idealStateKey.getPath(), idealStateCopy.getRecord(), idealState.getRecord().getVersion(),
        AccessOption.PERSISTENT);
  }

  private void createIdealState(final int numSegments, final int numReplicas,
      final String tableName)
      throws Exception {
    final HelixDataAccessor dataAccessor = _helixManagers.get(0).getHelixDataAccessor();
    final String tableNameWithType = tableName + "_OFFLINE";
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
    @StateModelInfo(states = "{'OFFLINE', 'ONLINE', , 'CONSUMING', 'DROPPED'}", initialState = "OFFLINE")
    public class FakeSegmentStateModel extends StateModel {

      @Transition(from = "OFFLINE", to = "CONSUMING")
      public void onBecomeConsumingFromOffline(Message message, NotificationContext context) {
      }

      @Transition(from = "CONSUMING", to = "ONLINE")
      public void onBecomeOnlineFromConsuming(Message message, NotificationContext context) {
      }

      @Transition(from = "OFFLINE", to = "ONLINE")
      public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
        if (_raiseErrorOnSegmentStateTransition && message.getPartitionName().equals("segment0")) {
          throw new IllegalStateException("Unable to move segment0 from offline to online state");
        }
      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
        if (_segmentStateTransitionStats != null) {
          _segmentStateTransitionStats.offFromOn.incrementAndGet();
        }
      }

      @Transition(from = "OFFLINE", to = "DROPPED")
      public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
        if (_segmentStateTransitionStats != null) {
          _segmentStateTransitionStats.dropFromOff.incrementAndGet();
        }
      }

      @Transition(from = "ONLINE", to = "DROPPED")
      public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
        if (_segmentStateTransitionStats != null) {
          System.out.println("online to dropped");
        }
      }
    }
  }

  private static class SegmentStateTransitionStats {
    private AtomicInteger offFromOn = new AtomicInteger();
    private AtomicInteger dropFromOff = new AtomicInteger();
  }

  @Override
  protected boolean isUsingNewConfigFormat() {
    return false;
  }

  @Override
  protected boolean useLlc() {
    return true;
  }
}
