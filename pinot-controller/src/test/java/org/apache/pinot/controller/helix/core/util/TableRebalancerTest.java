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
package org.apache.pinot.controller.helix.core.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.TableRebalancer;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceUserConfigConstants;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TableRebalancerTest {

  private IdealState current;
  private final String segmentId = "segment1";
  private Configuration downtime = new PropertiesConfiguration();
  private Configuration noDowntime = new PropertiesConfiguration();

  @BeforeMethod
  public void setup() {
    current = new IdealState("rebalance");
    current.setPartitionState(segmentId, "host1", "ONLINE");
    current.setPartitionState(segmentId, "host2", "ONLINE");

    downtime.setProperty(RebalanceUserConfigConstants.DOWNTIME, true);
    noDowntime.setProperty(RebalanceUserConfigConstants.DOWNTIME, false);
  }

  // no-downtime rebalance with common elements - target state is set in one go
  @Test
  public void noDowntimeUpdateWithCommonElements() {
    Map<String, String> targetMap = new HashMap<>();
    targetMap.put("host1", "ONLINE");
    targetMap.put("host3", "ONLINE");

    Map<String, String> srcMap = current.getRecord().getMapField(segmentId);
    Assert.assertEquals(srcMap.size(), 2);
    TableRebalancer updater = new TableRebalancer(null, null, null);
    updater.updateSegmentIfNeeded(segmentId, srcMap, targetMap, current, noDowntime);

    Map<String, String> tempMap = current.getRecord().getMapField(segmentId);
    Assert.assertTrue(EqualityUtils.isEqual(tempMap, targetMap));
  }

  // downtime rebalance with common elements - target state is set in one go
  @Test
  public void downtimeUpdateWithCommonElements() {
    Map<String, String> targetMap = new HashMap<>();
    targetMap.put("host1", "ONLINE");
    targetMap.put("host3", "ONLINE");

    Map<String, String> srcMap = current.getRecord().getMapField(segmentId);
    Assert.assertEquals(srcMap.size(), 2);
    TableRebalancer updater = new TableRebalancer(null, null, null);
    updater.updateSegmentIfNeeded(segmentId, srcMap, targetMap, current, downtime);

    Map<String, String> tempMap = current.getRecord().getMapField(segmentId);
    Assert.assertTrue(EqualityUtils.isEqual(tempMap, targetMap));
  }

  // no-downtime rebalance without common elements - target state is updated to have one up replica
  @Test
  public void noDowntimeUpdateWithNoCommonElements() {
    Map<String, String> targetMap = new HashMap<>();
    targetMap.put("host4", "ONLINE");
    targetMap.put("host3", "ONLINE");

    Map<String, String> srcMap = current.getRecord().getMapField(segmentId);
    TableRebalancer updater = new TableRebalancer(null, null, null);
    updater.updateSegmentIfNeeded(segmentId, srcMap, targetMap, current, noDowntime);
    updater.updateSegmentIfNeeded(segmentId, srcMap, targetMap, current, noDowntime);

    Map<String, String> tempMap = current.getRecord().getMapField(segmentId);
    Set<String> targetHosts = new HashSet<String>(Arrays.asList("host3", "host4"));
    Set<String> srcHosts = new HashSet<String>(Arrays.asList("host1", "host2"));
    Assert.assertEquals(tempMap.size(), targetHosts.size());
    for (String instance : tempMap.keySet()) {
      Assert.assertTrue(targetHosts.contains(instance) || srcHosts.contains(instance));
    }
  }

  // downtime rebalance without common elements - target state is set in one go
  @Test
  public void downtimeUpdateWithNoCommonElements() {
    Map<String, String> targetMap = new HashMap<>();
    targetMap.put("host4", "ONLINE");
    targetMap.put("host3", "ONLINE");

    Map<String, String> srcMap = current.getRecord().getMapField(segmentId);
    TableRebalancer updater = new TableRebalancer(null, null, null);
    updater.updateSegmentIfNeeded(segmentId, srcMap, targetMap, current, downtime);

    Map<String, String> tempMap = current.getRecord().getMapField(segmentId);
    Assert.assertTrue(EqualityUtils.isEqual(tempMap, targetMap));
  }

  /**
   * Test for now downtime rebalance with no common hosts between
   * current and target ideal state and a request to keep minimum
   * 2 replicas up while rebalancing
   */
  @Test
  public void noDowntimeUpdateWithNoCommonHostsAndTwoMinReplicas() {
    IdealState currentIdealState = new IdealState("rebalance");
    currentIdealState.setPartitionState(segmentId, "host1", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host2", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host3", "ONLINE");

    IdealState targetIdealState = new IdealState("rebalance");
    targetIdealState.setPartitionState(segmentId, "host4", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host5", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host6", "ONLINE");

    // copying the current ideal state here to mimic how
    // TableRebalancer.rebalance() works between multiple invocations to
    // updateSegmentIfNeeded. In other words, the latter function updates the
    // passed in copy of current ideal state, returns and the updated state is
    // then written into ZK as the latest current ideal state. Before
    // updateSegmentIfNeeded() again, the rebalance() method gets
    // the current ideal state from ZK
    IdealState toUpdateIdealState = HelixHelper.cloneIdealState(currentIdealState);

    // test for no-downtime rebalance with minimum 2 replicas of each segment to be kept up
    noDowntime.setProperty(RebalanceUserConfigConstants.MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME, 2);

    final TableRebalancer rebalancer = new TableRebalancer(null, null, null);
    final Map<String, String> targetSegmentInstancesMap = targetIdealState.getInstanceStateMap(segmentId);

    // STEP 1
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // After step 1, let's assume that
    // toUpdateIdealState : { SEGMENT1 : { host4 : online, host2 : online, host3 : online } }
    // Essentially, host1 from current state got replaced with host4 from target state
    // Now, what we are checking is that between toUpdate and target ideal state,
    // there is exactly 1 host in common
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 1, true);
    currentIdealState = toUpdateIdealState;

    // STEP 2
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // Now if the logic to keep up minimum number of serving replicas is doing
    // the right thing, in step 2 it should not have simply updated the ideal
    // state to target in one step simply because there is one common element (host4).
    // The reason being if we directly do this update, then we are not guaranteeing
    // that minimum 2 replicas should be kept up as in direct update host4
    // is the only one that might be up for serving queries. So right thing to do
    // is what happened in step 1 -- replace a host in current ideal state with
    // a host from target ideal state. This will ensure that 2 serving replicas are up.
    //
    // toUpdateIdealState : { SEGMENT1 : { host4 : online, host5 : online, host3 : online } }
    // Essentially, host2 from current state got replaced with host5
    // Now, what we are checking is that between toUpdate and target ideal state,
    // there are exactly 2 hosts in common and not 3
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 2, true);
    currentIdealState = toUpdateIdealState;

    // STEP 3
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // At this point, it would be perfectly fine to just directly set the ideal
    // state to target since the number of hosts in common >= min serving
    // replicas we need
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 3, true);

    // Verify the same behavior (as described in above steps) through stats
    TableRebalancer.RebalancerStats rebalancerStats = rebalancer.getRebalancerStats();
    // STEP 1 and STEP 2 -- incremental transitions
    Assert.assertEquals(2, rebalancerStats.getIncrementalTransitions());
    // STEP 3 -- final direct transition
    Assert.assertEquals(1, rebalancerStats.getDirectTransitions());
  }

  /**
   * Test for now downtime rebalance with no common hosts between
   * current and target ideal state and a request to keep minimum
   * 2 replicas up while rebalancing along with the increase
   */
  @Test
  public void noDowntimeUpdateWithNoCommonHostsAndIncreasedReplicas() {
    IdealState currentIdealState = new IdealState("rebalance");
    currentIdealState.setPartitionState(segmentId, "host1", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host2", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host3", "ONLINE");

    IdealState targetIdealState = new IdealState("rebalance");
    targetIdealState.setPartitionState(segmentId, "host4", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host5", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host6", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host7", "ONLINE");

    IdealState toUpdateIdealState = HelixHelper.cloneIdealState(currentIdealState);

    // test for no-downtime rebalance with minimum 2 replicas of each segment to be kept up
    noDowntime.setProperty(RebalanceUserConfigConstants.MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME, 2);

    final TableRebalancer rebalancer = new TableRebalancer(null, null, null);
    final Map<String, String> targetSegmentInstancesMap = targetIdealState.getInstanceStateMap(segmentId);

    // STEP 1
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // After step 1, let's assume that
    // toUpdateIdealState : { SEGMENT1 : { host4 : online, host2 : online, host3 : online } }
    // Essentially, host1 from current state got replaced with host4 from target state
    // Now, what we are checking is that between toUpdate and target ideal state,
    // there is exactly 1 host in common
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 1, false);
    currentIdealState = toUpdateIdealState;

    // STEP 2
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // another incremental transition to keep up with 2 serving replicas requirement
    // toUpdateIdealState : { SEGMENT1 : { host4 : online, host5 : online, host3 : online } }
    // Essentially, host2 from current state got replaced with host5
    // Now, what we are checking is that between toUpdate and target ideal state,
    // there are exactly 2 hosts in common and not 3
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 2, false);
    currentIdealState = toUpdateIdealState;

    // STEP 3
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // At this point, it would be perfectly fine to just directly set the ideal
    // state to target since the number of hosts in common >= min serving
    // replicas we need
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 4, true);

    // Verify the same behavior (as described in above steps) through stats
    TableRebalancer.RebalancerStats rebalancerStats = rebalancer.getRebalancerStats();
    // STEP 1 and STEP 2 -- incremental transitions
    Assert.assertEquals(2, rebalancerStats.getIncrementalTransitions());
    // STEP 3 -- final direct transition
    Assert.assertEquals(1, rebalancerStats.getDirectTransitions());
  }

  /**
   * Test for now downtime rebalance with no common hosts between
   * current and target ideal state and a request to keep minimum
   * 1 replica up while rebalancing
   */
  @Test
  public void noDowntimeUpdateWithNoCommonHostsAndOneMinReplica() {
    IdealState currentIdealState = new IdealState("rebalance");
    currentIdealState.setPartitionState(segmentId, "host1", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host2", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host3", "ONLINE");

    IdealState targetIdealState = new IdealState("rebalance");
    targetIdealState.setPartitionState(segmentId, "host4", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host5", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host6", "ONLINE");

    IdealState toUpdateIdealState = HelixHelper.cloneIdealState(currentIdealState);

    // test for no-downtime rebalance with minimum 2 replicas of each segment to be kept up
    noDowntime.setProperty(RebalanceUserConfigConstants.MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME, 1);

    final TableRebalancer rebalancer = new TableRebalancer(null, null, null);
    final Map<String, String> targetSegmentInstancesMap = targetIdealState.getInstanceStateMap(segmentId);

    // STEP 1
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // After step 1, let's assume that
    // toUpdateIdealState : { SEGMENT1 : { host4 : online, host2 : online, host3 : online } }
    // Essentially, host1 from current state got replaced with host4
    // Now, what we are checking is that between toUpdate and target ideal state,
    // there is exactly 1 host in common
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 1, true);
    currentIdealState = toUpdateIdealState;

    // STEP 2
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // Now since the minimum number of serving replicas we need is 1, it is fine
    // to directly (in one step) set the ideal state to target by replacing
    // host2 and host3 from current state with host5 and host6 from target state
    // Doing this direct update on current ideal state still ensures that
    // 1 replica (host4) is up for serving
    //
    // toUpdateIdealState : { SEGMENT1 : { host4 : online, host5 : online, host6 : online } }
    // Essentially, host2, host3 from current state got replaced with host5 and host6
    // Now, what we are checking is that between toUpdate and target ideal state,
    // all hosts are in common
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 3, true);

    TableRebalancer.RebalancerStats rebalancerStats = rebalancer.getRebalancerStats();
    // STEP 1 incremental transition
    Assert.assertEquals(1, rebalancerStats.getIncrementalTransitions());
    // STEP 2 -- final direct transition
    Assert.assertEquals(1, rebalancerStats.getDirectTransitions());
  }

  /**
   * Test for now downtime rebalance with no common hosts between
   * current and target ideal state and a request to keep an invalid
   * number of minimum replicas up while rebalancing
   */
  @Test
  public void noDowntimeUpdateWithNoCommonHostsAndInvalidMinReplicas() {
    IdealState currentIdealState = new IdealState("rebalance");
    currentIdealState.setPartitionState(segmentId, "host1", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host2", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host3", "ONLINE");

    IdealState targetIdealState = new IdealState("rebalance");
    targetIdealState.setPartitionState(segmentId, "host4", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host5", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host6", "ONLINE");

    IdealState toUpdateIdealState = HelixHelper.cloneIdealState(currentIdealState);

    // test for no-downtime rebalance with minimum 4 (impossible) replicas of each segment to be kept up
    noDowntime.setProperty(RebalanceUserConfigConstants.MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME, 4);

    final TableRebalancer rebalancer = new TableRebalancer(null, null, null);
    final Map<String, String> targetSegmentInstancesMap = targetIdealState.getInstanceStateMap(segmentId);

    // STEP 1
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // After step 1, let's assume that
    // toUpdateIdealState : { SEGMENT1 : { host4 : online, host2 : online, host3 : online } }
    // Essentially, host1 from current state got replaced with host4
    // Now, what we are checking is that between toUpdate and target ideal state,
    // there is exactly 1 host in common
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 1, true);
    currentIdealState = toUpdateIdealState;

    // STEP 2
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // Now since the minimum number of serving replicas we asked for are 4,
    // the rebalancer will detect that is impossible to keep up 4 replicas
    // since a segment only has 3 replicas in the current configuration. So
    // it is fine to directly set the ideal state to target in this step by replacing
    // host2 and host3 from current state with host5 and host6 from target state
    // Doing this direct update on current ideal state still ensures that
    // 1 replica (host4) is up for serving
    // Now, what we are checking is that between toUpdate and target ideal state,
    // all hosts are in common
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 3, true);

    TableRebalancer.RebalancerStats rebalancerStats = rebalancer.getRebalancerStats();
    // STEP 1 incremental transition
    Assert.assertEquals(1, rebalancerStats.getIncrementalTransitions());
    // STEP 2 -- final direct transition
    Assert.assertEquals(1, rebalancerStats.getDirectTransitions());
  }

  /**
   * Test for now downtime rebalance with common hosts between
   * current and target ideal state and a request to keep minimum
   * 2 replicas up while rebalancing
   */
  @Test
  public void noDowntimeUpdateWithCommonHostsAndMinReplicas() {
    IdealState currentIdealState = new IdealState("rebalance");
    currentIdealState.setPartitionState(segmentId, "host1", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host2", "ONLINE");
    currentIdealState.setPartitionState(segmentId, "host3", "ONLINE");

    IdealState targetIdealState = new IdealState("rebalance");
    targetIdealState.setPartitionState(segmentId, "host1", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host5", "ONLINE");
    targetIdealState.setPartitionState(segmentId, "host6", "ONLINE");

    IdealState toUpdateIdealState = HelixHelper.cloneIdealState(currentIdealState);

    // test for no-downtime rebalance with minimum 4 (impossible) replicas of each segment to be kept up
    noDowntime.setProperty(RebalanceUserConfigConstants.MIN_REPLICAS_TO_KEEPUP_FOR_NODOWNTIME, 2);

    final TableRebalancer rebalancer = new TableRebalancer(null, null, null);
    final Map<String, String> targetSegmentInstancesMap = targetIdealState.getInstanceStateMap(segmentId);

    // STEP 1
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // After step 1, let's assume that
    // toUpdateIdealState : { SEGMENT1 : { host1 : online, host5 : online, host3 : online } }
    // Essentially, host2 from current state got replaced with host5
    // Now, what we are checking is that between toUpdate and target ideal state,
    // there are exactly 2 common hosts since we already had 1 common host to begin with
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 2, true);
    currentIdealState = toUpdateIdealState;

    // STEP 2
    rebalancer.updateSegmentIfNeeded(segmentId, currentIdealState.getRecord().getMapField(segmentId),
        targetSegmentInstancesMap, toUpdateIdealState, noDowntime);
    // Now since the minimum number of serving replicas we asked for are 2,
    // we don't need to update the current ideal state incrementally. Since
    // the common hosts (host1 and host5) satisfy the requirement of minimum
    // 2 serving replicas, the ideal state can be set to target at one go.
    // Now, what we are checking is that between toUpdate and target ideal state,
    // all hosts are in common
    verifyStateForMinReplicaConstraint(toUpdateIdealState, targetIdealState, 3, true);

    TableRebalancer.RebalancerStats rebalancerStats = rebalancer.getRebalancerStats();
    // STEP 1 incremental transition
    Assert.assertEquals(1, rebalancerStats.getIncrementalTransitions());
    // STEP 2 -- final direct transition
    Assert.assertEquals(1, rebalancerStats.getDirectTransitions());
  }

  private void verifyStateForMinReplicaConstraint(
      final IdealState updated, final IdealState target,
      final int same, final boolean sizeCheck) {
    Set<String> updatedSegmentInstances = updated.getInstanceStateMap(segmentId).keySet();
    Set<String> currentSegmentInstances = target.getInstanceStateMap(segmentId).keySet();
    if (sizeCheck) {
      Assert.assertEquals(updatedSegmentInstances.size(), currentSegmentInstances.size());
    }
    int count = 0;
    for (String updatedHost : updatedSegmentInstances) {
      if (currentSegmentInstances.contains(updatedHost)) {
        ++count;
      }
    }
    Assert.assertEquals(count, same);
  }
}
