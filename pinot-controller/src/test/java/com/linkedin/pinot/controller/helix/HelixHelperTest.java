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
package com.linkedin.pinot.controller.helix;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.ControllerStarter;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Tests for HelixHelper. This is in pinot-controller mostly to have the necessary test fixtures.
 */
public class HelixHelperTest {
  public static final String CLUSTER_NAME = "HELIX_HELPER_TEST";
  public static final String RESOURCE_NAME = "potato_OFFLINE";
  public static final String INSTANCE_NAME = "Server_1.2.3.4_1234";
  private ZkStarter.ZookeeperInstance _zkInstance;
  private ControllerStarter _controller;

  @BeforeClass
  public void setUp() {
    _zkInstance = ZkStarter.startLocalZkServer();
    _controller = ControllerTestUtils.startController(CLUSTER_NAME, ZkStarter.DEFAULT_ZK_STR,
        ControllerTestUtils.getDefaultControllerConfiguration());
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.stopController(_controller);
    ZkStarter.stopLocalZkServer(_zkInstance);
  }

  /**
   * Regression test for large ideal state updates failing silently
   */
  @Test
  public void testWriteLargeIdealState() {
    final int segmentCount = 20000;

    PinotHelixResourceManager resourceManager = _controller.getHelixResourceManager();

    final IdealState idealState = new IdealState(RESOURCE_NAME);
    idealState.setStateModelDefRef("OnlineOffline");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setReplicas("0");
    resourceManager.getHelixAdmin().addResource(CLUSTER_NAME, RESOURCE_NAME, idealState);

    HelixHelper.updateIdealState(resourceManager.getHelixZkManager(), RESOURCE_NAME, new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        for(int i = 0; i < segmentCount; ++i) {
          idealState.setPartitionState("segment_" + i, INSTANCE_NAME, "ONLINE");
        }
        return idealState;
      }
    }, RetryPolicies.noDelayRetryPolicy(1));

    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

    final IdealState resourceIdealState =
        resourceManager.getHelixAdmin().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
    for (int i = 0; i < segmentCount; ++i) {
      assertEquals(resourceIdealState.getInstanceStateMap("segment_" + i).get(INSTANCE_NAME), "ONLINE");
    }
  }
}
