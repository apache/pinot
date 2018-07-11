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
package com.linkedin.pinot.controller.helix;

import com.google.common.base.Function;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for HelixHelper. This is in pinot-controller mostly to have the necessary test fixtures.
 */
public class HelixHelperTest extends ControllerTest {
  public static final String RESOURCE_NAME = "potato_OFFLINE";
  public static final String INSTANCE_NAME = "Server_1.2.3.4_1234";

  @BeforeClass
  public void setUp() {
    startZk();
    startController();
  }

  /**
   * Regression test for large ideal state updates failing silently
   */
  @Test
  public void testWriteLargeIdealState() {
    final int numSegments = 20000;

    IdealState idealState = new IdealState(RESOURCE_NAME);
    idealState.setStateModelDefRef("OnlineOffline");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setReplicas("0");
    String helixClusterName = getHelixClusterName();
    _helixAdmin.addResource(helixClusterName, RESOURCE_NAME, idealState);

    HelixHelper.updateIdealState(_helixManager, RESOURCE_NAME, new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        Assert.assertNotNull(idealState);
        for (int i = 0; i < numSegments; i++) {
          idealState.setPartitionState("segment_" + i, INSTANCE_NAME, "ONLINE");
        }
        return idealState;
      }
    }, RetryPolicies.noDelayRetryPolicy(1));

    IdealState resourceIdealState = _helixAdmin.getResourceIdealState(helixClusterName, RESOURCE_NAME);
    for (int i = 0; i < numSegments; i++) {
      Assert.assertEquals(resourceIdealState.getInstanceStateMap("segment_" + i).get(INSTANCE_NAME), "ONLINE");
    }
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
