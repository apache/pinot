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
package org.apache.pinot.controller.helix;

import com.google.common.base.Function;
import javax.annotation.Nullable;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for HelixHelper. This is in pinot-controller mostly to have the necessary test fixtures.
 */
public class HelixHelperTest {
  public static final String RESOURCE_NAME = "potato_OFFLINE";
  public static final String INSTANCE_NAME = "Server_1.2.3.4_1234";

  @BeforeClass
  public void setUp() throws Exception {
    ControllerTestUtils.setupClusterAndValidate();

    IdealState idealState = new IdealState(RESOURCE_NAME);
    idealState.setStateModelDefRef("OnlineOffline");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setReplicas("0");
    ControllerTestUtils.getHelixAdmin().addResource(ControllerTestUtils.getHelixClusterName(), RESOURCE_NAME,
        idealState);
  }

  /**
   * Regression test for large ideal state updates failing silently
   */
  @Test
  public void testWriteLargeIdealState() {
    final int numSegments = 20000;

    HelixHelper.updateIdealState(ControllerTestUtils.getHelixManager(), RESOURCE_NAME,
        new Function<IdealState, IdealState>() {
          @Override
          public IdealState apply(@Nullable IdealState idealState) {
            Assert.assertNotNull(idealState);
            for (int i = 0; i < numSegments; i++) {
              idealState.setPartitionState("segment_" + i, INSTANCE_NAME, "ONLINE");
            }
            return idealState;
          }
        }, RetryPolicies.noDelayRetryPolicy(1));

    IdealState resourceIdealState = ControllerTestUtils.getHelixAdmin()
        .getResourceIdealState(ControllerTestUtils.getHelixClusterName(), RESOURCE_NAME);
    for (int i = 0; i < numSegments; i++) {
      Assert.assertEquals(resourceIdealState.getInstanceStateMap("segment_" + i).get(INSTANCE_NAME), "ONLINE");
    }
  }

  @Test
  public void testPermanentIdealStateUpdaterException() {
    Assert.assertTrue(catchExceptionInISUpdate(null));
    Assert.assertFalse(catchExceptionInISUpdate("TestSegment"));
  }

  private boolean catchExceptionInISUpdate(String testSegment) {
    boolean caughtException = false;
    try {
      aMethodWhichThrowsExceptionInUpdater(testSegment);
    } catch (Exception e) {
      caughtException = true;
    }
    return caughtException;
  }

  private void aMethodWhichThrowsExceptionInUpdater(String testSegment) {
    HelixHelper.updateIdealState(ControllerTestUtils.getHelixManager(), RESOURCE_NAME,
        new Function<IdealState, IdealState>() {
          @Override
          public IdealState apply(@Nullable IdealState idealState) {
            if (testSegment == null) {
              throw new HelixHelper.PermanentUpdaterException("Throwing test exception for " + testSegment);
            }
            return idealState;
          }
        }, RetryPolicies.noDelayRetryPolicy(5));
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
