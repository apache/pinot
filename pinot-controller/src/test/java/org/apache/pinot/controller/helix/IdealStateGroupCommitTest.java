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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.helix.IdealStateGroupCommit;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class IdealStateGroupCommitTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateGroupCommit.class);
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String TABLE_NAME = "potato_OFFLINE";
  private static final int NUM_UPDATES = 2400;

  @BeforeClass
  public void setUp()
      throws Exception {
    TEST_INSTANCE.setupSharedStateAndValidate();

    IdealState idealState = new IdealState(TABLE_NAME);
    idealState.setStateModelDefRef("OnlineOffline");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setReplicas("1");
    idealState.setNumPartitions(0);
    TEST_INSTANCE.getHelixAdmin()
        .addResource(TEST_INSTANCE.getHelixClusterName(), TABLE_NAME, idealState);
  }

  @AfterClass
  public void tearDown() {
    TEST_INSTANCE.cleanup();
  }

  @Test
  public void testGroupCommit()
      throws InterruptedException {
    final IdealStateGroupCommit commit = new IdealStateGroupCommit();
    ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(400);
    for (int i = 0; i < NUM_UPDATES; i++) {
      Runnable runnable = new IdealStateUpdater(TEST_INSTANCE.getHelixManager(), commit, TABLE_NAME, i);
      newFixedThreadPool.submit(runnable);
    }
    IdealState idealState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), TABLE_NAME);
    while (idealState.getNumPartitions() < NUM_UPDATES) {
      Thread.sleep(500);
      idealState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), TABLE_NAME);
    }
    Assert.assertEquals(idealState.getNumPartitions(), NUM_UPDATES);
    ControllerMetrics controllerMetrics = ControllerMetrics.get();
    long idealStateUpdateSuccessCount =
        controllerMetrics.getMeteredTableValue(TABLE_NAME, ControllerMeter.IDEAL_STATE_UPDATE_SUCCESS).count();
    Assert.assertTrue(idealStateUpdateSuccessCount < NUM_UPDATES);
    LOGGER.info("{} IdealState update are successfully commited with {} times zk updates.", NUM_UPDATES,
        idealStateUpdateSuccessCount);
  }
}

class IdealStateUpdater implements Runnable {
  private final HelixManager _helixManager;
  private final IdealStateGroupCommit _commit;
  private final String _tableName;
  private final int _i;

  public IdealStateUpdater(HelixManager helixManager, IdealStateGroupCommit commit, String tableName, int i) {
    _helixManager = helixManager;
    _commit = commit;
    _tableName = tableName;
    _i = i;
  }

  @Override
  public void run() {
    _commit.commit(_helixManager, _tableName, new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        idealState.setPartitionState("test_id" + _i, "test_id" + _i, "ONLINE");
        return idealState;
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f), false);
    HelixHelper.getTableIdealState(_helixManager, _tableName);
  }
}
