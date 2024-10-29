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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class IdealStateGroupCommitTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateGroupCommitTest.class);
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();
  private static final String TABLE_NAME_PREFIX = "potato_";

  private static final int SYSTEM_MULTIPLIER = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);

  private static final int NUM_PROCESSORS = 5 * SYSTEM_MULTIPLIER;
  private static final int NUM_UPDATES = 100 * SYSTEM_MULTIPLIER;
  private static final int NUM_TABLES = 20;

  private ExecutorService _executorService;

  @BeforeClass
  public void setUp()
      throws Exception {
    LOGGER.info("Starting IdealStateGroupCommitTest with SYSTEM_MULTIPLIER: {}", SYSTEM_MULTIPLIER);
    TEST_INSTANCE.setupSharedStateAndValidate();
    _executorService = Executors.newFixedThreadPool(4);
  }

  @BeforeMethod
  public void beforeTest() {
    for (int i = 0; i < NUM_UPDATES; i++) {
      String tableName = TABLE_NAME_PREFIX + i + "_OFFLINE";
      IdealState idealState = new IdealState(tableName);
      idealState.setStateModelDefRef("OnlineOffline");
      idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
      idealState.setReplicas("1");
      idealState.setNumPartitions(0);
      TEST_INSTANCE.getHelixAdmin().addResource(TEST_INSTANCE.getHelixClusterName(), tableName, idealState);
      ControllerMetrics.get().removeTableMeter(tableName, ControllerMeter.IDEAL_STATE_UPDATE_SUCCESS);
    }
  }

  @AfterMethod
  public void afterTest() {
    for (int i = 0; i < NUM_UPDATES; i++) {
      String tableName = TABLE_NAME_PREFIX + i + "_OFFLINE";
      TEST_INSTANCE.getHelixAdmin().dropResource(TEST_INSTANCE.getHelixClusterName(), tableName);
    }
  }

  @AfterClass
  public void tearDown() {
    _executorService.shutdown();
    TEST_INSTANCE.cleanup();
  }

  @Test(invocationCount = 5)
  public void testGroupCommit()
      throws InterruptedException {
    List<IdealStateGroupCommit> groupCommitList = new ArrayList<>();
    for (int i = 0; i < NUM_PROCESSORS; i++) {
      groupCommitList.add(new IdealStateGroupCommit());
    }
    for (int i = 0; i < NUM_UPDATES; i++) {
      for (int j = 0; j < NUM_TABLES; j++) {
        String tableName = TABLE_NAME_PREFIX + j + "_OFFLINE";
        IdealStateGroupCommit commit = groupCommitList.get(new Random().nextInt(NUM_PROCESSORS));
        Runnable runnable = new IdealStateUpdater(TEST_INSTANCE.getHelixManager(), commit, tableName, i);
        _executorService.submit(runnable);
      }
    }
    for (int i = 0; i < NUM_TABLES; i++) {
      String tableName = TABLE_NAME_PREFIX + i + "_OFFLINE";
      IdealState idealState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), tableName);
      while (idealState.getNumPartitions() < NUM_UPDATES) {
        Thread.sleep(500);
        idealState = HelixHelper.getTableIdealState(TEST_INSTANCE.getHelixManager(), tableName);
      }
      Assert.assertEquals(idealState.getNumPartitions(), NUM_UPDATES);
      ControllerMetrics controllerMetrics = ControllerMetrics.get();
      long idealStateUpdateSuccessCount =
          controllerMetrics.getMeteredTableValue(tableName, ControllerMeter.IDEAL_STATE_UPDATE_SUCCESS).count();
      Assert.assertTrue(idealStateUpdateSuccessCount <= NUM_UPDATES);
      LOGGER.info("{} IdealState update are successfully commited with {} times zk updates.", NUM_UPDATES,
          idealStateUpdateSuccessCount);
    }
  }
}

class IdealStateUpdater implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateGroupCommitTest.class);

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
    Function<IdealState, IdealState> updater = new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        idealState.setPartitionState("test_id" + _i, "test_id" + _i, "ONLINE");
        return idealState;
      }
    };

    while (true) {
      try {
        if (_commit.commit(_helixManager, _tableName, updater, RetryPolicies.noDelayRetryPolicy(1), false) != null) {
          break;
        }
      } catch (Throwable e) {
        LOGGER.warn("IdealState updater {} failed to commit.", _i, e);
      }
    }
  }
}
