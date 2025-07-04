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
package org.apache.pinot.spi.executor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadExceedStrategy;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class HardLimitExecutorTest {

  @Test
  public void testHardLimit()
      throws Exception {
    HardLimitExecutor ex = new HardLimitExecutor(1, Executors.newCachedThreadPool());
    CyclicBarrier barrier = new CyclicBarrier(2);

    try {
      ex.execute(() -> {
        try {
          barrier.await();
          Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException | BrokenBarrierException e) {
          // do nothing
        }
      });

      barrier.await();
      try {
        ex.execute(() -> {
          // do nothing
        });
        fail("Should not allow more than 1 task");
      } catch (Exception e) {
        // as expected
        assertEquals(e.getMessage(), "Tasks limit exceeded.");
      }
    } finally {
      ex.shutdownNow();
    }
  }

  @Test
  public void testHardLimitLogExceedStrategy()
      throws Exception {
    HardLimitExecutor ex = new HardLimitExecutor(1, Executors.newCachedThreadPool(), QueryThreadExceedStrategy.LOG);
    CyclicBarrier barrier = new CyclicBarrier(2);

    try {
      ex.execute(() -> {
        try {
          barrier.await();
          Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException | BrokenBarrierException e) {
          // do nothing
        }
      });

      barrier.await();

      ex.execute(() -> {
        // do nothing, we just don't want it to throw an exception
      });
    } finally {
      ex.shutdownNow();
    }
  }

  @Test
  public void testGetMultiStageExecutorHardLimit() {
    // Only cluster config is set
    Map<String, Object> configMap1 = new HashMap<>();
    configMap1.put(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, "10");
    configMap1.put(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR, "5");
    PinotConfiguration config1 = new PinotConfiguration(configMap1);
    assertEquals(HardLimitExecutor.getMultiStageExecutorHardLimit(config1), 50);

    // Only server config is set
    Map<String, Object> configMap2 = new HashMap<>();
    configMap2.put(CommonConstants.Server.CONFIG_OF_MSE_MAX_EXECUTION_THREADS, "30");
    PinotConfiguration config2 = new PinotConfiguration(configMap2);
    assertEquals(HardLimitExecutor.getMultiStageExecutorHardLimit(config2), 30);

    // Both configs are set. Server is lower. Server config prioritized.
    Map<String, Object> configMap3 = new HashMap<>();
    configMap3.put(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, "10");
    configMap3.put(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR, "5");
    configMap3.put(CommonConstants.Server.CONFIG_OF_MSE_MAX_EXECUTION_THREADS, "30");
    PinotConfiguration config3 = new PinotConfiguration(configMap3);
    assertEquals(HardLimitExecutor.getMultiStageExecutorHardLimit(config3), 30);

    // Both configs are set. Cluster is lower. Server config prioritized.
    Map<String, Object> configMap4 = new HashMap<>();
    configMap4.put(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, "10");
    configMap4.put(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR, "2");
    configMap4.put(CommonConstants.Server.CONFIG_OF_MSE_MAX_EXECUTION_THREADS, "30");
    PinotConfiguration config4 = new PinotConfiguration(configMap4);
    assertEquals(HardLimitExecutor.getMultiStageExecutorHardLimit(config4), 30);

    // No configs set, should return non-positive
    Map<String, Object> configMap5 = new HashMap<>();
    PinotConfiguration config5 = new PinotConfiguration(configMap5);
    assertEquals(HardLimitExecutor.getMultiStageExecutorHardLimit(config5), -1);
  }
}
