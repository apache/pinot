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
package org.apache.pinot.core.query.scheduler.resources;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ResourceManagerTest {
  private ResourceManager _resourceManager;

  @AfterMethod
  public void tearDown() {
    if (_resourceManager != null) {
      _resourceManager.stop();
      _resourceManager = null;
    }
  }

  @Test
  public void testCanSchedule() {
    _resourceManager = getResourceManager(2, 5, 1, 3);

    SchedulerGroupAccountant accountant = mock(SchedulerGroupAccountant.class);
    when(accountant.totalReservedThreads()).thenReturn(3);
    assertFalse(_resourceManager.canSchedule(accountant));

    when(accountant.totalReservedThreads()).thenReturn(2);
    assertTrue(_resourceManager.canSchedule(accountant));
  }

  @Test
  public void testResizeThreadPoolsIncrease() {
    _resourceManager = getResourceManager(2, 4, 1, 3);
    assertEquals(_resourceManager.getNumQueryRunnerThreads(), 2);
    assertEquals(_resourceManager.getNumQueryWorkerThreads(), 4);

    _resourceManager.resizeThreadPools(4, 8);
    assertEquals(_resourceManager.getNumQueryRunnerThreads(), 4);
    assertEquals(_resourceManager.getNumQueryWorkerThreads(), 8);
    assertEquals(_resourceManager._queryRunnerPool.getCorePoolSize(), 4);
    assertEquals(_resourceManager._queryRunnerPool.getMaximumPoolSize(), 4);
    assertEquals(_resourceManager._queryWorkerPool.getCorePoolSize(), 8);
    assertEquals(_resourceManager._queryWorkerPool.getMaximumPoolSize(), 8);
  }

  @Test
  public void testResizeThreadPoolsDecrease() {
    _resourceManager = getResourceManager(8, 16, 1, 3);
    assertEquals(_resourceManager.getNumQueryRunnerThreads(), 8);
    assertEquals(_resourceManager.getNumQueryWorkerThreads(), 16);

    _resourceManager.resizeThreadPools(4, 8);
    assertEquals(_resourceManager.getNumQueryRunnerThreads(), 4);
    assertEquals(_resourceManager.getNumQueryWorkerThreads(), 8);
    assertEquals(_resourceManager._queryRunnerPool.getCorePoolSize(), 4);
    assertEquals(_resourceManager._queryRunnerPool.getMaximumPoolSize(), 4);
    assertEquals(_resourceManager._queryWorkerPool.getCorePoolSize(), 8);
    assertEquals(_resourceManager._queryWorkerPool.getMaximumPoolSize(), 8);
  }

  @Test
  public void testResizeThreadPoolsNoChange() {
    _resourceManager = getResourceManager(2, 4, 1, 3);
    _resourceManager.resizeThreadPools(2, 4);
    assertEquals(_resourceManager.getNumQueryRunnerThreads(), 2);
    assertEquals(_resourceManager.getNumQueryWorkerThreads(), 4);
  }

  @Test
  public void testResizeThreadPoolsInvalidValues() {
    _resourceManager = getResourceManager(2, 4, 1, 3);

    _resourceManager.resizeThreadPools(0, 4);
    assertEquals(_resourceManager.getNumQueryRunnerThreads(), 2);
    assertEquals(_resourceManager.getNumQueryWorkerThreads(), 4);

    _resourceManager.resizeThreadPools(2, -1);
    assertEquals(_resourceManager.getNumQueryRunnerThreads(), 2);
    assertEquals(_resourceManager.getNumQueryWorkerThreads(), 4);
  }

  @Test
  public void testResizeListenerCalledOnResize() {
    _resourceManager = getResourceManager(2, 4, 1, 3);
    AtomicInteger capturedRunners = new AtomicInteger(-1);
    AtomicInteger capturedWorkers = new AtomicInteger(-1);
    _resourceManager.addThreadPoolResizeListener((newRunnerThreads, newWorkerThreads) -> {
      capturedRunners.set(newRunnerThreads);
      capturedWorkers.set(newWorkerThreads);
    });

    _resourceManager.resizeThreadPools(6, 12);
    assertEquals(capturedRunners.get(), 6);
    assertEquals(capturedWorkers.get(), 12);
  }

  @Test
  public void testResizeListenerNotCalledOnNoChange() {
    _resourceManager = getResourceManager(2, 4, 1, 3);
    AtomicInteger callCount = new AtomicInteger(0);
    _resourceManager.addThreadPoolResizeListener((newRunnerThreads, newWorkerThreads) -> callCount.incrementAndGet());

    _resourceManager.resizeThreadPools(2, 4);
    assertEquals(callCount.get(), 0);
  }

  @Test
  public void testResizeListenerNotCalledOnInvalidValues() {
    _resourceManager = getResourceManager(2, 4, 1, 3);
    AtomicInteger callCount = new AtomicInteger(0);
    _resourceManager.addThreadPoolResizeListener((newRunnerThreads, newWorkerThreads) -> callCount.incrementAndGet());

    _resourceManager.resizeThreadPools(0, 4);
    assertEquals(callCount.get(), 0);

    _resourceManager.resizeThreadPools(2, -1);
    assertEquals(callCount.get(), 0);
  }

  private ResourceManager getResourceManager(int runners, int workers, final int softLimit, final int hardLimit) {

    return new ResourceManager(getConfig(runners, workers)) {

      @Override
      public QueryExecutorService getExecutorService(ServerQueryRequest query, SchedulerGroupAccountant accountant) {
        return new QueryExecutorService() {
          @Override
          public void execute(Runnable command) {
            getQueryWorkers().execute(command);
          }
        };
      }

      @Override
      public int getTableThreadsHardLimit() {
        return hardLimit;
      }

      @Override
      public int getTableThreadsSoftLimit() {
        return softLimit;
      }
    };
  }

  private PinotConfiguration getConfig(int runners, int workers) {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ResourceManager.QUERY_RUNNER_CONFIG_KEY, runners);
    properties.put(ResourceManager.QUERY_WORKER_CONFIG_KEY, workers);
    return new PinotConfiguration(properties);
  }
}
