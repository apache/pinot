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
package org.apache.pinot.core.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CombinePlanNodeTest {
  private final QueryContext _queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");
  private final ExecutorService _executorService = Executors.newFixedThreadPool(10);

  /**
   * Tests that the tasks are executed as expected in parallel mode.
   */
  @Test
  public void testParallelExecution() {
    AtomicInteger count = new AtomicInteger(0);

    Random rand = new Random();
    for (int i = 0; i < 5; i++) {
      count.set(0);
      int numPlans = rand.nextInt(5000);
      List<PlanNode> planNodes = new ArrayList<>();
      for (int index = 0; index < numPlans; index++) {
        planNodes.add(() -> {
          count.incrementAndGet();
          return null;
        });
      }
      _queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
      CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, _queryContext, _executorService, null);
      combinePlanNode.run();
      Assert.assertEquals(numPlans, count.get());
    }
  }

  @Test
  public void testSlowPlanNode() {
    AtomicBoolean notInterrupted = new AtomicBoolean();

    List<PlanNode> planNodes = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      planNodes.add(() -> {
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          // Thread should be interrupted
          throw new RuntimeException(e);
        }
        notInterrupted.set(true);
        return null;
      });
    }
    _queryContext.setEndTimeMs(System.currentTimeMillis() + 100);
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, _queryContext, _executorService, null);
    try {
      combinePlanNode.run();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof TimeoutException);
      Assert.assertFalse(notInterrupted.get());
      return;
    }
    // Fail.
    Assert.fail();
  }

  @Test
  public void testPlanNodeThrowException() {
    List<PlanNode> planNodes = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      planNodes.add(() -> {
        throw new RuntimeException("Inner exception message.");
      });
    }
    _queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, _queryContext, _executorService, null);
    try {
      combinePlanNode.run();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getCause().getMessage(), "java.lang.RuntimeException: Inner exception message.");
      return;
    }
    // Fail.
    Assert.fail();
  }

  @Test
  public void testCancelPlanNode() {
    CountDownLatch ready = new CountDownLatch(20);
    List<PlanNode> planNodes = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      planNodes.add(() -> {
        ready.countDown();
        return null;
      });
    }
    // This planNode will keep the planning running and wait to be cancelled.
    CountDownLatch hold = new CountDownLatch(1);
    planNodes.add(() -> {
      try {
        hold.await();
        return null;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    _queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, _queryContext, _executorService, null);
    AtomicReference<Exception> exp = new AtomicReference<>();
    ExecutorService combineExecutor = Executors.newSingleThreadExecutor();
    try {
      Future<?> future = combineExecutor.submit(() -> {
        try {
          return combinePlanNode.run();
        } catch (Exception e) {
          exp.set(e);
          throw e;
        }
      });
      ready.await();
      // At this point, the combinePlanNode is or will be waiting on future.get() for all sub planNodes, and the
      // waiting can be cancelled as below.
      future.cancel(true);
    } catch (Exception e) {
      Assert.fail();
    } finally {
      combineExecutor.shutdownNow();
    }
    TestUtils.waitForCondition((aVoid) -> exp.get() instanceof QueryCancelledException, 10_000,
        "Should have been cancelled");
  }
}
