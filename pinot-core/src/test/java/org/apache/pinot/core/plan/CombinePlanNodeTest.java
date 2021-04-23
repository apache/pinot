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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CombinePlanNodeTest {
  private final QueryContext _queryContext =
      QueryContextConverterUtils.getQueryContextFromSQL("SELECT * FROM testTable");
  private final ExecutorService _executorService = Executors.newFixedThreadPool(10);

  /**
   * Tests that the tasks are executed as expected in parallel mode.
   */
  @Test
  public void testParallelExecution() {
    AtomicInteger count = new AtomicInteger(0);

    Random rand = new Random();
    for (int i = 0; i < 5; ++i) {
      count.set(0);
      int numPlans = rand.nextInt(5000);
      List<PlanNode> planNodes = new ArrayList<>();
      for (int index = 0; index < numPlans; index++) {
        planNodes.add(() -> {
          count.incrementAndGet();
          return null;
        });
      }
      CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, _queryContext, _executorService,
          System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS,
          InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, null,
          InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD);
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
    CombinePlanNode combinePlanNode =
        new CombinePlanNode(planNodes, _queryContext, _executorService, System.currentTimeMillis() + 100,
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, null,
            InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD);
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
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, _queryContext, _executorService,
        System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS,
        InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, null, InstancePlanMakerImplV2.DEFAULT_GROUPBY_TRIM_THRESHOLD);
    try {
      combinePlanNode.run();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getCause().getMessage(), "java.lang.RuntimeException: Inner exception message.");
      return;
    }
    // Fail.
    Assert.fail();
  }
}
