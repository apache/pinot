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
package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.Assert;
import org.testng.annotations.Test;


public class CombinePlanNodeTest {
  private ExecutorService _executorService = Executors.newFixedThreadPool(10);

  /**
   * Tests that the tasks are executed as expected in parallel mode.
   */
  @Test
  public void testParallelExecution() {
    AtomicInteger count = new AtomicInteger(0);
    int numPlans = 99;
    List<PlanNode> planNodes = new ArrayList<>();
    for (int i = 0; i < numPlans; i++) {
      planNodes.add(new PlanNode() {
        @Override
        public Operator run() {
          count.incrementAndGet();
          return null;
        }

        @Override
        public void showTree(String prefix) {
        }
      });
    }
    CombinePlanNode combinePlanNode =
        new CombinePlanNode(planNodes, new BrokerRequest(), _executorService, 1000,
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT);
    combinePlanNode.run();
    Assert.assertEquals(numPlans, count.get());
  }

  @Test
  public void testSlowPlanNode() {
    // Warning: this test is slow (take 10 seconds).

    List<PlanNode> planNodes = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      planNodes.add(new PlanNode() {
        @Override
        public Operator run() {
          try {
            Thread.sleep(20000);
          } catch (InterruptedException e) {
            // Ignored.
          }
          return null;
        }

        @Override
        public void showTree(String prefix) {
        }
      });
    }
    CombinePlanNode combinePlanNode =
        new CombinePlanNode(planNodes, null, _executorService, 0, InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT);
    try {
      combinePlanNode.run();
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getCause().toString(), "java.util.concurrent.TimeoutException");
      return;
    }
    // Fail.
    Assert.fail();
  }

  @Test
  public void testPlanNodeThrowException() {
    List<PlanNode> planNodes = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      planNodes.add(new PlanNode() {
        @Override
        public Operator run() {
          throw new RuntimeException("Inner exception message.");
        }

        @Override
        public void showTree(String prefix) {
        }
      });
    }
    CombinePlanNode combinePlanNode =
        new CombinePlanNode(planNodes, null, _executorService, 0, InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT);
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
