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
package org.apache.pinot.query;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.query.planner.rules.DefaultRuleSetCustomizer;
import org.apache.pinot.query.planner.rules.PinotRuleSet;
import org.apache.pinot.query.planner.spi.Phase;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Checks that query planning stops once the planning thread is interrupted. The broker interrupts the planning
/// thread when planning times out, and so does terminating the query.
public class QueryPlanningCancellationTest {
  private static final String QUERY = "SELECT col1, col3 FROM a WHERE col3 > 0";

  @Test
  public void testPlanningStopsWhenPlanningTaskIsCancelled()
      throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      // Like MultiStageBrokerRequestHandler when planning times out
      assertPlanningStops(executor, future -> future.cancel(true));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testPlanningStopsWhenQueryIsTerminated()
      throws Exception {
    try (QueryThreadContext ignore = QueryThreadContext.openForMseTest()) {
      // Like the broker's compile executor, this executor registers its tasks with the query, so that terminating the
      // query cancels them
      ExecutorService executor = QueryThreadContext.contextAwareExecutorService(Executors.newSingleThreadExecutor());
      try {
        assertPlanningStops(executor, future -> QueryThreadContext.get().getExecutionContext()
            .terminate(QueryErrorCode.QUERY_CANCELLATION, "Query is terminated"));
      } finally {
        executor.shutdownNow();
      }
    }
  }

  /// Plans [#QUERY] on the executor, and cancels planning with the canceller while a rule runs. Then checks that no
  /// other rule fires, and that planning fails.
  private static void assertPlanningStops(ExecutorService executor, Consumer<Future<?>> canceller)
      throws Exception {
    BlockingRule blockingRule = new BlockingRule();
    QueryEnvironment queryEnvironment = createQueryEnvironment(blockingRule);
    CompletableFuture<Throwable> planningError = new CompletableFuture<>();
    Future<?> future = executor.submit(() -> {
      try (QueryEnvironment.CompiledQuery ignored = queryEnvironment.compile(QUERY)) {
        planningError.complete(null);
      } catch (Throwable t) {
        planningError.complete(t);
      }
    });
    try {
      assertTrue(blockingRule._fired.await(60, TimeUnit.SECONDS),
          "Planning did not fire the blocking rule, planning error: " + planningError.getNow(null));
      canceller.accept(future);
    } finally {
      // Release the rule also when the test fails, so that the planning thread does not stay blocked
      blockingRule._released.countDown();
    }

    Throwable error = planningError.get(60, TimeUnit.SECONDS);
    assertNotNull(error, "Planning must fail after its thread is interrupted");
    assertNotNull(ExceptionUtils.throwableOfType(error, EarlyTerminationException.class),
        "Planning must fail because its thread is interrupted, but failed with: " + error);
    // Without the interrupt, the rule fires once for each node of the plan
    assertEquals(blockingRule._numFirings.get(), 1, "No rule must fire after the planning thread is interrupted");
  }

  private static QueryEnvironment createQueryEnvironment(RelOptRule extraRule) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    factory.registerTable(QueryEnvironmentTestBase.TABLE_SCHEMAS.get("a_REALTIME"), "a_REALTIME");
    PinotRuleSet ruleSet = new PinotRuleSet(List.of(new DefaultRuleSetCustomizer(), (phase, rules) -> {
      if (phase == Phase.BASIC) {
        rules.add(0, extraRule);
      }
    }));
    return new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(-1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(factory.buildTableCache())
        .ruleSet(ruleSet)
        .build());
  }

  /// Rule that matches every node, and that blocks the first time it fires until the test releases it. Like Calcite
  /// code, it ignores interrupts while it runs.
  private static class BlockingRule extends RelRule<BlockingRule.Config> {
    final CountDownLatch _fired = new CountDownLatch(1);
    final CountDownLatch _released = new CountDownLatch(1);
    final AtomicInteger _numFirings = new AtomicInteger();

    BlockingRule() {
      super(new Config(b -> b.operand(RelNode.class).anyInputs()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      if (_numFirings.incrementAndGet() == 1) {
        _fired.countDown();
        // Keeps the interrupt status set when it returns
        Uninterruptibles.awaitUninterruptibly(_released);
      }
    }

    record Config(OperandTransform operandSupplier) implements RelRule.Config {
      @Override
      public String description() {
        return "BlockingRule";
      }

      @Override
      public RelOptRule toRule() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Config withRelBuilderFactory(RelBuilderFactory factory) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Config withDescription(String description) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Config withOperandSupplier(OperandTransform transform) {
        throw new UnsupportedOperationException();
      }
    }
  }
}
