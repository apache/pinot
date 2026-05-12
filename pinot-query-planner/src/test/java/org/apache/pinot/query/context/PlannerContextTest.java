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
package org.apache.pinot.query.context;

import java.util.Map;
import org.apache.calcite.plan.Context;
import org.apache.pinot.query.QueryEnvironment;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


/**
 * Unit tests for {@link PlannerContext} as a Calcite {@link Context}.
 *
 * <p>Verifies that Calcite rules can retrieve both {@link PlannerContext} and the backward-compatible
 * {@link QueryEnvironment.Config} via {@code unwrap()}, and that the opt/trait planners expose the
 * same context.
 */
public class PlannerContextTest {

  @Test
  public void testUnwrapReturnsSelfForPlannerContext() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of("k", "v"), config);

    Assert.assertSame(ctx.unwrap(PlannerContext.class), ctx);
  }

  @Test
  public void testUnwrapReturnsSelfForContextInterface() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    Assert.assertSame(ctx.unwrap(Context.class), ctx);
  }

  @Test
  public void testUnwrapDelegatesToEnvConfigForBackwardCompat() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    Assert.assertSame(ctx.unwrap(QueryEnvironment.Config.class), config,
        "unwrap(QueryEnvironment.Config) must return the configured envConfig for backward compatibility");
  }

  @Test
  public void testUnwrapReturnsNullForUnknownType() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    Assert.assertNull(ctx.unwrap(String.class));
  }

  @Test
  public void testOptPlannerContextExposesPlannerContext() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of("opt", "true"), config);

    PlannerContext fromPlanner = ctx.getRelOptPlanner().getContext().unwrap(PlannerContext.class);
    Assert.assertSame(fromPlanner, ctx,
        "opt planner context should expose PlannerContext via unwrap");
  }

  @Test
  public void testTraitPlannerContextExposesEnvConfigForBackwardCompat() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    QueryEnvironment.Config fromPlanner =
        ctx.getRelTraitPlanner().getContext().unwrap(QueryEnvironment.Config.class);
    Assert.assertSame(fromPlanner, config,
        "trait planner context must still expose QueryEnvironment.Config for existing rules");
  }
}
