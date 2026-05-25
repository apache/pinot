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
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


public class PlannerContextTest {

  @Test
  public void testUnwrapReturnsSelf() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    assertSame(ctx.unwrap(PlannerContext.class), ctx);
    assertSame(ctx.unwrap(Context.class), ctx);
  }

  @Test
  public void testUnwrapReturnsEnvConfig() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    assertSame(ctx.unwrap(QueryEnvironment.Config.class), config);
  }

  @Test
  public void testUnwrapReturnsNullForUnknownType() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    assertNull(ctx.unwrap(String.class));
  }

  @Test
  public void testPlannersExposeContextInstance() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of("k", "v"), config);

    // Both planners must expose this PlannerContext via their context, so rules can
    // call call.getPlanner().getContext().unwrap(PlannerContext.class) to read options.
    assertSame(ctx.getRelOptPlanner().getContext().unwrap(PlannerContext.class), ctx);
    assertSame(ctx.getRelTraitPlanner().getContext().unwrap(PlannerContext.class), ctx);
  }

  @Test
  public void testOptionsAreAccessibleThroughUnwrap() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    Map<String, String> options = Map.of("workerRuntime", "datafusion");
    PlannerContext ctx = PlannerContext.forTesting(options, config);

    PlannerContext unwrapped = ctx.getRelOptPlanner().getContext().unwrap(PlannerContext.class);
    assertSame(unwrapped, ctx);
    assertSame(unwrapped.getOptions(), options);
  }
}
