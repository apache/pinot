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

import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.spi.CostModelProvider;
import org.apache.pinot.query.planner.spi.QueryOptimizerProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


public class PlannerContextOptimizerTest {

  @Test
  public void testOptimizerProviderInstallsPlannerAndMetadataProviders() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    Map<String, String> options = Map.of("queryOptimizer", "cbo");

    RelOptPlanner planner = mock(RelOptPlanner.class);
    RelMetadataProvider metadataProvider = mock(RelMetadataProvider.class);
    CostModelProvider costModelProvider = mock(CostModelProvider.class);

    QueryOptimizerProvider optimizerProvider = mock(QueryOptimizerProvider.class);
    when(optimizerProvider.getMetadataProviders()).thenReturn(List.of(metadataProvider));
    when(optimizerProvider.createCostModelProvider()).thenReturn(costModelProvider);
    when(optimizerProvider.createPlanner(any(CostModelProvider.class), any(Context.class), anyList()))
        .thenReturn(planner);

    PlannerContext ctx = new PlannerContext(options, config, optimizerProvider);

    assertSame(ctx.getRelOptPlanner(), planner);
    assertEquals(ctx.getMetadataProviders(), List.of(metadataProvider));
    assertSame(ctx.unwrap(CostModelProvider.class), costModelProvider);
    verify(costModelProvider).configure(options);
  }

  @Test
  public void testMetadataProvidersEmptyWithoutOptimizer() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    assertEquals(ctx.getMetadataProviders(), List.of());
    assertNull(ctx.unwrap(CostModelProvider.class));
  }

  @Test
  public void testCloseRestoresThreadMetadataProvider() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    JaninoRelMetadataProvider original = RelMetadataQueryBase.THREAD_PROVIDERS.get();
    try {
      JaninoRelMetadataProvider saved = mock(JaninoRelMetadataProvider.class);
      RelMetadataQueryBase.THREAD_PROVIDERS.set(saved);
      ctx.rememberPreviousThreadMetadataProvider(saved);

      RelMetadataQueryBase.THREAD_PROVIDERS.set(mock(JaninoRelMetadataProvider.class));
      ctx.close();

      assertSame(RelMetadataQueryBase.THREAD_PROVIDERS.get(), saved);
    } finally {
      restoreThreadMetadataProvider(original);
    }
  }

  @Test
  public void testCloseRemovesThreadMetadataProviderWhenPreviousWasNull() {
    QueryEnvironment.Config config = mock(QueryEnvironment.Config.class);
    PlannerContext ctx = PlannerContext.forTesting(Map.of(), config);

    JaninoRelMetadataProvider original = RelMetadataQueryBase.THREAD_PROVIDERS.get();
    try {
      RelMetadataQueryBase.THREAD_PROVIDERS.remove();
      ctx.rememberPreviousThreadMetadataProvider(null);

      RelMetadataQueryBase.THREAD_PROVIDERS.set(mock(JaninoRelMetadataProvider.class));
      ctx.close();

      assertNull(RelMetadataQueryBase.THREAD_PROVIDERS.get());
    } finally {
      restoreThreadMetadataProvider(original);
    }
  }

  private static void restoreThreadMetadataProvider(JaninoRelMetadataProvider value) {
    if (value == null) {
      RelMetadataQueryBase.THREAD_PROVIDERS.remove();
    } else {
      RelMetadataQueryBase.THREAD_PROVIDERS.set(value);
    }
  }
}
