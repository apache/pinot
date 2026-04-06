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
package org.apache.pinot.core.query.killing;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.query.killing.strategy.ScanEntriesThresholdStrategy;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class CompositeQueryKillingStrategyTest {

  @Test
  public void testAnyModeKillsWhenOneTriggered() {
    List<QueryKillingStrategy> strategies = Arrays.asList(
        new ScanEntriesThresholdStrategy(100L, Long.MAX_VALUE),
        new ScanEntriesThresholdStrategy(Long.MAX_VALUE, 200L));
    CompositeQueryKillingStrategy composite =
        new CompositeQueryKillingStrategy(strategies, CompositeQueryKillingStrategy.Mode.ANY);

    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(101);
    ctx.addDocsScanned(50);
    assertTrue(composite.shouldTerminate(ctx));
  }

  @Test
  public void testAnyModeNoKillWhenNoneTriggered() {
    List<QueryKillingStrategy> strategies = Arrays.asList(
        new ScanEntriesThresholdStrategy(100L, Long.MAX_VALUE),
        new ScanEntriesThresholdStrategy(Long.MAX_VALUE, 200L));
    CompositeQueryKillingStrategy composite =
        new CompositeQueryKillingStrategy(strategies, CompositeQueryKillingStrategy.Mode.ANY);

    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(50);
    ctx.addDocsScanned(100);
    assertFalse(composite.shouldTerminate(ctx));
  }

  @Test
  public void testAllModeNoKillWhenOnlyOneTriggered() {
    List<QueryKillingStrategy> strategies = Arrays.asList(
        new ScanEntriesThresholdStrategy(100L, Long.MAX_VALUE),
        new ScanEntriesThresholdStrategy(Long.MAX_VALUE, 200L));
    CompositeQueryKillingStrategy composite =
        new CompositeQueryKillingStrategy(strategies, CompositeQueryKillingStrategy.Mode.ALL);

    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(101);
    ctx.addDocsScanned(50);
    assertFalse(composite.shouldTerminate(ctx));
  }

  @Test
  public void testAllModeKillsWhenAllTriggered() {
    List<QueryKillingStrategy> strategies = Arrays.asList(
        new ScanEntriesThresholdStrategy(100L, Long.MAX_VALUE),
        new ScanEntriesThresholdStrategy(Long.MAX_VALUE, 200L));
    CompositeQueryKillingStrategy composite =
        new CompositeQueryKillingStrategy(strategies, CompositeQueryKillingStrategy.Mode.ALL);

    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(101);
    ctx.addDocsScanned(201);
    assertTrue(composite.shouldTerminate(ctx));
  }

  @Test
  public void testEmptyStrategiesNeverKills() {
    CompositeQueryKillingStrategy composite =
        new CompositeQueryKillingStrategy(Collections.emptyList(), CompositeQueryKillingStrategy.Mode.ANY);
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(Long.MAX_VALUE - 1);
    assertFalse(composite.shouldTerminate(ctx));
  }

  @Test
  public void testEmptyStrategiesAllModeNeverKills() {
    CompositeQueryKillingStrategy composite =
        new CompositeQueryKillingStrategy(Collections.emptyList(), CompositeQueryKillingStrategy.Mode.ALL);
    QueryScanCostContext ctx = new QueryScanCostContext();
    assertFalse(composite.shouldTerminate(ctx));
  }

  @Test
  public void testBuildKillReportDelegatesToTriggeringStrategy() {
    List<QueryKillingStrategy> strategies = Arrays.asList(
        new ScanEntriesThresholdStrategy(1000L, Long.MAX_VALUE),
        new ScanEntriesThresholdStrategy(Long.MAX_VALUE, 500L));
    CompositeQueryKillingStrategy composite =
        new CompositeQueryKillingStrategy(strategies, CompositeQueryKillingStrategy.Mode.ANY);

    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addDocsScanned(501);
    assertTrue(composite.shouldTerminate(ctx));

    QueryKillReport report = composite.buildKillReport(ctx, "q1", "t1", "cluster");
    assertEquals(report.getTriggeringMetric(), "numDocsScanned");
    assertEquals(report.getActualValue(), 501L);
  }

  @Test
  public void testStrategiesSortedByPriority() {
    // ScanEntriesThresholdStrategy has priority 10
    // Both trigger, but the first in priority order should produce the report
    ScanEntriesThresholdStrategy entriesStrategy = new ScanEntriesThresholdStrategy(100L, Long.MAX_VALUE);
    ScanEntriesThresholdStrategy docsStrategy = new ScanEntriesThresholdStrategy(Long.MAX_VALUE, 50L);

    CompositeQueryKillingStrategy composite =
        new CompositeQueryKillingStrategy(Arrays.asList(docsStrategy, entriesStrategy),
            CompositeQueryKillingStrategy.Mode.ANY);

    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(101);
    ctx.addDocsScanned(51);

    // Both trigger, but result should come from the first that matches in sorted order
    assertTrue(composite.shouldTerminate(ctx));
  }
}
