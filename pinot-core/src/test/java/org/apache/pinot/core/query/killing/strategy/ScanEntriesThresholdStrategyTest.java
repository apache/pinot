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
package org.apache.pinot.core.query.killing.strategy;

import org.apache.pinot.core.query.killing.QueryKillReport;
import org.apache.pinot.core.query.killing.QueryKillingStrategy;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ScanEntriesThresholdStrategyTest {

  @Test
  public void testBelowThresholdDoesNotKill() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(100_000_000L, 10_000_000L);
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(50_000_000);
    ctx.addDocsScanned(5_000_000);
    assertFalse(strategy.shouldTerminate(ctx));
  }

  @Test
  public void testAtThresholdDoesNotKill() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(100_000_000L, 10_000_000L);
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(100_000_000);
    assertFalse(strategy.shouldTerminate(ctx));
  }

  @Test
  public void testAboveEntriesScannedThresholdKills() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(100_000_000L, Long.MAX_VALUE);
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(100_000_001);
    assertTrue(strategy.shouldTerminate(ctx));
  }

  @Test
  public void testAboveDocsScannedThresholdKills() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(Long.MAX_VALUE, 10_000_000L);
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addDocsScanned(10_000_001);
    assertTrue(strategy.shouldTerminate(ctx));
  }

  @Test
  public void testMaxValueThresholdEffectivelyDisables() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(Long.MAX_VALUE, Long.MAX_VALUE);
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(999_999_999_999L);
    ctx.addDocsScanned(999_999_999_999L);
    assertFalse(strategy.shouldTerminate(ctx));
  }

  @Test
  public void testBuildKillReportForEntriesScanned() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(100_000_000L, Long.MAX_VALUE);
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addEntriesScannedInFilter(150_000_000);

    QueryKillReport report = strategy.buildKillReport(ctx, "q1", "myTable", "cluster");
    assertEquals(report.getTriggeringMetric(), "numEntriesScannedInFilter");
    assertEquals(report.getActualValue(), 150_000_000L);
    assertEquals(report.getThresholdValue(), 100_000_000L);
  }

  @Test
  public void testBuildKillReportForDocsScanned() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(Long.MAX_VALUE, 10_000_000L);
    QueryScanCostContext ctx = new QueryScanCostContext();
    ctx.addDocsScanned(15_000_000);

    QueryKillReport report = strategy.buildKillReport(ctx, "q2", "myTable", "table:myTable");
    assertEquals(report.getTriggeringMetric(), "numDocsScanned");
    assertEquals(report.getActualValue(), 15_000_000L);
    assertEquals(report.getThresholdValue(), 10_000_000L);
  }

  @Test
  public void testPriority() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(100L, 100L);
    assertEquals(strategy.priority(), 10);
  }

  @Test
  public void testEitherMetricCanTrigger() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(100L, 200L);

    // Only entries exceeds
    QueryScanCostContext ctx1 = new QueryScanCostContext();
    ctx1.addEntriesScannedInFilter(101);
    ctx1.addDocsScanned(50);
    assertTrue(strategy.shouldTerminate(ctx1));

    // Only docs exceeds
    QueryScanCostContext ctx2 = new QueryScanCostContext();
    ctx2.addEntriesScannedInFilter(50);
    ctx2.addDocsScanned(201);
    assertTrue(strategy.shouldTerminate(ctx2));
  }

  // --- forQuery() table override tests ---

  @Test
  public void testForQueryReturnsThisWhenNoOverrides() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(100L, 200L);
    QueryKillingStrategy result = strategy.forQuery(null, null);
    assertTrue(result == strategy, "forQuery(null) should return same instance");

    QueryConfig emptyConfig = new QueryConfig(null, null, null, null, null, null, null, null, null);
    result = strategy.forQuery(emptyConfig, null);
    assertTrue(result == strategy, "forQuery with no scan overrides should return same instance");
  }

  @Test
  public void testForQueryAppliesTableOverride() {
    ScanEntriesThresholdStrategy strategy = new ScanEntriesThresholdStrategy(100L, 200L);
    QueryConfig queryConfig = new QueryConfig(null, null, null, null, null, null, 500L, null, null);

    QueryKillingStrategy result = strategy.forQuery(queryConfig, null);
    assertTrue(result != strategy, "forQuery with override should return new instance");
    assertTrue(result instanceof ScanEntriesThresholdStrategy);

    ScanEntriesThresholdStrategy overridden = (ScanEntriesThresholdStrategy) result;
    assertEquals(overridden.getMaxEntriesScannedInFilter(), 500L);
    assertEquals(overridden.getMaxDocsScanned(), 200L); // inherited from original
  }

  // --- Factory tests ---

  @Test
  public void testFactoryCreatesStrategyWithThresholds() {
    java.util.Map<String, Object> props = new java.util.HashMap<>();
    props.put("accounting.scan.based.killing.enabled", true);
    props.put("accounting.scan.based.killing.max.entries.scanned.in.filter", 500_000_000L);
    props.put("accounting.scan.based.killing.max.docs.scanned", 50_000_000L);
    org.apache.pinot.spi.env.PinotConfiguration pinotConfig = new org.apache.pinot.spi.env.PinotConfiguration(props);
    org.apache.pinot.core.accounting.QueryMonitorConfig config =
        new org.apache.pinot.core.accounting.QueryMonitorConfig(pinotConfig, 1_000_000_000L);

    ScanEntriesThresholdStrategy.Factory factory = new ScanEntriesThresholdStrategy.Factory();
    QueryKillingStrategy strategy = factory.create(config);
    assertNotNull(strategy);
    assertTrue(strategy instanceof ScanEntriesThresholdStrategy);
  }

  @Test
  public void testFactoryReturnsNullWhenNoThresholds() {
    java.util.Map<String, Object> props = new java.util.HashMap<>();
    props.put("accounting.scan.based.killing.enabled", true);
    // No thresholds set — defaults are Long.MAX_VALUE
    org.apache.pinot.spi.env.PinotConfiguration pinotConfig = new org.apache.pinot.spi.env.PinotConfiguration(props);
    org.apache.pinot.core.accounting.QueryMonitorConfig config =
        new org.apache.pinot.core.accounting.QueryMonitorConfig(pinotConfig, 1_000_000_000L);

    ScanEntriesThresholdStrategy.Factory factory = new ScanEntriesThresholdStrategy.Factory();
    QueryKillingStrategy strategy = factory.create(config);
    assertNull(strategy, "Factory should return null when no thresholds are configured");
  }
}
