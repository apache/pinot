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
package org.apache.pinot.core.segment.processing.framework;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for PercentileAdaptiveSegmentNumRowProvider
 */
public class PercentileAdaptiveSegmentNumRowProviderTest {

  @Test
  public void testConstructorWithDefaults() {
    long desiredSize = 200 * 1024 * 1024; // 200MB
    int maxRows = 2_000_000;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows);

    // Should use default P75 percentile
    // With default 5000 bytes/row estimate, should calculate 200MB / 5000 = ~40,000 rows
    int initialRows = provider.getNumRows();
    assertTrue(initialRows > 0 && initialRows <= maxRows,
        "Initial row count should be positive and <= max");
    assertEquals(provider.getSegmentsProcessed(), 0);
    assertEquals(provider.getReservoirCurrentSize(), 0);
  }

  @Test
  public void testConstructorWithCustomPercentile() {
    long desiredSize = 100 * 1024 * 1024; // 100MB
    int maxRows = 1_000_000;
    int percentile = 90; // P90

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, percentile);

    // Verify construction succeeded (percentile validation happens internally)
    assertTrue(provider.getNumRows() > 0);
  }

  @Test
  public void testConstructorWithFullCustomization() {
    long desiredSize = 100 * 1024 * 1024; // 100MB
    int maxRows = 1_000_000;
    int percentile = 80;
    int reservoirSize = 500;
    double initialEstimate = 8000.0;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, percentile,
            reservoirSize, initialEstimate);

    // 100MB (104,857,600 bytes) / 8,000 bytes/row = 13,107 rows
    assertEquals(provider.getNumRows(), 13_107);
  }

  @Test
  public void testInvalidConstructorArguments() {
    // Negative desired size
    assertThrows(IllegalArgumentException.class, () ->
        new PercentileAdaptiveSegmentNumRowProvider(-1, 1_000_000));

    // Zero max rows
    assertThrows(IllegalArgumentException.class, () ->
        new PercentileAdaptiveSegmentNumRowProvider(100_000_000, 0));

    // Invalid percentile (too low)
    assertThrows(IllegalArgumentException.class, () ->
        new PercentileAdaptiveSegmentNumRowProvider(100_000_000, 1_000_000, 0));

    // Invalid percentile (too high)
    assertThrows(IllegalArgumentException.class, () ->
        new PercentileAdaptiveSegmentNumRowProvider(100_000_000, 1_000_000, 100));

    // Invalid reservoir size
    assertThrows(IllegalArgumentException.class, () ->
        new PercentileAdaptiveSegmentNumRowProvider(100_000_000, 1_000_000, 75, 0, 5000));

    // Invalid initial estimate
    assertThrows(IllegalArgumentException.class, () ->
        new PercentileAdaptiveSegmentNumRowProvider(100_000_000, 1_000_000, 75, 1000, -100));
  }

  @Test
  public void testReservoirFilling() {
    long desiredSize = 100 * 1024 * 1024;
    int maxRows = 1_000_000;
    int reservoirSize = 10; // Small reservoir for testing

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, 75, reservoirSize, 5000.0);

    assertEquals(provider.getReservoirCurrentSize(), 0);

    // Add observations up to reservoir size
    for (int i = 0; i < reservoirSize; i++) {
      provider.updateSegmentInfo(10_000, 10_000L * 5_000);
      assertEquals(provider.getReservoirCurrentSize(), i + 1);
    }

    // Reservoir should be full
    assertEquals(provider.getReservoirCurrentSize(), reservoirSize);

    // Add more observations - reservoir size should stay constant
    for (int i = 0; i < 5; i++) {
      provider.updateSegmentInfo(10_000, 10_000L * 5_000);
      assertEquals(provider.getReservoirCurrentSize(), reservoirSize,
          "Reservoir size should stay at max");
    }

    assertEquals(provider.getSegmentsProcessed(), reservoirSize + 5);
  }

  @Test
  public void testPercentileCalculationWithUniformData() {
    long desiredSize = 200 * 1024 * 1024; // 200MB
    int maxRows = 2_000_000;
    int percentile = 75;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, percentile);

    // All segments have exactly 8KB per row
    for (int i = 0; i < 20; i++) {
      provider.updateSegmentInfo(25_000, 25_000L * 8_000);
    }

    // With uniform data, all percentiles should equal the value
    double p75 = provider.getPercentileEstimate();
    assertEquals(p75, 8000.0, 0.01, "P75 should equal the uniform value");

    // Row count should be 200MB (209,715,200 bytes) / 8,000 bytes/row = 26,214 rows
    assertEquals(provider.getNumRows(), 26_214);
  }

  @Test
  public void testPercentileCalculationWithVariedData() {
    long desiredSize = 100 * 1024 * 1024; // 100MB
    int maxRows = 1_000_000;
    int percentile = 75;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, percentile);

    // Add segments with varying bytes per row: 2KB, 4KB, 8KB, 16KB
    // Pattern: 25% at 2KB, 25% at 4KB, 25% at 8KB, 25% at 16KB
    for (int i = 0; i < 10; i++) {
      provider.updateSegmentInfo(10_000, 10_000L * 2_000);  // 2KB/row
    }
    for (int i = 0; i < 10; i++) {
      provider.updateSegmentInfo(10_000, 10_000L * 4_000);  // 4KB/row
    }
    for (int i = 0; i < 10; i++) {
      provider.updateSegmentInfo(10_000, 10_000L * 8_000);  // 8KB/row
    }
    for (int i = 0; i < 10; i++) {
      provider.updateSegmentInfo(10_000, 10_000L * 16_000); // 16KB/row
    }

    // P75 should be at or near 8KB (75% of values are <= 8KB)
    double p75 = provider.getPercentileEstimate();
    assertTrue(p75 >= 6_000 && p75 <= 10_000,
        "P75 should be around 8KB for this distribution, got: " + p75);

    assertEquals(provider.getSegmentsProcessed(), 40);
  }

  @Test
  public void testPercentileRobustnessToOutliers() {
    long desiredSize = 100 * 1024 * 1024; // 100MB
    int maxRows = 1_000_000;
    int percentile = 75;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, percentile);

    // Most segments have ~5KB per row
    for (int i = 0; i < 30; i++) {
      provider.updateSegmentInfo(20_000, 20_000L * 5_000);
    }

    // Add a few outliers with very large sizes (e.g., one customer with huge sketches)
    for (int i = 0; i < 5; i++) {
      provider.updateSegmentInfo(2_000, 2_000L * 100_000); // 100KB/row - outlier!
    }

    // P75 should still be close to 5KB, not affected much by outliers
    double p75 = provider.getPercentileEstimate();
    assertTrue(p75 < 20_000,
        "P75 should not be heavily influenced by outliers, got: " + p75);

    // Overall average would be significantly higher due to outliers
    double overallAvg = provider.getOverallAverageBytesPerRow();
    assertTrue(overallAvg > p75,
        "Overall average should be higher than P75 due to outliers");
  }

  @Test
  public void testDifferentPercentiles() {
    long desiredSize = 100 * 1024 * 1024;
    int maxRows = 1_000_000;

    // Create providers with different percentiles
    PercentileAdaptiveSegmentNumRowProvider p50 =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, 50);
    PercentileAdaptiveSegmentNumRowProvider p75 =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, 75);
    PercentileAdaptiveSegmentNumRowProvider p90 =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, 90);

    // Feed same data to all: values from 1KB to 10KB
    for (int bytesPerRow = 1000; bytesPerRow <= 10_000; bytesPerRow += 1000) {
      p50.updateSegmentInfo(10_000, 10_000L * bytesPerRow);
      p75.updateSegmentInfo(10_000, 10_000L * bytesPerRow);
      p90.updateSegmentInfo(10_000, 10_000L * bytesPerRow);
    }

    // P90 should have highest estimate
    // P50 should have lowest estimate
    double est50 = p50.getPercentileEstimate();
    double est75 = p75.getPercentileEstimate();
    double est90 = p90.getPercentileEstimate();

    assertTrue(est50 < est75, "P50 should be less than P75");
    assertTrue(est75 < est90, "P75 should be less than P90");

    // Higher percentile = higher bytes/row estimate = fewer rows
    assertTrue(p90.getNumRows() < p75.getNumRows(), "P90 should suggest fewer rows");
    assertTrue(p75.getNumRows() < p50.getNumRows(), "P75 should suggest fewer rows than P50");
  }

  @Test
  public void testMaxRowsEnforcement() {
    long desiredSize = 1_000 * 1024 * 1024; // 1GB
    int maxRows = 50_000;
    int percentile = 75;

    // Very small initial estimate
    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, percentile, 1000, 100.0);

    // Should be capped at maxRows
    assertEquals(provider.getNumRows(), maxRows);

    // Even after learning, should respect max
    provider.updateSegmentInfo(maxRows, maxRows * 1000L);
    assertTrue(provider.getNumRows() <= maxRows);
  }

  @Test
  public void testMinRowsEnforcement() {
    long desiredSize = 1024; // 1KB
    int maxRows = 1_000_000;
    int percentile = 90;

    // Large initial estimate
    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, percentile, 1000, 100_000.0);

    // Should be at least 1 row
    assertTrue(provider.getNumRows() >= 1);
  }

  @Test
  public void testInvalidUpdateArguments() {
    long desiredSize = 100 * 1024 * 1024;
    int maxRows = 1_000_000;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows);

    int rowsBefore = provider.getNumRows();

    // Invalid updates should be ignored
    provider.updateSegmentInfo(0, 100_000); // Zero rows
    assertEquals(provider.getSegmentsProcessed(), 0);
    assertEquals(provider.getReservoirCurrentSize(), 0);

    provider.updateSegmentInfo(1000, 0); // Zero size
    assertEquals(provider.getSegmentsProcessed(), 0);

    provider.updateSegmentInfo(-100, 100_000); // Negative
    assertEquals(provider.getSegmentsProcessed(), 0);

    // Row count should not change
    assertEquals(provider.getNumRows(), rowsBefore);
  }

  @Test
  public void testInitialEstimateUsedBeforeSufficientData() {
    long desiredSize = 100 * 1024 * 1024;
    int maxRows = 1_000_000;
    double initialEstimate = 10_000.0;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, 75, 1000, initialEstimate);

    // Before any updates, should use initial estimate
    assertEquals(provider.getPercentileEstimate(), initialEstimate, 0.01);

    // Add just 1-2 observations (less than MIN_OBSERVATIONS_FOR_PERCENTILE)
    provider.updateSegmentInfo(10_000, 10_000L * 5_000);
    provider.updateSegmentInfo(10_000, 10_000L * 5_000);

    // Should still use initial estimate (need at least 5 observations)
    assertEquals(provider.getPercentileEstimate(), initialEstimate, 0.01);

    // Add more to reach threshold
    for (int i = 0; i < 5; i++) {
      provider.updateSegmentInfo(10_000, 10_000L * 5_000);
    }

    // Now should use actual percentile calculation
    double newEstimate = provider.getPercentileEstimate();
    assertTrue(newEstimate < initialEstimate,
        "Should switch to percentile calculation after sufficient data");
  }

  @Test
  public void testOverallAverageTracking() {
    long desiredSize = 100 * 1024 * 1024;
    int maxRows = 1_000_000;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows);

    assertEquals(provider.getOverallAverageBytesPerRow(), 0.0, 0.01);

    // Add varied segments
    provider.updateSegmentInfo(10_000, 10_000L * 5_000);  // 5KB/row
    provider.updateSegmentInfo(20_000, 20_000L * 10_000); // 10KB/row
    provider.updateSegmentInfo(15_000, 15_000L * 8_000);  // 8KB/row

    double expectedAvg = (10_000.0 * 5_000 + 20_000.0 * 10_000 + 15_000.0 * 8_000) / (10_000 + 20_000 + 15_000);
    assertEquals(provider.getOverallAverageBytesPerRow(), expectedAvg, 1.0);
  }

  @Test
  public void testStatisticsOutput() {
    long desiredSize = 200 * 1024 * 1024;
    int maxRows = 2_000_000;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, 75);

    provider.updateSegmentInfo(20_000, 20_000L * 8_000);
    provider.updateSegmentInfo(25_000, 25_000L * 8_000);

    String stats = provider.getStatistics();
    assertTrue(stats.contains("segments=2"));
    assertTrue(stats.contains("P75"));
    assertTrue(stats.contains("overallAverage"));
    assertTrue(stats.contains("currentRowCount"));
    assertTrue(stats.contains("reservoirSize"));

    String reservoirStats = provider.getReservoirStatistics();
    assertTrue(reservoirStats.contains("min"));
    assertTrue(reservoirStats.contains("median"));
    assertTrue(reservoirStats.contains("max"));
  }

  @Test
  public void testReservoirStatisticsEmpty() {
    long desiredSize = 100 * 1024 * 1024;
    int maxRows = 1_000_000;

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows);

    String reservoirStats = provider.getReservoirStatistics();
    assertEquals(reservoirStats, "Reservoir empty");
  }

  @Test
  public void testRealWorldMultiTenantScenario() {
    // Simulate multi-tenant table where different customers have vastly different sketch sizes
    long desiredSize = 200 * 1024 * 1024; // 200MB
    int maxRows = 2_000_000;
    int percentile = 75; // P75 = 75% of segments will be under target

    PercentileAdaptiveSegmentNumRowProvider provider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, percentile);

    // Customer A: Small sketches (10 segments at ~500 bytes per row)
    for (int i = 0; i < 10; i++) {
      provider.updateSegmentInfo(100_000, 100_000L * 500);
    }

    // Customer B: Medium sketches (10 segments at ~5KB per row)
    for (int i = 0; i < 10; i++) {
      provider.updateSegmentInfo(40_000, 40_000L * 5_000);
    }

    // Customer C: Large sketches (10 segments at ~50KB per row)
    for (int i = 0; i < 10; i++) {
      provider.updateSegmentInfo(4_000, 4_000L * 50_000);
    }

    // Customer D: Very large sketches (5 segments at ~120KB per row - at capacity)
    for (int i = 0; i < 5; i++) {
      provider.updateSegmentInfo(1_500, 1_500L * 120_000);
    }

    // P75 should be closer to the median/upper-medium range, not heavily influenced by largest
    double p75 = provider.getPercentileEstimate();

    // With this distribution: sorted values are mostly 500, 5000, 50000, 120000
    // 75th percentile should be around 50KB (between medium and large)
    assertTrue(p75 > 5_000 && p75 < 120_000,
        "P75 should be in reasonable middle range, got: " + p75);

    // Suggested row count should be conservative
    int suggestedRows = provider.getNumRows();
    long projectedSize = (long) suggestedRows * (long) p75;
    assertTrue(projectedSize <= desiredSize * 1.5,
        "Projected segment size should be reasonable: " + (projectedSize / (1024 * 1024)) + "MB");

    assertEquals(provider.getSegmentsProcessed(), 35);
  }

  @Test
  public void testComparisonWithAdaptiveProvider() {
    // Demonstrate that percentile-based is more robust than simple EMA for heterogeneous data
    long desiredSize = 100 * 1024 * 1024;
    int maxRows = 1_000_000;

    // Percentile provider (P75)
    PercentileAdaptiveSegmentNumRowProvider percentileProvider =
        new PercentileAdaptiveSegmentNumRowProvider(desiredSize, maxRows, 75);

    // EMA-based provider for comparison
    AdaptiveSegmentNumRowProvider emaProvider =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, 0.3, 5000.0);

    // Simulate highly variable data
    // 20 segments with small sizes (2KB/row)
    for (int i = 0; i < 20; i++) {
      percentileProvider.updateSegmentInfo(40_000, 40_000L * 2_000);
      emaProvider.updateSegmentInfo(40_000, 40_000L * 2_000);
    }

    // Then 5 segments with huge sizes (100KB/row)
    for (int i = 0; i < 5; i++) {
      percentileProvider.updateSegmentInfo(1_000, 1_000L * 100_000);
      emaProvider.updateSegmentInfo(1_000, 1_000L * 100_000);
    }

    // Percentile (P75) should still be close to 2KB since 80% of segments are at 2KB
    double percentileEst = percentileProvider.getPercentileEstimate();
    assertTrue(percentileEst < 10_000,
        "Percentile estimate should be robust to outliers: " + percentileEst);

    // EMA will be more influenced by the large values
    double emaEst = emaProvider.getEstimatedBytesPerRow();

    // Percentile should suggest more rows (smaller per-row estimate)
    assertTrue(percentileProvider.getNumRows() > emaProvider.getNumRows(),
        "Percentile provider should be less affected by outliers");
  }
}
