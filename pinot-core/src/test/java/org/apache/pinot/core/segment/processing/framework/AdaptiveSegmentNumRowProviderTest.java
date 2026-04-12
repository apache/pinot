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
 * Unit tests for AdaptiveSegmentNumRowProvider
 */
public class AdaptiveSegmentNumRowProviderTest {

  @Test
  public void testConstructorWithDefaults() {
    long desiredSize = 200 * 1024 * 1024; // 200MB
    int maxRows = 2_000_000;

    AdaptiveSegmentNumRowProvider provider = new AdaptiveSegmentNumRowProvider(desiredSize, maxRows);

    // With default 5000 bytes/row estimate, should calculate 200MB / 5000 = ~40,000 rows
    int initialRows = provider.getNumRows();
    assertTrue(initialRows > 0 && initialRows <= maxRows,
        "Initial row count should be positive and <= max");
    assertEquals(provider.getSegmentsProcessed(), 0);
    assertEquals(provider.getEstimatedBytesPerRow(), 5000.0, 0.01);
  }

  @Test
  public void testConstructorWithCustomParameters() {
    long desiredSize = 100 * 1024 * 1024; // 100MB
    int maxRows = 1_000_000;
    double learningRate = 0.5;
    double initialEstimate = 10000.0;

    AdaptiveSegmentNumRowProvider provider =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, learningRate, initialEstimate);

    assertEquals(provider.getEstimatedBytesPerRow(), initialEstimate, 0.01);
    // 100MB / 10000 = 10,000 rows
    assertEquals(provider.getNumRows(), 10_485);
  }

  @Test
  public void testInvalidConstructorArguments() {
    // Negative desired size
    assertThrows(IllegalArgumentException.class, () ->
        new AdaptiveSegmentNumRowProvider(-1, 1_000_000));

    // Zero max rows
    assertThrows(IllegalArgumentException.class, () ->
        new AdaptiveSegmentNumRowProvider(100_000_000, 0));

    // Invalid learning rate (too low)
    assertThrows(IllegalArgumentException.class, () ->
        new AdaptiveSegmentNumRowProvider(100_000_000, 1_000_000, 0.0, 5000));

    // Invalid learning rate (too high)
    assertThrows(IllegalArgumentException.class, () ->
        new AdaptiveSegmentNumRowProvider(100_000_000, 1_000_000, 1.5, 5000));

    // Negative initial estimate
    assertThrows(IllegalArgumentException.class, () ->
        new AdaptiveSegmentNumRowProvider(100_000_000, 1_000_000, 0.3, -100));
  }

  @Test
  public void testAdaptationWithConsistentSizes() {
    long desiredSize = 200 * 1024 * 1024; // 200MB
    int maxRows = 5_000_000;
    double learningRate = 0.3;
    double initialEstimate = 5000.0;

    AdaptiveSegmentNumRowProvider provider =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, learningRate, initialEstimate);

    // Simulate segments with consistent 10KB per row
    int actualRows1 = 20_000;
    long actualSize1 = 20_000L * 10_000; // 200MB exactly

    provider.updateSegmentInfo(actualRows1, actualSize1);

    // After first update: estimate should move toward 10000
    // New estimate = 0.3 * 10000 + 0.7 * 5000 = 3000 + 3500 = 6500
    assertEquals(provider.getEstimatedBytesPerRow(), 6500.0, 0.01);
    assertEquals(provider.getSegmentsProcessed(), 1);

    // Next segment row count should be 200MB / 6500 = ~30,769 rows
    int nextRows = provider.getNumRows();
    assertTrue(nextRows > 20_000 && nextRows < 35_000,
        "Row count should increase since we underestimated bytes/row");

    // Simulate another segment with same 10KB per row
    provider.updateSegmentInfo(nextRows, nextRows * 10_000L);

    // Estimate should continue converging toward 10000
    assertTrue(provider.getEstimatedBytesPerRow() > 6500.0,
        "Estimate should increase toward actual 10000");
    assertTrue(provider.getEstimatedBytesPerRow() < 10000.0,
        "Estimate hasn't converged fully yet");
    assertEquals(provider.getSegmentsProcessed(), 2);
  }

  @Test
  public void testAdaptationWithIncreasingSize() {
    long desiredSize = 100 * 1024 * 1024; // 100MB
    int maxRows = 2_000_000;
    double learningRate = 0.5; // Higher learning rate for faster adaptation

    AdaptiveSegmentNumRowProvider provider =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, learningRate, 5000.0);

    // First segment: 2KB per row
    provider.updateSegmentInfo(50_000, 50_000L * 2_000);
    double estimateAfter1 = provider.getEstimatedBytesPerRow();
    // estimate = 0.5 * 2000 + 0.5 * 5000 = 1000 + 2500 = 3500
    assertEquals(estimateAfter1, 3500.0, 0.01);

    // Second segment: 4KB per row
    provider.updateSegmentInfo(30_000, 30_000L * 4_000);
    double estimateAfter2 = provider.getEstimatedBytesPerRow();
    // estimate = 0.5 * 4000 + 0.5 * 3500 = 2000 + 1750 = 3750
    assertEquals(estimateAfter2, 3750.0, 0.01);

    // Third segment: 8KB per row
    provider.updateSegmentInfo(15_000, 15_000L * 8_000);
    double estimateAfter3 = provider.getEstimatedBytesPerRow();
    // estimate = 0.5 * 8000 + 0.5 * 3750 = 4000 + 1875 = 5875
    assertEquals(estimateAfter3, 5875.0, 0.01);

    // Row count should decrease as estimate increases
    assertTrue(provider.getNumRows() < 25_000,
        "Row count should decrease as bytes/row estimate increases");
  }

  @Test
  public void testMaxRowsEnforcement() {
    long desiredSize = 1_000 * 1024 * 1024; // 1GB
    int maxRows = 50_000;
    double initialEstimate = 100.0; // Very small estimate

    AdaptiveSegmentNumRowProvider provider =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, 0.3, initialEstimate);

    // With 100 bytes/row estimate and 1GB desired: would calculate 10M rows
    // But should be capped at maxRows
    assertEquals(provider.getNumRows(), maxRows,
        "Row count should be capped at maxRows");

    // Even after learning about larger sizes, should still respect max
    provider.updateSegmentInfo(maxRows, maxRows * 1000L);
    assertEquals(provider.getNumRows(), maxRows,
        "Row count should still be capped after update");
  }

  @Test
  public void testMinRowsEnforcement() {
    long desiredSize = 1024; // 1KB desired size
    int maxRows = 1_000_000;
    double initialEstimate = 10_000.0; // Large estimate

    AdaptiveSegmentNumRowProvider provider =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, 0.3, initialEstimate);

    // With 10KB bytes/row estimate and 1KB desired: would calculate < 1 row
    // But should be at least 1
    assertTrue(provider.getNumRows() >= 1,
        "Row count should be at least 1");
  }

  @Test
  public void testInvalidUpdateArguments() {
    long desiredSize = 200 * 1024 * 1024;
    int maxRows = 2_000_000;

    AdaptiveSegmentNumRowProvider provider = new AdaptiveSegmentNumRowProvider(desiredSize, maxRows);

    // Should log warning but not crash
    int rowsBefore = provider.getNumRows();
    provider.updateSegmentInfo(0, 100_000); // Zero rows
    assertEquals(provider.getNumRows(), rowsBefore, "Row count should not change on invalid update");
    assertEquals(provider.getSegmentsProcessed(), 0, "Should not count invalid update");

    provider.updateSegmentInfo(1000, 0); // Zero size
    assertEquals(provider.getNumRows(), rowsBefore, "Row count should not change on invalid update");
    assertEquals(provider.getSegmentsProcessed(), 0, "Should not count invalid update");

    provider.updateSegmentInfo(-100, 100_000); // Negative rows
    assertEquals(provider.getNumRows(), rowsBefore, "Row count should not change on invalid update");
    assertEquals(provider.getSegmentsProcessed(), 0, "Should not count invalid update");
  }

  @Test
  public void testConvergenceWithMultipleUpdates() {
    long desiredSize = 200 * 1024 * 1024; // 200MB
    int maxRows = 2_000_000;
    double learningRate = 0.2; // Lower learning rate for stable convergence

    AdaptiveSegmentNumRowProvider provider =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, learningRate, 5000.0);

    // Actual data has 8KB per row
    long actualBytesPerRow = 8000;

    // Simulate 10 segments
    for (int i = 0; i < 10; i++) {
      int numRows = provider.getNumRows();
      long segmentSize = numRows * actualBytesPerRow;
      provider.updateSegmentInfo(numRows, segmentSize);
    }

    // After 10 segments, estimate should be close to actual
    double finalEstimate = provider.getEstimatedBytesPerRow();
    assertTrue(Math.abs(finalEstimate - actualBytesPerRow) < 1000,
        "Estimate should converge close to actual: expected ~8000, got " + finalEstimate);

    // Final row count should be close to optimal
    int finalRowCount = provider.getNumRows();
    int optimalRowCount = (int) (desiredSize / actualBytesPerRow); // 200MB / 8KB = 25,000
    assertTrue(Math.abs(finalRowCount - optimalRowCount) < 3000,
        "Row count should converge to optimal: expected ~25000, got " + finalRowCount);
  }

  @Test
  public void testOverallAverageTracking() {
    long desiredSize = 100 * 1024 * 1024; // 100MB
    int maxRows = 1_000_000;

    AdaptiveSegmentNumRowProvider provider = new AdaptiveSegmentNumRowProvider(desiredSize, maxRows);

    // Initially no data
    assertEquals(provider.getOverallAverageBytesPerRow(), 0.0, 0.01);

    // Add varied segment sizes
    provider.updateSegmentInfo(10_000, 10_000L * 5_000); // 5KB/row
    provider.updateSegmentInfo(20_000, 20_000L * 10_000); // 10KB/row
    provider.updateSegmentInfo(15_000, 15_000L * 8_000); // 8KB/row

    // Overall average = (50MB + 200MB + 120MB) / (10k + 20k + 15k) = 370MB / 45k = ~8222 bytes/row
    double totalBytes = 10_000L * 5_000 + 20_000L * 10_000 + 15_000L * 8_000;
    double totalRows = 10_000 + 20_000 + 15_000;
    double expectedAverage = totalBytes / totalRows;

    assertEquals(provider.getOverallAverageBytesPerRow(), expectedAverage, 1.0);
  }

  @Test
  public void testStatisticsOutput() {
    long desiredSize = 200 * 1024 * 1024;
    int maxRows = 2_000_000;

    AdaptiveSegmentNumRowProvider provider = new AdaptiveSegmentNumRowProvider(desiredSize, maxRows);

    provider.updateSegmentInfo(20_000, 20_000L * 8_000);
    provider.updateSegmentInfo(25_000, 25_000L * 8_000);

    String stats = provider.getStatistics();

    // Verify statistics string contains key information
    assertTrue(stats.contains("segments=2"), "Stats should show 2 segments processed");
    assertTrue(stats.contains("currentEstimate"), "Stats should show current estimate");
    assertTrue(stats.contains("overallAverage"), "Stats should show overall average");
    assertTrue(stats.contains("currentRowCount"), "Stats should show current row count");
  }

  @Test
  public void testLearningRateImpact() {
    long desiredSize = 100 * 1024 * 1024; // 100MB
    int maxRows = 1_000_000;

    // Provider with fast learning (high rate)
    AdaptiveSegmentNumRowProvider fastLearner =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, 0.9, 5000.0);

    // Provider with slow learning (low rate)
    AdaptiveSegmentNumRowProvider slowLearner =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, 0.1, 5000.0);

    // Both see segment with 10KB per row
    fastLearner.updateSegmentInfo(10_000, 10_000L * 10_000);
    slowLearner.updateSegmentInfo(10_000, 10_000L * 10_000);

    // Fast learner should adapt more quickly
    // Fast: 0.9 * 10000 + 0.1 * 5000 = 9500
    // Slow: 0.1 * 10000 + 0.9 * 5000 = 5500
    assertEquals(fastLearner.getEstimatedBytesPerRow(), 9500.0, 0.01);
    assertEquals(slowLearner.getEstimatedBytesPerRow(), 5500.0, 0.01);

    assertTrue(fastLearner.getEstimatedBytesPerRow() > slowLearner.getEstimatedBytesPerRow(),
        "Fast learner should adapt more toward observed value");
  }

  @Test
  public void testRealWorldScenarioWithThetaSketches() {
    // Simulate real-world scenario with variable Theta sketch sizes
    long desiredSize = 200 * 1024 * 1024; // 200MB target
    int maxRows = 2_000_000;
    double learningRate = 0.3;

    AdaptiveSegmentNumRowProvider provider =
        new AdaptiveSegmentNumRowProvider(desiredSize, maxRows, learningRate, 5000.0);

    // Simulate segments with varying sketch sizes
    // Small sketches: ~2KB per row
    provider.updateSegmentInfo(80_000, 80_000L * 2_000);

    // Medium sketches: ~5KB per row
    provider.updateSegmentInfo(50_000, 50_000L * 5_000);

    // Large sketches: ~15KB per row
    provider.updateSegmentInfo(15_000, 15_000L * 15_000);

    // Another large sketch segment
    provider.updateSegmentInfo(14_000, 14_000L * 15_000);

    // Estimate should reflect the observed distribution
    double finalEstimate = provider.getEstimatedBytesPerRow();
    assertTrue(finalEstimate > 5000 && finalEstimate < 15000,
        "Estimate should be between min and max observed values");

    // Provider should now suggest reasonable row counts
    int suggestedRows = provider.getNumRows();
    assertTrue(suggestedRows > 10_000 && suggestedRows < 100_000,
        "Suggested row count should be reasonable for mixed sketch sizes");

    assertEquals(provider.getSegmentsProcessed(), 4);
  }
}
