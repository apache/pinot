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
package org.apache.pinot.segment.local.aggregator;

import com.tdunning.math.stats.TDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests that the {@link PercentileTDigestValueAggregator} produces accurate quantile estimates
 * under production-like conditions using the default compression (100).
 *
 * <p>These tests simulate single-path production behavior — where raw values are ingested into
 * a TDigest, serialized/deserialized through segment and star-tree building, and then queried.
 * The results are compared against exact quantiles computed from the raw data.
 *
 * <p>This is different from the star-tree comparison tests which compare two merge paths against
 * each other. Here we verify absolute accuracy against ground truth.
 */
public class PercentileTDigestValueAggregatorTest {
  private static final int MAX_VALUE = 10000;
  // Single-path accuracy: raw values merged into one TDigest, compared against exact quantiles.
  // With default compression=100 and uniform data, error is well below 0.5%.
  private static final double SINGLE_PATH_MAX_ERROR = 0.005; // 0.5%
  // Multi-level merge accuracy: TDigests merged in batches with intermediate serialization,
  // simulating star-tree building. The extra ser/deser cycles increase merge-order sensitivity
  // in t-digest 3.3, leading to slightly higher error vs ground truth.
  private static final double MULTI_LEVEL_MAX_ERROR = 0.01; // 1.0%

  /**
   * Simulates production ingestion: raw double values are added one at a time via applyRawValue,
   * then quantile estimates are compared against exact values.
   */
  @Test
  public void testSinglePathAccuracyWithRawValues() {
    PercentileTDigestValueAggregator aggregator =
        new PercentileTDigestValueAggregator(Collections.emptyList());

    int numValues = 100_000;
    double[] rawValues = new double[numValues];
    Random random = new Random(42);

    // Ingest first value
    rawValues[0] = random.nextInt(MAX_VALUE);
    TDigest digest = aggregator.getInitialAggregatedValue(rawValues[0]);

    // Ingest remaining values
    for (int i = 1; i < numValues; i++) {
      rawValues[i] = random.nextInt(MAX_VALUE);
      digest = aggregator.applyRawValue(digest, rawValues[i]);
    }

    // Serialize/deserialize to simulate segment storage
    byte[] serialized = aggregator.serializeAggregatedValue(digest);
    TDigest result = aggregator.deserializeAggregatedValue(serialized);

    // Compute exact quantiles and compare
    Arrays.sort(rawValues);
    assertQuantilesWithinTolerance(result, rawValues, SINGLE_PATH_MAX_ERROR);
  }

  /**
   * Simulates production with pre-aggregated TDigest bytes (e.g., from a previous segment).
   * Multiple TDigests are merged via applyRawValue(byte[]), serialized, and verified.
   */
  @Test
  public void testSinglePathAccuracyWithPreAggregatedBytes() {
    PercentileTDigestValueAggregator aggregator =
        new PercentileTDigestValueAggregator(Collections.emptyList());

    int numDigests = 1000;
    int valuesPerDigest = 100;
    double[] allValues = new double[numDigests * valuesPerDigest];
    Random random = new Random(42);

    // Create first TDigest and serialize
    TDigest first = TDigest.createMergingDigest(PercentileTDigestValueAggregator.DEFAULT_TDIGEST_COMPRESSION);
    for (int j = 0; j < valuesPerDigest; j++) {
      double val = random.nextInt(MAX_VALUE);
      first.add(val);
      allValues[j] = val;
    }
    byte[] firstBytes = aggregator.serializeAggregatedValue(first);

    // Initialize aggregated value from bytes
    TDigest merged = aggregator.getInitialAggregatedValue(firstBytes);

    // Merge remaining TDigests via applyRawValue(byte[])
    for (int i = 1; i < numDigests; i++) {
      TDigest td = TDigest.createMergingDigest(PercentileTDigestValueAggregator.DEFAULT_TDIGEST_COMPRESSION);
      for (int j = 0; j < valuesPerDigest; j++) {
        double val = random.nextInt(MAX_VALUE);
        td.add(val);
        allValues[i * valuesPerDigest + j] = val;
      }
      byte[] bytes = aggregator.serializeAggregatedValue(td);
      merged = aggregator.applyRawValue(merged, bytes);
    }

    // Serialize/deserialize round-trip
    byte[] serialized = aggregator.serializeAggregatedValue(merged);
    TDigest result = aggregator.deserializeAggregatedValue(serialized);

    Arrays.sort(allValues);
    assertQuantilesWithinTolerance(result, allValues, SINGLE_PATH_MAX_ERROR);
  }

  /**
   * Simulates star-tree multi-level merge: TDigests are merged via applyAggregatedValue,
   * with intermediate serialization/deserialization between levels.
   */
  @Test
  public void testMultiLevelMergeAccuracy() {
    PercentileTDigestValueAggregator aggregator =
        new PercentileTDigestValueAggregator(Collections.emptyList());

    int numDigests = 1000;
    int valuesPerDigest = 100;
    int batchSize = 50; // Simulates leaf node size in star-tree
    double[] allValues = new double[numDigests * valuesPerDigest];
    Random random = new Random(42);

    // Create all TDigests
    TDigest[] digests = new TDigest[numDigests];
    for (int i = 0; i < numDigests; i++) {
      digests[i] = TDigest.createMergingDigest(PercentileTDigestValueAggregator.DEFAULT_TDIGEST_COMPRESSION);
      for (int j = 0; j < valuesPerDigest; j++) {
        double val = random.nextInt(MAX_VALUE);
        digests[i].add(val);
        allValues[i * valuesPerDigest + j] = val;
      }
    }

    // Level 1: merge into batches with serialize/deserialize between levels
    int numBatches = (numDigests + batchSize - 1) / batchSize;
    TDigest[] batchResults = new TDigest[numBatches];
    for (int b = 0; b < numBatches; b++) {
      int start = b * batchSize;
      int end = Math.min(start + batchSize, numDigests);
      TDigest batchAcc = aggregator.cloneAggregatedValue(digests[start]);
      for (int i = start + 1; i < end; i++) {
        batchAcc = aggregator.applyAggregatedValue(batchAcc, digests[i]);
      }
      // Serialize/deserialize between levels (as star-tree builder does)
      batchResults[b] = aggregator.deserializeAggregatedValue(aggregator.serializeAggregatedValue(batchAcc));
    }

    // Level 2: merge batch results
    TDigest finalResult = aggregator.cloneAggregatedValue(batchResults[0]);
    for (int i = 1; i < numBatches; i++) {
      finalResult = aggregator.applyAggregatedValue(finalResult, batchResults[i]);
    }

    // Final serialize/deserialize (as stored in star-tree forward index)
    byte[] serialized = aggregator.serializeAggregatedValue(finalResult);
    TDigest result = aggregator.deserializeAggregatedValue(serialized);

    Arrays.sort(allValues);
    assertQuantilesWithinTolerance(result, allValues, MULTI_LEVEL_MAX_ERROR);
  }

  /**
   * Tests accuracy across multiple random seeds to ensure stability.
   */
  @Test
  public void testAccuracyStability() {
    for (int seed = 0; seed < 10; seed++) {
      PercentileTDigestValueAggregator aggregator =
          new PercentileTDigestValueAggregator(Collections.emptyList());

      int numValues = 50_000;
      double[] rawValues = new double[numValues];
      Random random = new Random(seed);

      rawValues[0] = random.nextInt(MAX_VALUE);
      TDigest digest = aggregator.getInitialAggregatedValue(rawValues[0]);

      for (int i = 1; i < numValues; i++) {
        rawValues[i] = random.nextInt(MAX_VALUE);
        digest = aggregator.applyRawValue(digest, rawValues[i]);
      }

      byte[] serialized = aggregator.serializeAggregatedValue(digest);
      TDigest result = aggregator.deserializeAggregatedValue(serialized);

      Arrays.sort(rawValues);
      assertQuantilesWithinTolerance(result, rawValues, SINGLE_PATH_MAX_ERROR);
    }
  }

  /**
   * Tests that serialization round-trip preserves TDigest accuracy.
   */
  @Test
  public void testSerializationRoundTripPreservesAccuracy() {
    PercentileTDigestValueAggregator aggregator =
        new PercentileTDigestValueAggregator(Collections.emptyList());

    int numValues = 10_000;
    double[] rawValues = new double[numValues];
    Random random = new Random(42);

    rawValues[0] = random.nextInt(MAX_VALUE);
    TDigest digest = aggregator.getInitialAggregatedValue(rawValues[0]);
    for (int i = 1; i < numValues; i++) {
      rawValues[i] = random.nextInt(MAX_VALUE);
      digest = aggregator.applyRawValue(digest, rawValues[i]);
    }

    // Multiple round-trips should not degrade accuracy
    TDigest current = digest;
    for (int round = 0; round < 5; round++) {
      byte[] bytes = aggregator.serializeAggregatedValue(current);
      current = aggregator.deserializeAggregatedValue(bytes);
    }

    Arrays.sort(rawValues);
    assertQuantilesWithinTolerance(current, rawValues, SINGLE_PATH_MAX_ERROR);
  }

  /**
   * Asserts that TDigest quantile estimates are within the given error tolerance of exact values.
   *
   * @param digest the TDigest to verify
   * @param sortedValues the raw data sorted in ascending order (ground truth)
   * @param maxError maximum allowed error as a fraction of MAX_VALUE (e.g., 0.005 = 0.5%)
   */
  private void assertQuantilesWithinTolerance(TDigest digest, double[] sortedValues, double maxError) {
    int n = sortedValues.length;
    double delta = MAX_VALUE * maxError;

    for (int q = 0; q <= 100; q++) {
      double p = q / 100.0;
      double estimated = digest.quantile(p);

      // Compute exact quantile using linear interpolation
      double exactIndex = p * (n - 1);
      int lower = (int) Math.floor(exactIndex);
      int upper = Math.min(lower + 1, n - 1);
      double fraction = exactIndex - lower;
      double exact = sortedValues[lower] * (1 - fraction) + sortedValues[upper] * fraction;

      assertEquals(estimated, exact, delta,
          String.format("Quantile %d: estimated=%.2f, exact=%.2f, diff=%.2f, tolerance=%.2f",
              q, estimated, exact, Math.abs(estimated - exact), delta));
    }

    // Also verify the max absolute error across all percentiles
    double maxAbsError = 0;
    for (int q = 0; q <= 100; q++) {
      double p = q / 100.0;
      double exactIndex = p * (n - 1);
      int lower = (int) Math.floor(exactIndex);
      int upper = Math.min(lower + 1, n - 1);
      double fraction = exactIndex - lower;
      double exact = sortedValues[lower] * (1 - fraction) + sortedValues[upper] * fraction;
      maxAbsError = Math.max(maxAbsError, Math.abs(digest.quantile(p) - exact));
    }
    double maxErrorPct = maxAbsError / MAX_VALUE;
    assertTrue(maxErrorPct < maxError,
        String.format("Max error %.4f%% exceeds tolerance %.4f%%", maxErrorPct * 100, maxError * 100));
  }
}
