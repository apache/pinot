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

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/**
 * Pure t-digest reproducer for the merge-order sensitivity observed while upgrading Pinot from
 * t-digest 3.2 to 3.3 in PR 18103.
 *
 * <p>The sample data intentionally mirrors Pinot's pre-aggregated percentileTDigest star-tree
 * path: many tiny leaf digests (two points each), most values near zero, sparse large spikes near
 * the upper tail, and repeated serialize/deserialize round-trips between hierarchical merge levels.
 *
 * <p>This test intentionally exercises the 3.3 behavior only. See {@link TDigestVersionComparisonTest}
 * for the direct 3.2 vs 3.3 exact-quantile comparison on a fixed dataset.
 *
 * <p>On t-digest 3.3 this deterministic generator produces a large divergence between sequential
 * merging and hierarchical merging at low compression, while compression 500 restores stable
 * results. The generator depends only on t-digest APIs so it can be copied directly into upstream
 * t-digest tests for further investigation.
 */
public class TDigestMergeOrderReproducerTest {
  private static final int SCALE = 10_000;
  private static final int NUM_DIGESTS = 1_024;
  private static final int VALUES_PER_DIGEST = 2;
  private static final int BATCH_SIZE = 16;
  private static final int DATA_SEED = 5;

  @Test
  public void testTailSpikesScenarioRequiresHighCompressionForStableHierarchicalMerges() {
    double divergenceAt100 = maxMergeOrderDivergence(100);
    double divergenceAt150 = maxMergeOrderDivergence(150);
    double divergenceAt200 = maxMergeOrderDivergence(200);
    double divergenceAt500 = maxMergeOrderDivergence(500);

    assertTrue(divergenceAt100 > 0.02,
        String.format("Expected large merge-order divergence at compression 100 but saw %.6f", divergenceAt100));
    assertTrue(divergenceAt150 > 0.02,
        String.format("Expected large merge-order divergence at compression 150 but saw %.6f", divergenceAt150));
    assertTrue(divergenceAt200 > 0.02,
        String.format("Expected large merge-order divergence at compression 200 but saw %.6f", divergenceAt200));
    assertTrue(divergenceAt500 < 0.001,
        String.format("Expected stable merge-order behavior at compression 500 but saw %.6f", divergenceAt500));
  }

  private double maxMergeOrderDivergence(int compression) {
    List<TDigest> leafDigests = createLeafDigests(compression);
    TDigest sequential = roundTrip(mergeSequential(leafDigests));
    TDigest hierarchical = roundTrip(mergeHierarchical(leafDigests, BATCH_SIZE));

    double maxNormalizedDivergence = 0d;
    for (int percentile = 0; percentile <= 100; percentile++) {
      double quantile = percentile / 100d;
      double delta = Math.abs(sequential.quantile(quantile) - hierarchical.quantile(quantile)) / SCALE;
      maxNormalizedDivergence = Math.max(maxNormalizedDivergence, delta);
    }
    return maxNormalizedDivergence;
  }

  private List<TDigest> createLeafDigests(int compression) {
    Random random = new Random(DATA_SEED);
    List<TDigest> digests = new ArrayList<>(NUM_DIGESTS);
    for (int i = 0; i < NUM_DIGESTS; i++) {
      TDigest digest = TDigest.createMergingDigest(compression);
      for (int j = 0; j < VALUES_PER_DIGEST; j++) {
        digest.add(nextTailSpikeValue(random));
      }
      digests.add(roundTrip(digest));
    }
    return digests;
  }

  private TDigest mergeSequential(List<TDigest> digests) {
    TDigest accumulator = roundTrip(digests.get(0));
    for (int i = 1; i < digests.size(); i++) {
      accumulator.add(digests.get(i));
    }
    return accumulator;
  }

  private TDigest mergeHierarchical(List<TDigest> digests, int batchSize) {
    List<TDigest> currentLevel = digests;
    while (currentLevel.size() > 1) {
      List<TDigest> nextLevel = new ArrayList<>((currentLevel.size() + batchSize - 1) / batchSize);
      for (int start = 0; start < currentLevel.size(); start += batchSize) {
        int end = Math.min(start + batchSize, currentLevel.size());
        TDigest accumulator = roundTrip(currentLevel.get(start));
        for (int i = start + 1; i < end; i++) {
          accumulator.add(currentLevel.get(i));
        }
        nextLevel.add(roundTrip(accumulator));
      }
      currentLevel = nextLevel;
    }
    return currentLevel.get(0);
  }

  private double nextTailSpikeValue(Random random) {
    double roll = random.nextDouble();
    if (roll < 0.97d) {
      return random.nextDouble() * 100d;
    }
    if (roll < 0.995d) {
      return 9_900d + random.nextDouble() * 50d;
    }
    return random.nextDouble() * SCALE;
  }

  private TDigest roundTrip(TDigest digest) {
    ByteBuffer buffer = ByteBuffer.allocate(digest.smallByteSize());
    digest.asSmallBytes(buffer);
    buffer.flip();
    return MergingDigest.fromBytes(buffer);
  }
}
