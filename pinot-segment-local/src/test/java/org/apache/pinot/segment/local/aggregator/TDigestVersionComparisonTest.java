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
import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/**
 * Side-by-side t-digest 3.2 vs 3.3 comparison for the deterministic tail-spike dataset used to
 * investigate the percentileTDigest regression in PR 18103.
 *
 * <p>The test keeps the Pinot-like hierarchical merge pattern fixed and evaluates the final digest
 * against exact quantiles. Under compression 100, t-digest 3.2 stays accurate while 3.3 collapses
 * to materially fewer centroids and exhibits a much larger tail error on the same input. Raising
 * 3.3 to compression 150 recovers the exact-quantile accuracy on this dataset.
 */
public class TDigestVersionComparisonTest {
  private static final double SCALE = 10_000d;
  private static final double[] QUANTILES = {0.5d, 0.9d, 0.95d, 0.99d, 0.995d, 0.999d};
  private static final int NUM_DIGESTS = 5_000;
  private static final int VALUES_PER_DIGEST = 2;
  private static final int BATCH_SIZE = 32;
  private static final int DATA_SEED = 32;
  private static final Path TDIGEST_32_JAR = Path.of("target", "tdigest-compat", "t-digest-3.2.jar");

  @Test
  public void testTailSpikesScenarioShowsDirectAccuracyDifferenceBetween32And33() throws Exception {
    Scenario compression100 = new Scenario(100);
    Scenario compression150 = new Scenario(150);

    try (VersionedTDigestApi td32 = new VersionedTDigestApi(TDIGEST_32_JAR.toFile(), "3.2");
        VersionedTDigestApi td33 = new VersionedTDigestApi(currentTDigestJar(), "3.3")) {
      Result result32 = td32.runScenario(compression100);
      Result result33At100 = td33.runScenario(compression100);
      Result result33At150 = td33.runScenario(compression150);

      assertTrue(result32._maxNormalizedError < 0.0002d,
          String.format("Expected t-digest 3.2 to stay accurate at compression 100 but saw %s", result32));
      assertTrue(result33At100._maxNormalizedError > 0.004d,
          String.format("Expected t-digest 3.3 to show a large accuracy gap at compression 100 but saw %s",
              result33At100));
      assertTrue(result33At100._maxNormalizedError > result32._maxNormalizedError * 20d,
          String.format("Expected t-digest 3.3 to be materially worse than 3.2 but saw 3.2=%s and 3.3=%s",
              result32, result33At100));
      assertTrue(result33At150._maxNormalizedError < 0.0002d,
          String.format("Expected t-digest 3.3 compression 150 to recover exact-quantile accuracy but saw %s",
              result33At150));
      assertTrue(result33At100._centroidCount < result32._centroidCount,
          String.format("Expected t-digest 3.3 to collapse to fewer centroids than 3.2 but saw 3.2=%s and 3.3=%s",
              result32, result33At100));
    }
  }

  private File currentTDigestJar() throws Exception {
    return new File(TDigest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
  }

  private static double exactQuantile(double[] sortedValues, double quantile) {
    if (sortedValues.length == 1) {
      return sortedValues[0];
    }
    double exactIndex = quantile * (sortedValues.length - 1);
    int lowerIndex = (int) Math.floor(exactIndex);
    int upperIndex = Math.min(lowerIndex + 1, sortedValues.length - 1);
    double fraction = exactIndex - lowerIndex;
    return sortedValues[lowerIndex] * (1d - fraction) + sortedValues[upperIndex] * fraction;
  }

  private static double nextTailSpikeValue(Random random) {
    double roll = random.nextDouble();
    if (roll < 0.97d) {
      return random.nextDouble() * 100d;
    }
    if (roll < 0.995d) {
      return 9_900d + random.nextDouble() * 50d;
    }
    return random.nextDouble() * SCALE;
  }

  private static final class Scenario {
    private final int _compression;

    private Scenario(int compression) {
      _compression = compression;
    }

    private double[][] generateValues() {
      Random random = new Random(DATA_SEED);
      double[][] values = new double[NUM_DIGESTS][VALUES_PER_DIGEST];
      for (int i = 0; i < NUM_DIGESTS; i++) {
        for (int j = 0; j < VALUES_PER_DIGEST; j++) {
          values[i][j] = nextTailSpikeValue(random);
        }
      }
      return values;
    }
  }

  private static final class Result {
    private final String _version;
    private final int _compression;
    private final int _centroidCount;
    private final double _maxNormalizedError;

    private Result(String version, int compression, int centroidCount, double maxNormalizedError) {
      _version = version;
      _compression = compression;
      _centroidCount = centroidCount;
      _maxNormalizedError = maxNormalizedError;
    }

    @Override
    public String toString() {
      return String.format("version=%s compression=%d centroids=%d maxNormalizedError=%.6f", _version, _compression,
          _centroidCount, _maxNormalizedError);
    }
  }

  private static final class VersionedTDigestApi implements AutoCloseable {
    private final URLClassLoader _loader;
    private final String _version;
    private final Method _createMergingDigest;
    private final Method _addDouble;
    private final Method _addDigest;
    private final Method _quantile;
    private final Method _smallByteSize;
    private final Method _asSmallBytes;
    private final Method _fromBytes;
    private final Method _centroidCount;

    private VersionedTDigestApi(File jarFile, String version) throws Exception {
      assertTrue(jarFile.isFile(), "Missing t-digest jar for comparison test: " + jarFile.getAbsolutePath());

      _loader = new URLClassLoader(new URL[]{jarFile.toURI().toURL()}, null);
      _version = version;

      Class<?> tDigestClass = Class.forName("com.tdunning.math.stats.TDigest", true, _loader);
      Class<?> mergingDigestClass = Class.forName("com.tdunning.math.stats.MergingDigest", true, _loader);

      _createMergingDigest = tDigestClass.getMethod("createMergingDigest", double.class);
      _addDouble = tDigestClass.getMethod("add", double.class);
      _addDigest = tDigestClass.getMethod("add", tDigestClass);
      _quantile = tDigestClass.getMethod("quantile", double.class);
      _smallByteSize = tDigestClass.getMethod("smallByteSize");
      _asSmallBytes = tDigestClass.getMethod("asSmallBytes", ByteBuffer.class);
      _fromBytes = mergingDigestClass.getMethod("fromBytes", ByteBuffer.class);
      _centroidCount = tDigestClass.getMethod("centroidCount");
    }

    private Result runScenario(Scenario scenario) throws Exception {
      double[][] chunks = scenario.generateValues();
      List<Double> allValues = new ArrayList<>(NUM_DIGESTS * VALUES_PER_DIGEST);
      Object[] digests = createLeafDigests(chunks, scenario, allValues);
      Object finalDigest = buildHierarchicalDigest(digests);
      double[] sorted = allValues.stream().mapToDouble(Double::doubleValue).sorted().toArray();

      double maxError = 0d;
      for (double quantile : QUANTILES) {
        double actual = exactQuantile(sorted, quantile);
        double estimated = (double) _quantile.invoke(finalDigest, quantile);
        maxError = Math.max(maxError, Math.abs(estimated - actual) / SCALE);
      }
      return new Result(_version, scenario._compression, (int) _centroidCount.invoke(finalDigest), maxError);
    }

    private Object[] createLeafDigests(double[][] chunks, Scenario scenario, List<Double> allValues) throws Exception {
      Object[] digests = new Object[chunks.length];
      for (int i = 0; i < chunks.length; i++) {
        Object digest = _createMergingDigest.invoke(null, (double) scenario._compression);
        for (double value : chunks[i]) {
          _addDouble.invoke(digest, value);
          allValues.add(value);
        }
        digests[i] = roundTrip(digest);
      }
      return digests;
    }

    private Object buildHierarchicalDigest(Object[] digests) throws Exception {
      List<Object> currentLevel = Arrays.asList(digests);
      while (currentLevel.size() > 1) {
        List<Object> nextLevel = new ArrayList<>((currentLevel.size() + BATCH_SIZE - 1) / BATCH_SIZE);
        for (int start = 0; start < currentLevel.size(); start += BATCH_SIZE) {
          int end = Math.min(start + BATCH_SIZE, currentLevel.size());
          Object accumulator = roundTrip(currentLevel.get(start));
          for (int i = start + 1; i < end; i++) {
            _addDigest.invoke(accumulator, currentLevel.get(i));
          }
          nextLevel.add(roundTrip(accumulator));
        }
        currentLevel = nextLevel;
      }
      return currentLevel.get(0);
    }

    private Object roundTrip(Object digest) throws Exception {
      int size = (int) _smallByteSize.invoke(digest);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      _asSmallBytes.invoke(digest, buffer);
      buffer.flip();
      return _fromBytes.invoke(null, buffer);
    }

    @Override
    public void close() throws Exception {
      _loader.close();
    }
  }
}
