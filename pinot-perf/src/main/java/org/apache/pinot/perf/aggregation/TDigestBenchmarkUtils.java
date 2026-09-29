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
package org.apache.pinot.perf.aggregation;

import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;

/// Keeps benchmark inputs on Pinot's K1 policy while allowing the same benchmark bytecode to run with t-digest 3.2.
final class TDigestBenchmarkUtils {
  private static final String SCALE_FUNCTION_CLASS = "com.tdunning.math.stats.ScaleFunction";
  private static final int VERBOSE_ENCODING = 1;
  private static final int VERBOSE_HEADER_SIZE = 32;
  private static final int VERBOSE_CENTROID_SIZE = 16;

  private TDigestBenchmarkUtils() {
  }

  static <T extends TDigest> T usePinotScaleFunction(T digest) {
    try {
      Class<?> scaleFunctionClass = Class.forName(SCALE_FUNCTION_CLASS);
      Object k1 = scaleFunctionClass.getField("K_1").get(null);
      digest.getClass().getMethod("setScaleFunction", scaleFunctionClass).invoke(digest, k1);
      return digest;
    } catch (ClassNotFoundException e) {
      // T-digest 3.2 predates configurable scale functions and already uses the K1 weight limit.
      return digest;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot configure the TDigest benchmark scale function", e);
    }
  }

  /// Creates a deterministic verbose TDigest encoding from sorted raw values.
  ///
  /// The endpoint values remain singleton centroids, matching the TDigest boundary invariant. This layout is used as
  /// a byte-for-byte dependency-version control by benchmarks that otherwise consume native 3.2 or 3.3 encodings.
  static byte[] createFixedVerboseBytes(double[] sortedValues, double compression) {
    if (sortedValues.length < 2) {
      throw new IllegalArgumentException("Fixed verbose benchmark input requires at least two values");
    }
    int numCentroids = Math.min(sortedValues.length, Math.max(2, (int) Math.ceil(compression * 1.28)));
    ByteBuffer buffer = ByteBuffer.allocate(VERBOSE_HEADER_SIZE + numCentroids * VERBOSE_CENTROID_SIZE);
    buffer.putInt(VERBOSE_ENCODING);
    buffer.putDouble(sortedValues[0]);
    buffer.putDouble(sortedValues[sortedValues.length - 1]);
    buffer.putDouble(compression);
    buffer.putInt(numCentroids);
    for (int centroidId = 0; centroidId < numCentroids; centroidId++) {
      int start;
      int end;
      if (centroidId == 0) {
        start = 0;
        end = 1;
      } else if (centroidId == numCentroids - 1) {
        start = sortedValues.length - 1;
        end = sortedValues.length;
      } else {
        int numInteriorCentroids = numCentroids - 2;
        int numInteriorValues = sortedValues.length - 2;
        start = 1 + (centroidId - 1) * numInteriorValues / numInteriorCentroids;
        end = 1 + centroidId * numInteriorValues / numInteriorCentroids;
      }
      double sum = 0.0;
      for (int valueId = start; valueId < end; valueId++) {
        sum += sortedValues[valueId];
      }
      double weight = end - start;
      buffer.putDouble(weight);
      buffer.putDouble(sum / weight);
    }
    return buffer.array();
  }
}
