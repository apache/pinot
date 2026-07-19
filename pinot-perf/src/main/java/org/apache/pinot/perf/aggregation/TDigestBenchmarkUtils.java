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

/// Keeps benchmark inputs on Pinot's K1 policy while allowing the same benchmark bytecode to run with t-digest 3.2.
final class TDigestBenchmarkUtils {
  private static final String SCALE_FUNCTION_CLASS = "com.tdunning.math.stats.ScaleFunction";

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
}
