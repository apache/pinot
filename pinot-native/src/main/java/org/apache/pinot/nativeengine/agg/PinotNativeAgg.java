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
package org.apache.pinot.nativeengine.agg;

/**
 * Java entry points to Pinot's native aggregation kernels.
 *
 * <p>Methods are static native; symbols resolve into the {@code libpinot_native} shared library
 * loaded by {@link NativeLibLoader} at class initialization. If the native library cannot be
 * loaded (unsupported platform, missing binary, etc.), {@link #isAvailable()} returns
 * {@code false} and callers must fall back to the Java implementation. Calling a native method
 * when {@code isAvailable()} is false will throw {@link UnsatisfiedLinkError}.
 *
 * <p>Thread safety: all kernels are stateless and safe to call concurrently from any thread.
 */
public final class PinotNativeAgg {

  private static final boolean AVAILABLE;

  static {
    AVAILABLE = NativeLibLoader.tryLoad();
  }

  private PinotNativeAgg() {
  }

  /**
   * @return {@code true} if the native library was loaded successfully and kernels are callable.
   */
  public static boolean isAvailable() {
    return AVAILABLE;
  }

  /**
   * Self-test entry point. Returns a known magic number (0x5049_4E4F == 'PINO') when the
   * native library is correctly loaded and JNI symbol resolution succeeds.
   */
  public static native int probe();

  /**
   * Computes {@code SUM} over a {@code long[]} as a {@code double}, matching
   * {@code SumAggregationFunction.aggregateSV(LONG)} semantics: per-value {@code long -> double}
   * conversion with straight {@code +=} accumulation.
   *
   * @param values input array (must be non-null)
   * @param length number of leading elements of {@code values} to aggregate
   * @return the sum, or {@link Double#NaN} if the native call failed unexpectedly
   */
  public static native double sumLong(long[] values, int length);
}
