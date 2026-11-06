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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.nativeengine.agg.PinotNativeAgg;


/**
 * Routes eligible aggregation function constructions to the native (Rust+JNI) acceleration
 * path. This is the single integration point between Pinot's aggregation engine and
 * {@code pinot-native}.
 *
 * <p>Eligibility checks (short-circuiting):
 * <ol>
 *   <li>Feature flag {@code pinot.native.aggregation.enabled} is set to {@code true}</li>
 *   <li>The native library loaded successfully ({@link PinotNativeAgg#isAvailable()})</li>
 *   <li>Null handling is disabled (no native null path yet)</li>
 *   <li>Function type is in the Phase 1 POC scope: {@code SUM} / {@code SUM0}</li>
 *   <li>The aggregated expression is a simple column identifier (no transforms)</li>
 * </ol>
 *
 * <p>When any check fails the caller must construct the original Java AggregationFunction
 * unchanged. The router never throws.
 *
 * <p>Routing here covers all aggregation contexts in Pinot — SSE V1, MSE leaf (which
 * delegates to V1), MSE intermediate, star-tree, realtime consuming segments, and
 * Materialized View refresh — because every one of them obtains AggregationFunction
 * instances from {@link AggregationFunctionFactory}. See {@code docs/native/phase-1-design.md}
 * §2 for the engine landscape.
 */
public final class NativeAggregationRouter {

  /** System property gating the native engine. Default {@code false}. */
  public static final String ENABLED_PROPERTY = "pinot.native.aggregation.enabled";

  private NativeAggregationRouter() {
  }

  /**
   * @return {@code true} if a native AggregationFunction should be constructed for the
   *         given function name + arguments. Caller must use {@link #createNative} to do so.
   */
  public static boolean shouldAccelerate(
      String upperCaseFunctionName, List<ExpressionContext> arguments,
      boolean nullHandlingEnabled) {
    if (!enabled()) {
      return false;
    }
    if (!PinotNativeAgg.isAvailable()) {
      return false;
    }
    if (nullHandlingEnabled) {
      return false;
    }
    if (!isInScopeFunction(upperCaseFunctionName)) {
      return false;
    }
    return isSimpleColumnArg(arguments);
  }

  /**
   * Constructs a native AggregationFunction for a name + arguments combination previously
   * accepted by {@link #shouldAccelerate}. Caller is responsible for the eligibility check.
   *
   * @throws IllegalStateException if the function name is not in scope (programming error)
   */
  @SuppressWarnings("rawtypes")
  public static AggregationFunction createNative(
      String upperCaseFunctionName, List<ExpressionContext> arguments,
      boolean nullHandlingEnabled) {
    switch (upperCaseFunctionName) {
      case "SUM":
      case "SUM0":
        return new NativeSumAggregationFunction(arguments, nullHandlingEnabled);
      default:
        throw new IllegalStateException(
            "Native AggregationFunction requested for unsupported function: "
                + upperCaseFunctionName);
    }
  }

  static boolean enabled() {
    return Boolean.getBoolean(ENABLED_PROPERTY);
  }

  private static boolean isInScopeFunction(String upperCaseFunctionName) {
    return "SUM".equals(upperCaseFunctionName) || "SUM0".equals(upperCaseFunctionName);
  }

  private static boolean isSimpleColumnArg(List<ExpressionContext> arguments) {
    if (arguments.size() != 1) {
      return false;
    }
    return arguments.get(0).getType() == ExpressionContext.Type.IDENTIFIER;
  }
}
