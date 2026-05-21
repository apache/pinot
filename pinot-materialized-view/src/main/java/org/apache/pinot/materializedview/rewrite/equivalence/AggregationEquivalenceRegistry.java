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
package org.apache.pinot.materializedview.rewrite.equivalence;

import java.util.List;
import javax.annotation.Nullable;


/// Centralized registry of [AggregationEquivalence] rules for
/// re-aggregating pre-computed MV columns when the MV has finer granularity
/// than the user query.
///
/// All rules are statically initialized at class-load time. No external
/// registration or lifecycle management is required.
///
/// To add support for a new aggregation function equivalence, simply add
/// the corresponding rule instance to the [#RULES] list.
///
/// This class is stateless and thread-safe.
public final class AggregationEquivalenceRegistry {

  private static final List<AggregationEquivalence> RULES = List.of(
      // Distributive: re-aggregate with the same function
      new PassthroughEquivalence("SUM", "SUM"),
      new PassthroughEquivalence("MIN", "MIN"),
      new PassthroughEquivalence("MAX", "MAX"),

      // Algebraic (simple transformation): COUNT -> SUM
      new PassthroughEquivalence("COUNT", "SUM"),

      // Sketch-based: user wants cardinality/result, MV stores raw sketch
      new SketchMergeEquivalence("DISTINCTCOUNTHLL", "DISTINCTCOUNTRAWHLL", "DISTINCTCOUNTHLL"),
      new SketchMergeEquivalence("DISTINCTCOUNTHLLPLUS", "DISTINCTCOUNTRAWHLLPLUS", "DISTINCTCOUNTHLLPLUS"),
      new SketchMergeEquivalence("DISTINCTCOUNTTHETASKETCH", "DISTINCTCOUNTRAWTHETASKETCH",
          "DISTINCTCOUNTTHETASKETCH")
  );

  private AggregationEquivalenceRegistry() {
  }

  /// Finds the first equivalence rule that can handle the given user-function
  /// and MV-function pair.
  ///
  /// @param userFunctionName the aggregation function name from the user query
  ///                         (e.g. "SUM", "DISTINCTCOUNTHLL")
  /// @param materializedViewFunctionName   the aggregation function name from the MV
  ///                         definition (e.g. "SUM", "DISTINCTCOUNTRAWHLL")
  /// @return the matching rule, or `null` if no rule supports the
  ///         given function pair (indicating the function cannot be
  ///         re-aggregated)
  @Nullable
  public static AggregationEquivalence findRule(String userFunctionName, String materializedViewFunctionName) {
    for (AggregationEquivalence rule : RULES) {
      if (rule.matches(userFunctionName, materializedViewFunctionName)) {
        return rule;
      }
    }
    return null;
  }

  /// Returns true if any registered equivalence rule can use an MV column produced by
  /// `materializedViewFunctionName` as input.  Used by the analyzer to reject MV definitions
  /// whose aggregations have no re-aggregation rule, so the operator gets a clear
  /// create-time error instead of silently observing zero MV rewrites.
  public static boolean isMaterializedViewFunctionSupported(String materializedViewFunctionName) {
    for (AggregationEquivalence rule : RULES) {
      if (rule.supportsMaterializedViewFunction(materializedViewFunctionName)) {
        return true;
      }
    }
    return false;
  }

  /// Returns the canonical user-facing aggregation function names that can be served from
  /// some registered MV function.  Used by the analyzer to surface a helpful list when an
  /// unsupported aggregation is rejected.
  public static List<String> supportedMaterializedViewFunctions() {
    return List.of("SUM", "MIN", "MAX", "COUNT",
        "DISTINCTCOUNTRAWHLL", "DISTINCTCOUNTRAWHLLPLUS", "DISTINCTCOUNTRAWTHETASKETCH");
  }
}
