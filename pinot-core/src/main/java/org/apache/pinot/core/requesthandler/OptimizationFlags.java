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
package org.apache.pinot.core.requesthandler;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.BrokerRequest;


/**
 * Holder for optimization flags.
 */
public class OptimizationFlags {
  private final Set<String> _enabledOptimizations;
  private final Set<String> _disabledOptimizations;

  private OptimizationFlags(Set<String> enabledOptimizations, Set<String> disabledOptimizations) {
    Preconditions.checkArgument(enabledOptimizations.isEmpty() || disabledOptimizations.isEmpty(),
        "Cannot exclude and include optimizations at the same time");
    _enabledOptimizations = enabledOptimizations;
    _disabledOptimizations = disabledOptimizations;
  }

  public boolean isOptimizationEnabled(String optimizationName) {
    if (_enabledOptimizations.isEmpty()) {
      return !_disabledOptimizations.contains(optimizationName);
    } else {
      return _enabledOptimizations.contains(optimizationName);
    }
  }

  /**
   * Returns the optimization flags contained in the debug options of a broker request, or null if there are no
   * optimization flags to apply.
   *
   * Optimization flags are specified as a broker request debug option called "optimizationFlags" which contains a list
   * of optimizations to enable or skip. Optimizations that are prefixed with a plus sign are marked as enabled, while
   * ones that are prefixed with a minus sign are disabled. If at least one optimization is marked as enabled, this is
   * equivalent to disabling all non-explicitly enabled optimizations.
   *
   * For example, "+filterQueryTree,+flattenNestedPredicates" enables only the "FilterQueryTree" and
   * "FlattenNestedPredicates" optimizations, with all other optimizations being disabled. "-flattenNestedPredicates"
   * disables only the "FlattenNestedPredicates" optimization but keeps all other optimizations.
   *
   * @param brokerRequest The broker request from which to extract the optimization flags
   * @return The optimization flags for this request, or null.
   */
  public static @Nullable OptimizationFlags getOptimizationFlags(BrokerRequest brokerRequest) {
    if (brokerRequest == null || brokerRequest.getDebugOptions() == null) {
      return null;
    }

    String optimizationFlagString = brokerRequest.getDebugOptions().get("optimizationFlags");

    if (optimizationFlagString == null || optimizationFlagString.isEmpty()) {
      return null;
    }

    Set<String> enabledOptimizations = new HashSet<>();
    Set<String> disabledOptimizations = new HashSet<>();

    List<String> optimizations = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(optimizationFlagString);
    for (String optimization : optimizations) {
      if (optimization.charAt(0) == '+') {
        enabledOptimizations.add(optimization.substring(1));
      } else if (optimization.charAt(0) == '-') {
        disabledOptimizations.add(optimization.substring(1));
      } else {
        throw new RuntimeException("Optimization flag list contains an invalid value " + optimization
            + ", should be prefixed either with + or -");
      }
    }

    return new OptimizationFlags(enabledOptimizations, disabledOptimizations);
  }

  /**
   * Returns a friendly name for an optimization by stripping the irrelevant parts of the class name and lowering the
   * case of its first letter.
   *
   * @param clazz The class for which to get the friendly optimization name
   * @return A friendly optimization name
   */
  public static String optimizationName(Class<?> clazz) {
    String className = clazz.getSimpleName();
    String shortenedClassName;

    shortenedClassName = className.replaceAll("FilterQueryTreeOptimizer", "");

    return Character.toLowerCase(shortenedClassName.charAt(0)) + shortenedClassName.substring(1);
  }
}
