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
package org.apache.pinot.calcite.rel.hint;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;


/**
 * Default hint strategy set for Pinot query.
 */
public class PinotHintStrategyTable {
  private PinotHintStrategyTable() {
  }

  public static final HintStrategyTable PINOT_HINT_STRATEGY_TABLE = HintStrategyTable.builder()
      .hintStrategy(PinotHintOptions.AGGREGATE_HINT_OPTIONS, HintPredicates.AGGREGATE)
      .hintStrategy(PinotHintOptions.JOIN_HINT_OPTIONS, HintPredicates.JOIN)
      .hintStrategy(PinotHintOptions.TABLE_HINT_OPTIONS, HintPredicates.TABLE_SCAN)
      .build();

  /**
   * Get the first hint that has the specified name.
   */
  @Nullable
  public static RelHint getHint(Hintable hintable, String hintName) {
    return getHint(hintable.getHints(), hintName);
  }

  /**
   * Get the first hint that satisfies the predicate.
   */
  @Nullable
  public static RelHint getHint(Hintable hintable, Predicate<RelHint> predicate) {
    return getHint(hintable.getHints(), predicate);
  }

  /**
   * Check if a hint-able {@link RelNode} contains a specific {@link RelHint} by name.
   *
   * @param hintList hint list from the {@link RelNode}.
   * @param hintName the name of the {@link RelHint} to be matched
   * @return true if it contains the hint
   */
  public static boolean containsHint(List<RelHint> hintList, String hintName) {
    for (RelHint relHint : hintList) {
      if (relHint.hintName.equals(hintName)) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  public static RelHint getHint(List<RelHint> hintList, String hintName) {
    for (RelHint relHint : hintList) {
      if (relHint.hintName.equals(hintName)) {
        return relHint;
      }
    }
    return null;
  }

  @Nullable
  public static RelHint getHint(List<RelHint> hintList, Predicate<RelHint> predicate) {
    for (RelHint hint : hintList) {
      if (predicate.test(hint)) {
        return hint;
      }
    }
    return null;
  }

  /**
   * Returns the hint options for a specific hint name of {@link RelHint}, or {@code null} if the hint is not present.
   */
  @Nullable
  public static Map<String, String> getHintOptions(List<RelHint> hintList, String hintName) {
    for (RelHint relHint : hintList) {
      if (relHint.hintName.equals(hintName)) {
        return relHint.kvOptions;
      }
    }
    return null;
  }

  /**
   * Check if a hint-able {@link RelNode} contains an option key for a specific hint name of {@link RelHint}.
   *
   * @param hintList hint list from the {@link RelNode}.
   * @param hintName the name of the {@link RelHint}.
   * @param optionKey the option key to look for in the {@link RelHint#kvOptions}.
   * @return true if it contains the hint
   */
  public static boolean containsHintOption(List<RelHint> hintList, String hintName, String optionKey) {
    Map<String, String> options = getHintOptions(hintList, hintName);
    return options != null && options.containsKey(optionKey);
  }

  /**
   * Check if a hint-able {@link RelNode} has an option key as {@code true} for a specific hint name of {@link RelHint}.
   *
   * @param hintList hint list from the {@link RelNode}.
   * @param hintName the name of the {@link RelHint}.
   * @param optionKey the option key to look for in the {@link RelHint#kvOptions}.
   * @return true if it contains the hint
   */
  public static boolean isHintOptionTrue(List<RelHint> hintList, String hintName, String optionKey) {
    Map<String, String> options = getHintOptions(hintList, hintName);
    return options != null && Boolean.parseBoolean(options.get(optionKey));
  }

  /**
   * Retrieve the option value by option key in the {@link RelHint#kvOptions}. the option key is looked up from the
   * specified hint name for a hint-able {@link RelNode}.
   *
   * @param hintList hint list from the {@link RelNode}.
   * @param hintName the name of the {@link RelHint}.
   * @param optionKey the option key to look for in the {@link RelHint#kvOptions}.
   */
  @Nullable
  public static String getHintOption(List<RelHint> hintList, String hintName, String optionKey) {
    Map<String, String> options = getHintOptions(hintList, hintName);
    return options != null ? options.get(optionKey) : null;
  }
}
