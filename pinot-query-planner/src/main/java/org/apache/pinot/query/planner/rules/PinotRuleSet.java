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
package org.apache.pinot.query.planner.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.calcite.plan.RelOptRule;


/// Owns the multi-stage planner's per-phase Calcite rule lists. Constructed
/// once at broker startup; lists are immutable for the process lifetime.
///
/// Construction sequence:
///
/// 1. Allocate an empty mutable list per [Phase].
/// 2. For every [RuleSetCustomizer] (in supplied order), call
///    `customize(phase, list)` once per phase. The OSS defaults are themselves
///    contributed by [DefaultRuleSetCustomizer] which is registered as a
///    `ServiceLoader` entry; plugin customizers run after and observe a list
///    pre-populated with the OSS defaults.
/// 3. Defensively copy each per-phase list to an immutable [List] and freeze
///    the map.
///
/// `QueryEnvironment` reads `rulesFor(phase)` and applies per-query
/// `usePlannerRules` / `skipPlannerRules` filters on top.
public final class PinotRuleSet {

  private final Map<Phase, List<RelOptRule>> _rulesByPhase;

  /// Builds a rule set from the supplied customizers. Customizers run in the
  /// order of the iterable; the framework freezes the per-phase lists after
  /// every customizer has run.
  public PinotRuleSet(Iterable<RuleSetCustomizer> customizers) {
    EnumMap<Phase, List<RelOptRule>> mutable = new EnumMap<>(Phase.class);
    for (Phase phase : Phase.values()) {
      mutable.put(phase, new ArrayList<>());
    }
    for (RuleSetCustomizer customizer : customizers) {
      for (Phase phase : Phase.values()) {
        customizer.customize(phase, mutable.get(phase));
      }
    }
    EnumMap<Phase, List<RelOptRule>> frozen = new EnumMap<>(Phase.class);
    for (Map.Entry<Phase, List<RelOptRule>> entry : mutable.entrySet()) {
      frozen.put(entry.getKey(), List.copyOf(entry.getValue()));
    }
    _rulesByPhase = Collections.unmodifiableMap(frozen);
  }

  /// Discovers every [RuleSetCustomizer] via [ServiceLoader] and builds a
  /// rule set from them. Used by [#defaultInstance()] and by callers that
  /// don't have an externally-managed customizer list.
  public static PinotRuleSet loadFromServiceLoader() {
    List<RuleSetCustomizer> customizers = new ArrayList<>();
    for (RuleSetCustomizer customizer : ServiceLoader.load(RuleSetCustomizer.class)) {
      customizers.add(customizer);
    }
    return new PinotRuleSet(customizers);
  }

  /// Lazily-initialized process-wide singleton built from the
  /// `ServiceLoader`-discovered customizers. Used as the `@Value.Default` of
  /// `QueryEnvironment.Config#getRuleSet()` so per-query `Config` instances
  /// don't repeat the discovery work.
  public static PinotRuleSet defaultInstance() {
    return DefaultInstanceHolder.INSTANCE;
  }

  /// Returns the rule list for the given phase, after every customization
  /// was applied. Per-query filtering by `usePlannerRules` /
  /// `skipPlannerRules` is the caller's responsibility (see
  /// `QueryEnvironment#getOptProgram`).
  public List<RelOptRule> rulesFor(Phase phase) {
    return _rulesByPhase.getOrDefault(phase, List.of());
  }

  private static final class DefaultInstanceHolder {
    static final PinotRuleSet INSTANCE = loadFromServiceLoader();
  }
}
