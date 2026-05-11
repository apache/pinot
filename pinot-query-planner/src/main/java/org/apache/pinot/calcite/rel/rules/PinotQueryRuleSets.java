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
package org.apache.pinot.calcite.rel.rules;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.pinot.query.planner.rules.DefaultRuleSetCustomizer;


/// Deprecated bridge to [DefaultRuleSetCustomizer]'s per-phase rule lists.
///
/// All rule lists have been consolidated into [DefaultRuleSetCustomizer].
/// Callers that reference the old static fields here should migrate to that
/// class. The per-query `sortExchangeCopyLimit` parameter of
/// [#getPinotPostRules] is now handled by
/// `QueryEnvironment.getTraitProgram`; the parameter is ignored in this bridge.
@Deprecated
public final class PinotQueryRuleSets {

  private PinotQueryRuleSets() {
  }

  @Deprecated
  public static final List<RelOptRule> BASIC_RULES = DefaultRuleSetCustomizer.BASIC_RULES;

  @Deprecated
  public static final List<RelOptRule> FILTER_PUSHDOWN_RULES = DefaultRuleSetCustomizer.FILTER_PUSHDOWN_RULES;

  @Deprecated
  public static final List<RelOptRule> PROJECT_PUSHDOWN_RULES = DefaultRuleSetCustomizer.PROJECT_PUSHDOWN_RULES;

  @Deprecated
  public static final List<RelOptRule> PRUNE_RULES = DefaultRuleSetCustomizer.PRUNE_RULES;

  @Deprecated
  public static final List<RelOptRule> PINOT_POST_RULES_V2 = DefaultRuleSetCustomizer.POST_LOGICAL_PHYSICAL_RULES;

  /// Returns the default POST_LOGICAL rule list.
  ///
  /// **WARNING: the `sortExchangeCopyLimit` parameter is silently ignored.**
  /// The per-query sort-exchange-copy threshold is now applied by
  /// `QueryEnvironment.getTraitProgram` on a per-query copy of the POST_LOGICAL
  /// list. Callers that relied on a custom limit must migrate to using
  /// `QueryEnvironment` query options instead.
  @Deprecated
  public static List<RelOptRule> getPinotPostRules(int sortExchangeCopyLimit) {
    return DefaultRuleSetCustomizer.POST_LOGICAL_RULES;
  }
}
