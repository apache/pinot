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


/// HEP phases the multi-stage planner exposes to plugin
/// [RuleSetCustomizer]s. Every phase corresponds to one slot in the rule
/// programs built by `QueryEnvironment#getOptProgram` /
/// `QueryEnvironment#getTraitProgram`.
///
/// **Stability**: this enum is append-only — new phases may be added at the
/// end without breaking plugins compiled against an older version. Existing
/// phases must never be reordered or removed.
public enum Phase {
  /// Basic logical-rewrite phase. HEP, depth-first.
  /// OSS defaults: `DefaultRuleSetCustomizer.BASIC_RULES`.
  BASIC,

  /// Filter pushdown rules. The HEP program runs this phase twice (around the
  /// project pushdown phase). OSS defaults:
  /// `DefaultRuleSetCustomizer.FILTER_PUSHDOWN_RULES`.
  FILTER_PUSHDOWN,

  /// Project pushdown rules.
  /// OSS defaults: `DefaultRuleSetCustomizer.PROJECT_PUSHDOWN_RULES`.
  PROJECT_PUSHDOWN,

  /// Top-down pruning rules.
  /// OSS defaults: `DefaultRuleSetCustomizer.PRUNE_RULES`.
  PRUNE,

  /// Post-logical rules used when the physical optimizer is **not** enabled.
  /// OSS defaults: `DefaultRuleSetCustomizer.POST_LOGICAL_RULES`. The list
  /// includes `PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY` configured with
  /// the rule's hard-coded default `fetchLimitThreshold`. Per-query overrides
  /// (and broker-config overrides) are applied by `QueryEnvironment` swapping
  /// the rule on a per-query copy of this list.
  POST_LOGICAL,

  /// Post-logical rules used when the physical optimizer **is** enabled.
  /// OSS defaults: `DefaultRuleSetCustomizer.POST_LOGICAL_V2_RULES`.
  POST_LOGICAL_V2,

  /// Conditional enriched-join rules applied after the post-logical phase
  /// when the `JOIN_TO_ENRICHED_JOIN` rule is not skipped.
  /// OSS defaults: `PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES`.
  POST_LOGICAL_ENRICHED_JOIN
}
