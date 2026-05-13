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

import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.pinot.calcite.rel.rules.PinotEnrichedJoinRule;
import org.apache.pinot.calcite.rel.rules.PinotQueryRuleSets;


/// [RuleSetCustomizer] that seeds every [Phase] with the OSS default Calcite
/// rules for the multi-stage query planner. Registered as a [java.util.ServiceLoader]
/// service via [@AutoService], picked up automatically by [PinotRuleSet].
///
/// Rule lists are defined in [PinotQueryRuleSets] and may be consolidated here
/// in a future refactor once the [RuleSetCustomizer] SPI is the established
/// extension point for broker rule customization.
@AutoService(RuleSetCustomizer.class)
public final class DefaultRuleSetCustomizer implements RuleSetCustomizer {

  /// No-arg constructor required by [java.util.ServiceLoader].
  public DefaultRuleSetCustomizer() {
  }

  @Override
  public void customize(Phase phase, List<RelOptRule> rules) {
    switch (phase) {
      case BASIC:
        rules.addAll(PinotQueryRuleSets.BASIC_RULES);
        return;
      case FILTER_PUSHDOWN:
        rules.addAll(PinotQueryRuleSets.FILTER_PUSHDOWN_RULES);
        return;
      case PROJECT_PUSHDOWN:
        rules.addAll(PinotQueryRuleSets.PROJECT_PUSHDOWN_RULES);
        return;
      case PRUNE:
        rules.addAll(PinotQueryRuleSets.PRUNE_RULES);
        return;
      case POST_LOGICAL:
        rules.addAll(PinotQueryRuleSets.POST_LOGICAL_RULES);
        return;
      case POST_LOGICAL_PHYSICAL:
        rules.addAll(PinotQueryRuleSets.PINOT_POST_RULES_V2);
        return;
      case POST_LOGICAL_ENRICHED_JOIN:
        rules.addAll(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
        return;
      default:
        throw new IllegalStateException(
            "DefaultRuleSetCustomizer is missing OSS rule defaults for Phase." + phase
                + "; extend the switch when adding a new Phase value.");
    }
  }
}
