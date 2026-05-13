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

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.pinot.calcite.rel.rules.PinotQueryRuleSets;
import org.apache.pinot.query.planner.spi.Phase;
import org.apache.pinot.query.planner.spi.RuleSetCustomizer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Unit tests for [PinotRuleSet] and the [RuleSetCustomizer] SPI.
public class PinotRuleSetTest {

  @Test
  public void defaultsSeedEveryPhaseFromDefaultRuleSetCustomizer() {
    PinotRuleSet ruleSet = new PinotRuleSet(List.of(new DefaultRuleSetCustomizer()));

    assertEquals(ruleSet.rulesFor(Phase.BASIC), PinotQueryRuleSets.BASIC_RULES);
    assertEquals(ruleSet.rulesFor(Phase.FILTER_PUSHDOWN), PinotQueryRuleSets.FILTER_PUSHDOWN_RULES);
    assertEquals(ruleSet.rulesFor(Phase.PROJECT_PUSHDOWN), PinotQueryRuleSets.PROJECT_PUSHDOWN_RULES);
    assertEquals(ruleSet.rulesFor(Phase.PRUNE), PinotQueryRuleSets.PRUNE_RULES);
    assertEquals(ruleSet.rulesFor(Phase.POST_LOGICAL), PinotQueryRuleSets.POST_LOGICAL_RULES);
    assertEquals(ruleSet.rulesFor(Phase.POST_LOGICAL_PHYSICAL), PinotQueryRuleSets.PINOT_POST_RULES_V2);
    assertTrue(ruleSet.rulesFor(Phase.POST_LOGICAL_ENRICHED_JOIN).size() > 0,
        "POST_LOGICAL_ENRICHED_JOIN should be populated by PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES");
  }

  @Test
  public void serviceLoaderDiscoveryFindsDefault() {
    // The DefaultRuleSetCustomizer is registered via META-INF/services, so the
    // default instance built by ServiceLoader exposes the OSS defaults.
    PinotRuleSet ruleSet = PinotRuleSet.defaultInstance();
    assertTrue(ruleSet.rulesFor(Phase.BASIC).size() > 0);
    assertTrue(ruleSet.rulesFor(Phase.POST_LOGICAL).size() > 0);
  }

  @Test
  public void customizerCanAppendRule() {
    RelOptRule extraRule = PinotQueryRuleSets.BASIC_RULES.get(0);
    int defaultSize = PinotQueryRuleSets.BASIC_RULES.size();

    RuleSetCustomizer plugin = (phase, rules) -> {
      if (phase == Phase.BASIC) {
        rules.add(extraRule);
      }
    };
    PinotRuleSet ruleSet = new PinotRuleSet(List.of(new DefaultRuleSetCustomizer(), plugin));

    assertEquals(ruleSet.rulesFor(Phase.BASIC).size(), defaultSize + 1);
    assertSame(ruleSet.rulesFor(Phase.BASIC).get(defaultSize), extraRule);
  }

  @Test
  public void customizerCanRemoveOssRuleByName() {
    String firstRuleName = PinotQueryRuleSets.BASIC_RULES.get(0).toString();
    int defaultSize = PinotQueryRuleSets.BASIC_RULES.size();

    RuleSetCustomizer plugin = (phase, rules) -> {
      if (phase == Phase.BASIC) {
        rules.removeIf(r -> firstRuleName.equals(r.toString()));
      }
    };
    PinotRuleSet ruleSet = new PinotRuleSet(List.of(new DefaultRuleSetCustomizer(), plugin));

    assertEquals(ruleSet.rulesFor(Phase.BASIC).size(), defaultSize - 1);
    assertTrue(ruleSet.rulesFor(Phase.BASIC).stream().noneMatch(r -> firstRuleName.equals(r.toString())));
  }

  @Test
  public void rulesForUnseededPhaseReturnsEmpty() {
    // No customizers — every phase comes back empty.
    PinotRuleSet ruleSet = new PinotRuleSet(List.of());
    for (Phase phase : Phase.values()) {
      assertEquals(ruleSet.rulesFor(phase).size(), 0, phase + " should be empty");
    }
  }
}
