package org.apache.pinot.query.context;

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

import it.unimi.dsi.fastutil.Pair;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RuleTimingPlannerListener implements RelOptListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(RuleTimingPlannerListener.class);
  public static final String RULE_TIMINGS = "RULE_TIMINGS";

  private final PlannerContext _plannerContext;
  private final Map<RelOptRule, Long> _ruleStartTimes = new HashMap<>();
  private final Map<RelOptRule, Long> _ruleDurations = new HashMap<>();
  private final boolean _traceRuleProductions;
  private final List<Pair<RelOptRuleCall, RelNode>> _ruleProductions;

  public RuleTimingPlannerListener(PlannerContext plannerContext) {
    _plannerContext = plannerContext;
    _traceRuleProductions = QueryOptionsUtils.isTraceRuleProductions(plannerContext.getOptions());
    _ruleProductions = _traceRuleProductions ? new ArrayList<>() : null;
  }

  @Override
  public void ruleAttempted(RuleAttemptedEvent event) {
    // Capture start time when a rule is attempted
    if (event.isBefore()) {
      _ruleStartTimes.put(event.getRuleCall().getRule(), System.nanoTime());
    } else {
      if (_ruleStartTimes.containsKey(event.getRuleCall().getRule())) {
        long duration = System.nanoTime() - _ruleStartTimes.get(event.getRuleCall().getRule());
        _ruleDurations.put(event.getRuleCall().getRule(),
            _ruleDurations.getOrDefault(event.getRuleCall().getRule(), 0L) + duration);
      }
    }
  }

  @Override
  public void ruleProductionSucceeded(RuleProductionEvent event) {
    if (_traceRuleProductions && !event.isBefore()) {
      _ruleProductions.add(Pair.of(event.getRuleCall(), event.getRel()));
    }
  }

  @Override
  public void relEquivalenceFound(RelEquivalenceEvent event) {
    /* Not used */
  }

  @Override
  public void relDiscarded(RelDiscardedEvent event) {
    /* Not used */
  }

  @Override
  public void relChosen(RelChosenEvent event) {
    /* Not used */
  }

  public void printRuleTimings() {
    LOGGER.info(getRuleTimings(SqlExplainFormat.DOT));
  }

  public void populateRuleTimings() {
    _plannerContext.getPlannerOutput().put(RULE_TIMINGS, getRuleTimings(_plannerContext.getSqlExplainFormat()));
  }

  public String getRuleTimings(SqlExplainFormat format) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    switch (format) {
      case XML:
        pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        pw.println("<RuleExecutionTimes>");
        for (Map.Entry<RelOptRule, Long> entry : _ruleDurations.entrySet()) {
          String ruleName = entry.getKey().toString()
              .replace("&", "&amp;")
              .replace("<", "&lt;")
              .replace(">", "&gt;")
              .replace("\"", "&quot;")
              .replace("'", "&apos;");
          pw.println("\t<Rule>");
          pw.println("\t\t<Name>" + ruleName + "</Name>");
          pw.println("\t\t<Time>" + entry.getValue() / 1_000_000.0 + "</Time>");
          pw.println("\t</Rule>");
        }
        pw.println("</RuleExecutionTimes>");
        if (_traceRuleProductions) {
          pw.println("<RuleProductions>");
          for (Pair<RelOptRuleCall, RelNode> entry : _ruleProductions) {
            String ruleName = entry.first().getRule().toString()
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
            String beforeRel = RelOptUtil.toString(entry.first().rel(0))
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
            String afterRel = RelOptUtil.toString(entry.second())
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
            pw.println("\t<Rule>");
            pw.println("\t\t<Name>" + ruleName + "</Name>");
            pw.println("\t\t<Before>" + beforeRel + "</Before>");
            pw.println("\t\t<After>" + afterRel + "</After>");
            pw.println("\t</Rule>");
          }
          pw.println("</RuleProductions>");
        }
        break;
      case JSON:
        pw.println("{");
        pw.println("  \"ruleExecutionTimes\": [");
        boolean firstEntry = true;
        for (Map.Entry<RelOptRule, Long> entry : _ruleDurations.entrySet()) {
          if (!firstEntry) {
            pw.println(",");
          }
          firstEntry = false;
          // Escape special JSON characters
          String ruleName = entry.getKey().toString()
              .replace("\\", "\\\\")
              .replace("\"", "\\\"")
              .replace("\b", "\\b")
              .replace("\f", "\\f")
              .replace("\n", "\\n")
              .replace("\r", "\\r")
              .replace("\t", "\\t");
          double timeMillis = entry.getValue() / 1_000_000.0;
          pw.println("    {");
          pw.print("      \"rule\": \"");
          pw.print(ruleName);
          pw.println("\", ");
          pw.print("      \"time\": ");
          pw.printf("%.2f\n", timeMillis); // Format to 2 decimal places
          pw.print("    }");
        }
        pw.println("  ]");
        pw.println("}");
        if (_traceRuleProductions) {
          pw.println(",");
          pw.println("{");
          pw.println("  \"ruleProductions\": [");
          firstEntry = true;
          for (Pair<RelOptRuleCall, RelNode> entry : _ruleProductions) {
            if (!firstEntry) {
              pw.println(",");
            }
            firstEntry = false;
            // Escape special JSON characters
            String ruleName = entry.first().getRule().toString()
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
            String beforeRel = RelOptUtil.toString(entry.first().rel(0))
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
            String afterRel = RelOptUtil.toString(entry.second())
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
            pw.println("    {");
            pw.print("      \"rule\": \"");
            pw.print(ruleName);
            pw.println("\", ");
            pw.print("      \"before\": ");
            pw.printf(beforeRel); // Format to 2 decimal places
            pw.print("      \"after\": ");
            pw.printf(afterRel); // Format to 2 decimal places
            pw.print("    }");
          }
          pw.println("  ]");
          pw.println("}");
        }
        break;
      case DOT:
        pw.println("digraph PlannerTimings {");
        for (Map.Entry<RelOptRule, Long> entry : _ruleDurations.entrySet()) {
          pw.print("Rule: ");
          pw.print(entry.getKey());
          pw.print(" -> Time: ");
          pw.println(entry.getValue() / 1_000_000.0);
        }
        pw.println("}");
        if (_traceRuleProductions) {
          pw.println("\ndigraph RuleProductions {");
          for (Pair<RelOptRuleCall, RelNode> entry : _ruleProductions) {
            pw.print("Rule: ");
            pw.print(entry.first().getRule());
            pw.print("\nBefore:\n");
            pw.println(RelOptUtil.toString(entry.first().rel(0)).strip());
            pw.print("After:\n");
            pw.println(RelOptUtil.toString(entry.second()));
          }
          pw.println("}");
        }
        break;
      default:
        pw.println("Rule Execution Times");
        for (Map.Entry<RelOptRule, Long> entry : _ruleDurations.entrySet()) {
          pw.print("Rule: ");
          pw.print(entry.getKey());
          pw.print(" -> Time: ");
          pw.println(entry.getValue() / 1_000_000.0);
        }
        if (_traceRuleProductions) {
          pw.println("\nRule Productions");
          for (Pair<RelOptRuleCall, RelNode> entry : _ruleProductions) {
            pw.print("Rule: ");
            pw.print(entry.first().getRule());
            pw.print("\nBefore:\n");
            pw.println(RelOptUtil.toString(entry.first().rel(0)).strip());
            pw.print("After:\n");
            pw.println(RelOptUtil.toString(entry.second()));
          }
        }
        break;
    }
    pw.flush();
    return sw.toString();
  }
}
