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
package org.apache.pinot.query;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.PlannerRuleNames;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.skipRule;
import static org.testng.Assert.*;


public class QueryPlannerRuleOptionsTest extends QueryEnvironmentTestBase {

  private String explainQueryWithRuleDisabled(String query, String skipRule) {
    SqlNode sqlNode = CalciteSqlParser.compileToSqlNodeAndOptions(query).getSqlNode();
    Map<String, String> options = new HashMap<>();
    // disable rule
    options.put(skipRule, "true");
    SqlNodeAndOptions sqlNodeAndOptions =
        new SqlNodeAndOptions(
            sqlNode,
            PinotSqlType.DQL,
            QueryOptionsUtils.resolveCaseInsensitiveOptions(options));
    return _queryEnvironment
        .compile(query, sqlNodeAndOptions)
        .explain(RANDOM_REQUEST_ID_GEN.nextLong(), null)
        .getExplainPlan();
  }

  @Test
  public void testDisableCaseToFilter() {
    // Tests that when skipAggregateCaseToFilterRule=true,
    // CASE WHEN should not be optimized
    String query = "EXPLAIN PLAN FOR SELECT SUM(CASE WHEN col1 = 'a' THEN 1 ELSE 0 END) FROM a";
    String explain = explainQueryWithRuleDisabled(query, skipRule(PlannerRuleNames.AGGREGATE_CASE_TO_FILTER));

    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalProject(EXPR$0=[CASE(=($1, 0), null:BIGINT, $0)])\n"
            + "  PinotLogicalAggregate(group=[{}], agg#0=[$SUM0($0)], agg#1=[COUNT($1)], aggType=[FINAL])\n"
            + "    PinotLogicalExchange(distribution=[hash])\n"
            + "      PinotLogicalAggregate(group=[{}], agg#0=[$SUM0($0)], agg#1=[COUNT()], aggType=[LEAF])\n"
            + "        LogicalProject($f0=[CASE(=($0, _UTF-8'a'), 1, 0)])\n"
            + "          PinotLogicalTableScan(table=[[default, a]])\n");
    //@formatter:on
  }

  @Test
  public void testDisableReduceFunctions() {
    // Tests that when skipPinotAggregateReduceFunctionsRule=true,
    // SUM should not be reduced
    String query = "EXPLAIN PLAN FOR SELECT SUM(CASE WHEN col1 = 'a' THEN 3 ELSE 0 END) FROM a";

    String explain = explainQueryWithRuleDisabled(query,
        skipRule(PlannerRuleNames.PINOT_AGGREGATE_REDUCE_FUNCTIONS));
    //@formatter:off
    assertEquals(explain,
      "Execution Plan\n"
          + "PinotLogicalAggregate(group=[{}], agg#0=[SUM($0)], aggType=[FINAL])\n"
          + "  PinotLogicalExchange(distribution=[hash])\n"
          + "    PinotLogicalAggregate(group=[{}], agg#0=[SUM($0)], aggType=[LEAF])\n"
          + "      LogicalProject($f0=[CASE(=($0, _UTF-8'a'), 3, 0)])\n"
          + "        PinotLogicalTableScan(table=[[default, a]])\n");
    //@formatter:on
  }

  @Ignore("PruneEmptyRules.CORRELATE_LEFT_INSTANCE and its right equivalent will be added in the future")
  @Test
  public void testDisablePruneEmptyJoinLeft() {
    // Test that when skipPruneEmptyJoinLeft=true,
    String query = "EXPLAIN PLAN FOR SELECT *\n"
        + "FROM (\n"
        + "  SELECT * FROM a WHERE 1 = 0\n"
        + ") c\n"
        + "JOIN a ON c.col1 = a.col1 ;\n";

    String explain = explainQueryWithRuleDisabled(query,
        skipRule(PlannerRuleNames.PRUNE_EMPTY_CORRELATE_LEFT));
    //@formatter:off
    assertEquals(explain,
      "Execution Plan\n"
          + "LogicalJoin(condition=[=($0, $9)], joinType=[inner])\n"
          + "  PinotLogicalExchange(distribution=[hash[0]])\n"
          + "    LogicalValues(tuples=[[]])\n"
          + "  PinotLogicalExchange(distribution=[hash[0]])\n"
          + "    PinotLogicalTableScan(table=[[default, a]])\n");
    //@formatter:on
  }

  @Test
  public void testDisablePruneEmptyUnion() {
    // Test the description of PruneEmptyRules.UNION_INSTANCE
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT col1 FROM a\n"
        + "WHERE 1 = 0\n"
        + "UNION\n"
        + "SELECT col1 FROM b;\n";

    String explain = explainQueryWithRuleDisabled(query,
        skipRule(PlannerRuleNames.PRUNE_EMPTY_UNION));
    //@formatter:off
    assertEquals(explain,
      "Execution Plan\n"
          + "PinotLogicalAggregate(group=[{0}], aggType=[FINAL])\n"
          + "  PinotLogicalExchange(distribution=[hash[0]])\n"
          + "    PinotLogicalAggregate(group=[{0}], aggType=[LEAF])\n"
          + "      LogicalUnion(all=[true])\n"
          + "        PinotLogicalExchange(distribution=[hash[0]])\n"
          + "          LogicalValues(tuples=[[]])\n"
          + "        PinotLogicalExchange(distribution=[hash[0]])\n"
          + "          LogicalProject(col1=[$0])\n"
          + "            PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testDisablePinotEvaluateProjectLiteralRule() {
    // Test the knob of turning off PinotEvaluateLiteralRule.Project
    // works with the customized description
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT col1, ABS(-1) FROM b;\n";

    String explain = explainQueryWithRuleDisabled(query, skipRule(PlannerRuleNames.PINOT_EVALUATE_LITERAL_PROJECT));
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalProject(col1=[$0], EXPR$1=[ABS(-1)])\n"
            + "  PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testDisablePinotEvaluateFilterLiteralRule() {
    // Test the knob of turning off PinotEvaluateLiteralRule.Filter
    // works with the customized description
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT col1 FROM b WHERE col1=ABS(-1);\n";

    String explain = explainQueryWithRuleDisabled(query, skipRule(PlannerRuleNames.PINOT_EVALUATE_LITERAL_FILTER));
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalProject(col1=[$0])\n"
            + "  LogicalFilter(condition=[=(CAST($0):INTEGER NOT NULL, ABS(-1))])\n"
            + "    PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testDisablePinotProjectJoinTransposeRule() {
    // Test the knob of turning off PinotProjectJoinTransposeRule
    // works with default description
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT CASE WHEN a.col1=1 THEN 1 ELSE 0 END, b.col2 FROM a JOIN b ON a.col1=b.col1;\n";

    String explain = explainQueryWithRuleDisabled(query, skipRule(PlannerRuleNames.PINOT_PROJECT_JOIN_TRANSPOSE));
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalProject(EXPR$0=[CASE(=(CAST($0):INTEGER NOT NULL, 1), 1, 0)], col2=[$2])\n"
            + "  LogicalJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      LogicalProject(col1=[$0])\n"
            + "        PinotLogicalTableScan(table=[[default, a]])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      LogicalProject(col1=[$0], col2=[$1])\n"
            + "        PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }
}
