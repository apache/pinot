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
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.PlannerRuleNames;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class QueryPlannerRuleOptionsTest extends QueryEnvironmentTestBase {

  private String explainQueryWithRuleDisabled(String query, String ruleToSkip) {
    SqlNode sqlNode = CalciteSqlParser.compileToSqlNodeAndOptions(query).getSqlNode();
    Map<String, String> options = new HashMap<>();
    // disable rule
    options.put(CommonConstants.Broker.Request.QueryOptionKey.SKIP_PLANNER_RULES, ruleToSkip);
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

  private String explainQueryWithRuleEnabled(String query, String ruleToEnable) {
    SqlNode sqlNode = CalciteSqlParser.compileToSqlNodeAndOptions(query).getSqlNode();
    Map<String, String> options = new HashMap<>();
    // disable rule
    options.put(CommonConstants.Broker.Request.QueryOptionKey.USE_PLANNER_RULES, ruleToEnable);
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
    String explain = explainQueryWithRuleDisabled(query, PlannerRuleNames.AGGREGATE_CASE_TO_FILTER);

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

    String explain = explainQueryWithRuleDisabled(query, PlannerRuleNames.AGGREGATE_REDUCE_FUNCTIONS);
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

  @Test
  public void testDisablePruneEmptyCorrelateLeft() {
    // Test that when skipPruneEmptyJoinLeft=true, some queries involving correlate and dummy conditions
    // fail to be unnested
    String query = "EXPLAIN PLAN FOR SELECT *\n"
        + "FROM a WHERE EXISTS (\n"
        + "  SELECT * FROM b WHERE a.col1 = b.col1\n"
        + ") AND 1=0;\n";

    String explain = explainQueryWithRuleDisabled(query,
        PlannerRuleNames.PRUNE_EMPTY_CORRELATE_LEFT);
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalProject(col1=[$0], col2=[$1], col3=[$2], "
            + "col4=[$3], col5=[$4], col6=[$5], col7=[$6], ts=[$7], ts_timestamp=[$8])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($9)])\n"
            + "    LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0}])\n"
            + "      LogicalValues(tuples=[[]])\n"
            + "      PinotLogicalAggregate(group=[{}], agg#0=[MIN($0)], aggType=[FINAL])\n"
            + "        PinotLogicalExchange(distribution=[hash])\n"
            + "          PinotLogicalAggregate(group=[{}], agg#0=[MIN($0)], aggType=[LEAF])\n"
            + "            LogicalProject($f0=[true])\n"
            + "              LogicalFilter(condition=[=($cor0.col1, $0)])\n"
            + "                LogicalProject(col1=[$0])\n"
            + "                  PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testDisablePruneEmptyJoinLeft() {
    // without PruneEmptyRules.PRUNE_EMPTY_JOIN_LEFT,
    // some unnested queries that produces join with dummy
    // does not get simplified further to remove the join
    String query = "EXPLAIN PLAN FOR SELECT *\n"
        + "FROM (\n"
        + "  SELECT * FROM a WHERE 1 = 0\n"
        + ") t1\n"
        + "WHERE EXISTS (\n"
        + "  SELECT 1\n"
        + "  FROM a\n"
        + "  WHERE a.col1 = t1.col1\n"
        + ");\n";

    String explain = explainQueryWithRuleDisabled(query, PlannerRuleNames.PRUNE_EMPTY_JOIN_LEFT);
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalJoin(condition=[=($0, $9)], joinType=[semi])\n"
            + "  PinotLogicalExchange(distribution=[hash[0]])\n"
            + "    LogicalValues(tuples=[[]])\n"
            + "  PinotLogicalExchange(distribution=[hash[0]])\n"
            + "    LogicalProject(col1=[$0])\n"
            + "      PinotLogicalTableScan(table=[[default, a]])\n");
    //@formatter:on
  }

  @Test
  public void testDisablePruneEmptyUnion() {
    // Test the knob of PruneEmptyRules.UNION_INSTANCE
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT col1 FROM a\n"
        + "WHERE 1 = 0\n"
        + "UNION\n"
        + "SELECT col1 FROM b;\n";

    String explain = explainQueryWithRuleDisabled(query,
        PlannerRuleNames.PRUNE_EMPTY_UNION);
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
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT col1, ABS(-1) FROM b;\n";

    String explain = explainQueryWithRuleDisabled(query, PlannerRuleNames.EVALUATE_LITERAL_PROJECT);
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
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT col1 FROM b WHERE col1=ABS(-1);\n";

    String explain = explainQueryWithRuleDisabled(query, PlannerRuleNames.EVALUATE_LITERAL_FILTER);
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
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT CASE WHEN a.col1=1 THEN 1 ELSE 0 END, b.col2 FROM a JOIN b ON a.col1=b.col1;\n";

    String explain = explainQueryWithRuleDisabled(query, PlannerRuleNames.PROJECT_JOIN_TRANSPOSE);
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

  @Test
  public void testDisablePinotJoinPushTransitivePredicateRule() {
    // test disable PinotJoinPushTransitivePredicatesRule
    //
    String query = "EXPLAIN PLAN FOR\n"
        + "SELECT * FROM a\n"
        + "JOIN b\n"
        + "ON a.col1 = b.col1\n"
        + "WHERE a.col1 = 1;\n";

    String explain = explainQueryWithRuleDisabled(query, PlannerRuleNames.JOIN_PUSH_TRANSITIVE_PREDICATES);
    //@formatter:off
    assertEquals(explain,
    "Execution Plan\n"
        + "LogicalJoin(condition=[=($0, $9)], joinType=[inner])\n"
        + "  PinotLogicalExchange(distribution=[hash[0]])\n"
        + "    LogicalFilter(condition=[=(CAST($0):INTEGER NOT NULL, 1)])\n"
        + "      PinotLogicalTableScan(table=[[default, a]])\n"
        + "  PinotLogicalExchange(distribution=[hash[0]])\n"
        + "    PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testDisablePinotFilterIntoJoinRule() {
    // test disable PinotJoinPushTransitivePredicatesRule
    String query = "EXPLAIN PLAN FOR\n"
        + "SELECT * FROM a, b\n"
        + "WHERE a.col1 = b.col1\n";

    String explain = explainQueryWithRuleDisabled(query, PlannerRuleNames.FILTER_INTO_JOIN);
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalFilter(condition=[=($0, $9)])\n"
            + "  LogicalJoin(condition=[true], joinType=[inner])\n"
            + "    PinotLogicalExchange(distribution=[random])\n"
            + "      PinotLogicalTableScan(table=[[default, a]])\n"
            + "    PinotLogicalExchange(distribution=[broadcast])\n"
            + "      PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testDisableTwoRulesSeparatedByComma() {
    // test disable PinotJoinPushTransitivePredicatesRule
    String query = "EXPLAIN PLAN FOR\n"
        + "SELECT * FROM a, b\n"
        + "WHERE a.col1 = b.col1\n";

    String explain = explainQueryWithRuleDisabled(query, PlannerRuleNames.FILTER_INTO_JOIN.concat(",")
        .concat(PlannerRuleNames.PROJECT_REMOVE));
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalProject(col1=[$0], col2=[$1], col3=[$2], col4=[$3], col5=[$4], col6=[$5], col7=[$6],"
            + " ts=[$7], ts_timestamp=[$8], col10=[$9], col20=[$10], col30=[$11], col40=[$12], col50=[$13],"
            + " col60=[$14], col70=[$15], ts0=[$16], ts_timestamp0=[$17])\n"
            + "  LogicalFilter(condition=[=($0, $9)])\n"
            + "    LogicalJoin(condition=[true], joinType=[inner])\n"
            + "      PinotLogicalExchange(distribution=[random])\n"
            + "        PinotLogicalTableScan(table=[[default, a]])\n"
            + "      PinotLogicalExchange(distribution=[broadcast])\n"
            + "        PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testAggregateJoinTransposeExtendedDisabledByDefault() {
    // test aggregate function pushdown is disabled by default
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT SUM(b.col2)\n"
        + "FROM a INNER JOIN b\n"
        + "ON a.col1 = b.col1\n"
        + "GROUP BY a.col1, b.col1\n";

    String explain = _queryEnvironment.explainQuery(query, RANDOM_REQUEST_ID_GEN.nextLong());
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalProject(EXPR$0=[$2])\n"
            + "  PinotLogicalAggregate(group=[{0, 1}], agg#0=[$SUM0($2)], aggType=[FINAL])\n"
            + "    PinotLogicalExchange(distribution=[hash[0, 1]])\n"
            + "      PinotLogicalAggregate(group=[{0, 1}], agg#0=[$SUM0($2)], aggType=[LEAF])\n"
            + "        LogicalJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "          PinotLogicalExchange(distribution=[hash[0]])\n"
            + "            LogicalProject(col1=[$0])\n"
            + "              PinotLogicalTableScan(table=[[default, a]])\n"
            + "          PinotLogicalExchange(distribution=[hash[0]])\n"
            + "            LogicalProject(col1=[$0], $f2=[CAST($1):DECIMAL(2000, 1000) NOT NULL])\n"
            + "              PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testEnableAggregateJoinTransposeExtended() {
    // test aggregate function pushdown is disabled by default
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT SUM(b.col2)\n"
        + "FROM a INNER JOIN b\n"
        + "ON a.col1 = b.col1\n"
        + "GROUP BY a.col1, b.col1\n";

    String explain = explainQueryWithRuleEnabled(query, PlannerRuleNames.AGGREGATE_JOIN_TRANSPOSE_EXTENDED);
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalProject(EXPR$0=[CAST(*($1, $3)):DECIMAL(2000, 1000) NOT NULL])\n"
            + "  LogicalJoin(condition=[=($0, $2)], joinType=[inner])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      PinotLogicalAggregate(group=[{0}], agg#0=[COUNT($1)], aggType=[FINAL])\n"
            + "        PinotLogicalExchange(distribution=[hash[0]])\n"
            + "          PinotLogicalAggregate(group=[{0}], agg#0=[COUNT()], aggType=[LEAF])\n"
            + "            PinotLogicalTableScan(table=[[default, a]])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      PinotLogicalAggregate(group=[{0}], agg#0=[$SUM0($1)], aggType=[FINAL])\n"
            + "        PinotLogicalExchange(distribution=[hash[0]])\n"
            + "          PinotLogicalAggregate(group=[{0}], agg#0=[$SUM0($1)], aggType=[LEAF])\n"
            + "            LogicalProject(col1=[$0], $f2=[CAST($1):DECIMAL(2000, 1000) NOT NULL])\n"
            + "              PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testSortJoinTransposeDisabledByDefault() {
    // test aggregate function pushdown is disabled by default
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT a.col1, b.col1\n"
        + "FROM a LEFT JOIN b\n"
        + "ON a.col1 = b.col1\n"
        + "ORDER BY a.col1\n";

    String explain = _queryEnvironment.explainQuery(query, RANDOM_REQUEST_ID_GEN.nextLong());
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[0]], "
            + "isSortOnSender=[false], isSortOnReceiver=[true])\n"
            + "    LogicalJoin(condition=[=($0, $1)], joinType=[left])\n"
            + "      PinotLogicalExchange(distribution=[hash[0]])\n"
            + "        LogicalProject(col1=[$0])\n"
            + "          PinotLogicalTableScan(table=[[default, a]])\n"
            + "      PinotLogicalExchange(distribution=[hash[0]])\n"
            + "        LogicalProject(col1=[$0])\n"
            + "          PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testEnableSortJoinTranspose() {
    // test aggregate function pushdown is disabled by default
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT a.col1, b.col1\n"
        + "FROM a LEFT JOIN b\n"
        + "ON a.col1 = b.col1\n"
        + "ORDER BY a.col1\n";

    String explain = explainQueryWithRuleEnabled(query, PlannerRuleNames.SORT_JOIN_TRANSPOSE);
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[0]], "
            + "isSortOnSender=[false], isSortOnReceiver=[true])\n"
            + "    LogicalJoin(condition=[=($0, $1)], joinType=[left])\n"
            + "      PinotLogicalExchange(distribution=[hash[0]])\n"
            + "        LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "          PinotLogicalSortExchange(distribution=[hash], collation=[[0]], "
            + "isSortOnSender=[false], isSortOnReceiver=[true])\n"
            + "            LogicalProject(col1=[$0])\n"
            + "              PinotLogicalTableScan(table=[[default, a]])\n"
            + "      PinotLogicalExchange(distribution=[hash[0]])\n"
            + "        LogicalProject(col1=[$0])\n"
            + "          PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testSortJoinCopyDisabledByDefault() {
    // test aggregate function pushdown is disabled by default
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT a.col1, b.col1\n"
        + "FROM a INNER JOIN b\n"
        + "ON a.col1 = b.col1\n"
        + "ORDER BY a.col1\n";

    String explain = _queryEnvironment.explainQuery(query, RANDOM_REQUEST_ID_GEN.nextLong());
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[0]], "
            + "isSortOnSender=[false], isSortOnReceiver=[true])\n"
            + "    LogicalJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "      PinotLogicalExchange(distribution=[hash[0]])\n"
            + "        LogicalProject(col1=[$0])\n"
            + "          PinotLogicalTableScan(table=[[default, a]])\n"
            + "      PinotLogicalExchange(distribution=[hash[0]])\n"
            + "        LogicalProject(col1=[$0])\n"
            + "          PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testEnableSortJoinCopy() {
    // test aggregate function pushdown is disabled by default
    String query = "EXPLAIN PLAN FOR \n"
        + "SELECT a.col1, b.col1\n"
        + "FROM a INNER JOIN b\n"
        + "ON a.col1 = b.col1\n"
        + "ORDER BY a.col1\n";

    String explain = explainQueryWithRuleEnabled(query, PlannerRuleNames.SORT_JOIN_COPY);
    //@formatter:off
    assertEquals(explain,
        "Execution Plan\n"
            + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[0]], "
            + "isSortOnSender=[false], isSortOnReceiver=[true])\n"
            + "    LogicalJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "      PinotLogicalExchange(distribution=[hash[0]])\n"
            + "        LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "          PinotLogicalSortExchange(distribution=[hash], collation=[[0]], "
            + "isSortOnSender=[false], isSortOnReceiver=[true])\n"
            + "            LogicalProject(col1=[$0])\n"
            + "              PinotLogicalTableScan(table=[[default, a]])\n"
            + "      PinotLogicalExchange(distribution=[hash[0]])\n"
            + "        LogicalProject(col1=[$0])\n"
            + "          PinotLogicalTableScan(table=[[default, b]])\n");
    //@formatter:on
  }

  @Test
  public void testAggregateUnionAggregateDisabledByDefault() {
    // Verify that the AggregateUnionAggregateRule is disabled by default
    //@formatter:off
    String query = "EXPLAIN PLAN FOR "
        + "SELECT * FROM "
        + "(SELECT DISTINCT col1 FROM a) "
        + "UNION "
        + "(SELECT DISTINCT col1 FROM b)";
    //@formatter:on

    String explain = _queryEnvironment.explainQuery(query, RANDOM_REQUEST_ID_GEN.nextLong());

    // The aggregates above the table scans should not be merged into the one above the UNION ALL
    assertEquals(explain,
        "Execution Plan\n"
            + "PinotLogicalAggregate(group=[{0}], aggType=[FINAL])\n"
            + "  PinotLogicalExchange(distribution=[hash[0]])\n"
            + "    PinotLogicalAggregate(group=[{0}], aggType=[LEAF])\n"
            + "      LogicalUnion(all=[true])\n"
            + "        PinotLogicalExchange(distribution=[hash[0]])\n"
            + "          PinotLogicalAggregate(group=[{0}], aggType=[FINAL])\n"
            + "            PinotLogicalExchange(distribution=[hash[0]])\n"
            + "              PinotLogicalAggregate(group=[{0}], aggType=[LEAF])\n"
            + "                PinotLogicalTableScan(table=[[default, a]])\n"
            + "        PinotLogicalExchange(distribution=[hash[0]])\n"
            + "          PinotLogicalAggregate(group=[{0}], aggType=[FINAL])\n"
            + "            PinotLogicalExchange(distribution=[hash[0]])\n"
            + "              PinotLogicalAggregate(group=[{0}], aggType=[LEAF])\n"
            + "                PinotLogicalTableScan(table=[[default, b]])\n");
  }

  @Test
  public void testAggregateUnionAggregateEnabled() {
    // Verify that the AggregateUnionAggregateRule is disabled by default
    //@formatter:off
    String query = "EXPLAIN PLAN FOR "
        + "SELECT * FROM "
        + "(SELECT DISTINCT col1 FROM a) "
        + "UNION "
        + "(SELECT DISTINCT col1 FROM b)";
    //@formatter:on

    String explain = explainQueryWithRuleEnabled(query, PlannerRuleNames.AGGREGATE_UNION_AGGREGATE);

    // There shouldn't be aggregates above the table scans since they should be merged into the one above the UNION ALL
    assertEquals(explain,
        "Execution Plan\n"
            + "PinotLogicalAggregate(group=[{0}], aggType=[FINAL])\n"
            + "  PinotLogicalExchange(distribution=[hash[0]])\n"
            + "    PinotLogicalAggregate(group=[{0}], aggType=[LEAF])\n"
            + "      LogicalUnion(all=[true])\n"
            + "        PinotLogicalExchange(distribution=[hash[0]])\n"
            + "          LogicalProject(col1=[$0])\n"
            + "            PinotLogicalTableScan(table=[[default, a]])\n"
            + "        PinotLogicalExchange(distribution=[hash[0]])\n"
            + "          LogicalProject(col1=[$0])\n"
            + "            PinotLogicalTableScan(table=[[default, b]])\n");
  }
}
