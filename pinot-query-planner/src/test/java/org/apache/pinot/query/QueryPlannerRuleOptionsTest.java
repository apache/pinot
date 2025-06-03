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
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.Test;

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
    String explain = explainQueryWithRuleDisabled(query,
        CommonConstants.Broker.Request.QueryOptionKey.RuleOptionKey.SKIP_AGGREGATE_CASE_TO_FILTER_RULE);

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
        CommonConstants.Broker.Request.QueryOptionKey.RuleOptionKey.SKIP_PINOT_AGGREGATE_REDUCE_FUNCTIONS_RULE);
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
  public void testDisablePruneEmptyJoinLeft() {
    // Test that when skipPruneEmptyJoinLeft=true,
    String query = "EXPLAIN PLAN FOR SELECT *\n"
        + "FROM (\n"
        + "  SELECT * FROM a WHERE 1 = 0\n"
        + ") c\n"
        + "JOIN a ON c.col1 = a.col1 ;\n";

    String explain = explainQueryWithRuleDisabled(query,
        CommonConstants.Broker.Request.QueryOptionKey.RuleOptionKey.SKIP_PRUNE_EMPTY_JOIN_LEFT_RULE);
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
}
