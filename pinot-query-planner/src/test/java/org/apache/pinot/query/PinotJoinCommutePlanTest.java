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
import java.util.Random;
import org.apache.calcite.sql.SqlNode;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker.PlannerRuleNames;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.PinotSqlType;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/// End-to-end planner-pipeline tests for {@link org.apache.pinot.calcite.rel.rules.PinotJoinCommuteRule}.
///
/// <p>Unlike {@code PinotJoinCommuteRuleTest}, which exercises the rule in isolation via a private
/// {@link org.apache.calcite.plan.hep.HepPlanner}, these tests run queries through the production
/// {@link QueryEnvironment}. They prove the rule is wired into {@code BASIC_RULES} and that it survives composition
/// with the rest of the logical-phase rules. They also verify the {@code skipPlannerRules} query option can disable
/// the rule end-to-end — which is the off-switch users will reach for if a deployment ever wants to opt out.
public class PinotJoinCommutePlanTest {

  private static final Random RANDOM_REQUEST_ID_GEN = new Random();
  private static final String DIM_TABLE = "dim_tbl_OFFLINE";
  private static final String FACT_TABLE = "fact_tbl_OFFLINE";

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    Schema dimSchema = new Schema.SchemaBuilder()
        .setSchemaName("dim_tbl")
        .addSingleValueDimension("dim_id", FieldSpec.DataType.INT)
        .addSingleValueDimension("dim_name", FieldSpec.DataType.STRING)
        .build();
    Schema factSchema = new Schema.SchemaBuilder()
        .setSchemaName("fact_tbl")
        .addSingleValueDimension("fact_id", FieldSpec.DataType.INT)
        .addSingleValueDimension("dim_id", FieldSpec.DataType.INT)
        .addMetric("metric", FieldSpec.DataType.INT)
        .build();

    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    factory.registerDimTable(dimSchema, DIM_TABLE);
    factory.registerTable(factSchema, FACT_TABLE);
    factory.registerSegment(1, DIM_TABLE, "dim_s1");
    factory.registerSegment(2, DIM_TABLE, "dim_s2");
    factory.registerSegment(1, FACT_TABLE, "fact_s1");
    factory.registerSegment(2, FACT_TABLE, "fact_s2");

    RoutingManager routingManager = factory.buildRoutingManager(null);
    TableCache tableCache = factory.buildTableCache();
    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);
    _queryEnvironment = new QueryEnvironment(CommonConstants.DEFAULT_DATABASE, tableCache, workerManager);
  }

  /// Verifies the rule actually fires through the production planner. In EXPLAIN order, the LEFT input of a join
  /// is printed before the RIGHT input. So a successful commute means {@code fact_tbl}'s table scan appears
  /// BEFORE {@code dim_tbl}'s table scan in the output even though the user wrote {@code dim JOIN fact}.
  @Test
  public void testDimOnLeftCommutesEndToEnd() {
    String query = "EXPLAIN PLAN FOR SELECT dim_tbl.dim_name, fact_tbl.metric "
        + "FROM dim_tbl JOIN fact_tbl ON dim_tbl.dim_id = fact_tbl.dim_id";
    String explain = _queryEnvironment.explainQuery(query, RANDOM_REQUEST_ID_GEN.nextLong());

    int factScanIdx = explain.indexOf("LogicalTableScan(table=[[default, fact_tbl]])");
    int dimScanIdx = explain.indexOf("LogicalTableScan(table=[[default, dim_tbl]])");
    assertTrue(factScanIdx >= 0, "EXPLAIN missing fact_tbl scan. Got:\n" + explain);
    assertTrue(dimScanIdx >= 0, "EXPLAIN missing dim_tbl scan. Got:\n" + explain);
    assertTrue(factScanIdx < dimScanIdx,
        "PinotJoinCommuteRule did not fire: fact_tbl scan should appear before dim_tbl scan in EXPLAIN "
            + "(left input is printed first; commute should put dim on the right). Got:\n" + explain);
  }

  /// Sanity check: when the user already wrote the join in the optimal {@code fact JOIN dim} form, the rule must
  /// NOT fire (right is already dim). {@code fact_tbl} should remain on the left in the EXPLAIN output.
  @Test
  public void testFactOnLeftDoesNotCommute() {
    String query = "EXPLAIN PLAN FOR SELECT dim_tbl.dim_name, fact_tbl.metric "
        + "FROM fact_tbl JOIN dim_tbl ON fact_tbl.dim_id = dim_tbl.dim_id";
    String explain = _queryEnvironment.explainQuery(query, RANDOM_REQUEST_ID_GEN.nextLong());

    int factScanIdx = explain.indexOf("LogicalTableScan(table=[[default, fact_tbl]])");
    int dimScanIdx = explain.indexOf("LogicalTableScan(table=[[default, dim_tbl]])");
    assertTrue(factScanIdx >= 0 && dimScanIdx >= 0);
    assertTrue(factScanIdx < dimScanIdx,
        "Rule should not commute fact-on-left form (dim is already on the right). Got:\n" + explain);
  }

  /// Verifies the {@code skipPlannerRules=PinotJoinCommuteRule} query option disables the rule end-to-end.
  /// With the rule disabled, the dim-on-left join must remain in its user-written form.
  @Test
  public void testSkipPlannerRulesDisablesCommute() {
    String query = "EXPLAIN PLAN FOR SELECT dim_tbl.dim_name, fact_tbl.metric "
        + "FROM dim_tbl JOIN fact_tbl ON dim_tbl.dim_id = fact_tbl.dim_id";
    String explain = explainWithRuleSkipped(query, PlannerRuleNames.PINOT_JOIN_COMMUTE);

    int factScanIdx = explain.indexOf("LogicalTableScan(table=[[default, fact_tbl]])");
    int dimScanIdx = explain.indexOf("LogicalTableScan(table=[[default, dim_tbl]])");
    assertTrue(factScanIdx >= 0 && dimScanIdx >= 0);
    assertTrue(dimScanIdx < factScanIdx,
        "With PinotJoinCommuteRule skipped, dim_tbl should remain on the left (user-written form). Got:\n"
            + explain);
  }

  private String explainWithRuleSkipped(String query, String ruleToSkip) {
    SqlNode sqlNode = CalciteSqlParser.compileToSqlNodeAndOptions(query).getSqlNode();
    Map<String, String> options = new HashMap<>();
    options.put(CommonConstants.Broker.Request.QueryOptionKey.SKIP_PLANNER_RULES, ruleToSkip);
    SqlNodeAndOptions sqlNodeAndOptions = new SqlNodeAndOptions(
        sqlNode, PinotSqlType.DQL, QueryOptionsUtils.resolveCaseInsensitiveOptions(options));
    return _queryEnvironment
        .compile(query, sqlNodeAndOptions)
        .explain(RANDOM_REQUEST_ID_GEN.nextLong(), null)
        .getExplainPlan();
  }
}
