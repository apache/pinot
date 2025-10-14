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

import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link PinotProjectFilterTransposeRule}.
 */
public class PinotProjectFilterTransposeRuleTest extends QueryEnvironmentTestBase {

  private String explainQueryWithRule(String query) {
    return _queryEnvironment.explainQuery(query, RANDOM_REQUEST_ID_GEN.nextLong());
  }

  @Test
  public void testBasicCommonExpressionOptimization() {
    String query = "EXPLAIN PLAN FOR SELECT col3+col3 FROM a WHERE col3+col3 > 59";
    String explainPlan = explainQueryWithRule(query);

    assertTrue(explainPlan.contains("commonExpr"), "Should create commonExpr optimization");
    assertTrue(explainPlan.contains("LogicalProject(EXPR$0=[$0])"), "Should use pre-computed value");
    assertTrue(explainPlan.contains("LogicalFilter(condition=[>($0, 59)])"), "Filter should use pre-computed value");
    assertTrue(explainPlan.contains("commonExpr0=[+($2, $2)]"), "Should compute expression once");
  }

  @Test
  public void testMultipleCommonExpressions() {
    String query =
        "EXPLAIN PLAN FOR SELECT col3+col6, col3*col6, col3+col6+col3 FROM a WHERE col3+col6 > 10 AND col3*col6 < 100"
            + " OR col3+col6+col3>20";
    String explainPlan = explainQueryWithRule(query);

    assertTrue(explainPlan.contains("commonExpr"), "Should create commonExpr optimization");
    assertTrue(explainPlan.contains("commonExpr0") && explainPlan.contains("commonExpr1") && explainPlan.contains(
        "commonExpr2"), "Should optimize both expressions");
  }

  @Test
  public void testOrConditionsWithCommonExpressions() {
    String query = "EXPLAIN PLAN FOR SELECT col3+col6 FROM a WHERE col3+col6 > 100 OR col3+col6 < 10";
    String explainPlan = explainQueryWithRule(query);

    assertTrue(explainPlan.contains("commonExpr"), "Should optimize common expressions in OR conditions");
  }

  @Test
  public void testNoOptimizationWhenNoCommonExpression() {
    String query = "EXPLAIN PLAN FOR SELECT col3 FROM a WHERE col6 > 50";
    String explainPlan = explainQueryWithRule(query);

    assertFalse(explainPlan.contains("commonExpr"), "Should not create commonExpr when no common expressions exist");
  }

  @Test
  public void testCorrectPlanStructure() {
    String query = "EXPLAIN PLAN FOR SELECT col3+col3 FROM a WHERE col3+col3 > 59";
    String explainPlan = explainQueryWithRule(query);

    assertTrue(explainPlan.contains("commonExpr"), "Should create commonExpr optimization");
    assertTrue(explainPlan.contains("LogicalProject(EXPR$0=[$0])"), "Should have top project");
    assertTrue(explainPlan.contains("LogicalFilter(condition=[>($0, 59)])"), "Should have filter");
    assertTrue(explainPlan.contains("commonExpr0=[+($2, $2)]"), "Should have intermediate project");
    assertTrue(explainPlan.contains("PinotLogicalTableScan"), "Should have table scan");

    String[] lines = explainPlan.split("\n");
    boolean foundTopProject = false, foundFilter = false, foundBottomProject = false;

    for (String line : lines) {
      if (line.contains("LogicalProject(EXPR$0=[$0])")) {
        foundTopProject = true;
      } else if (line.contains("LogicalFilter(condition=[>($0, 59)])")) {
        assertTrue(foundTopProject, "Filter should come after top project");
        foundFilter = true;
      } else if (line.contains("commonExpr0=[+($2, $2)]")) {
        assertTrue(foundFilter, "Bottom project should come after filter");
        foundBottomProject = true;
      }
    }

    assertTrue(foundTopProject && foundFilter && foundBottomProject, "Should have correct plan structure");
  }

  @Test
  public void testJoinWithCommonExpressions() {
    String query = "EXPLAIN PLAN FOR SELECT a.col3+a.col6 FROM a JOIN b ON a.col1 = b.col1 WHERE a.col3+a.col6 > 100";
    String explainPlan = explainQueryWithRule(query);

    assertTrue(explainPlan.contains("commonExpr"), "Should optimize common expressions in JOIN");
  }

  @Test
  public void testAggregateWithCommonExpressions() {
    String query = "EXPLAIN PLAN FOR SELECT SUM(col3+col6) FROM a WHERE col3+col6 > 50 GROUP BY col1";
    String explainPlan = explainQueryWithRule(query);

    assertTrue(explainPlan.contains("commonExpr"), "Should optimize common expressions with aggregates");
  }
}