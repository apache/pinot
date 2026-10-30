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

package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import org.apache.pinot.common.evaluator.GroovyFunctionEvaluator;
import org.apache.pinot.common.evaluator.GroovyStaticAnalyzerConfig;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.utils.rewriter.ParentAggregationResultRewriter;
import org.apache.pinot.core.query.utils.rewriter.ResultRewriterFactory;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.rewriter.ExprMinMaxRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


public class QueryValidationTest {

  @Test
  public void testLargeLimit() {
    String query = "SELECT * FROM testTable LIMIT 10000";
    testUnsupportedQuery(query, "Value for 'LIMIT' (10000) exceeds maximum allowed value of 1000");
  }

  @Test
  public void testNonExistingColumns() {
    String query = "SELECT DISTINCT(col1, col2) FROM foo";
    testNonExistingColumns("foo", false, Map.of("col1", "col1"), query,
        "Unknown columnName 'col2' found in the query");
    testNonExistingColumns("foo", false, Map.of("col2", "col2"), query,
        "Unknown columnName 'col1' found in the query");
    testExistingColumns("foo", false, Map.of("col2", "col2", "col1", "col1"), query);
    query = "SELECT sum(Col1) FROM foo";
    testNonExistingColumns("foo", false, Map.of("col1", "col1"), query,
        "Unknown columnName 'Col1' found in the query");
    testExistingColumns("foo", false, Map.of("Col1", "Col1"), query);
    testExistingColumns("foo", true, Map.of("col1", "col1"), query);
    testExistingColumns("foo", true, Map.of("col1", "Col1"), query);
    query = "SELECT sum(Col1) AS sum_col1 FROM foo";
    testNonExistingColumns("foo", false, Map.of("col1", "col1"), query,
        "Unknown columnName 'Col1' found in the query");
    testExistingColumns("foo", false, Map.of("Col1", "Col1"), query);
    testExistingColumns("foo", true, Map.of("col1", "col1"), query);
    testExistingColumns("foo", true, Map.of("col1", "Col1"), query);
    query = "SELECT sum(Col1) AS sum_col1 FROM foo HAVING sum_col1 > 10";
    testNonExistingColumns("foo", false, Map.of("col1", "col1"), query,
        "Unknown columnName 'Col1' found in the query");
    testNonExistingColumns("foo", false, Map.of("col1", "cOL1"), query,
        "Unknown columnName 'Col1' found in the query");
    testExistingColumns("foo", false, Map.of("Col1", "Col1"), query);
    testExistingColumns("foo", true, Map.of("col1", "col1"), query);
    testExistingColumns("foo", true, Map.of("col1", "Col1"), query);
    testExistingColumns("foo", true, Map.of("col1", "cOL1"), query);
    query = "SELECT sum(Col1) AS sum_col1, b AS B, c as D FROM foo GROUP BY B, D";
    testNonExistingColumns("foo", false, Map.of("col1", "col1", "b", "b", "c", "c"), query,
        "Unknown columnName 'Col1' found in the query");
    testNonExistingColumns("foo", false, Map.of("Col1", "Col1", "B", "B", "c", "c"), query,
        "Unknown columnName 'b' found in the query");
    testNonExistingColumns("foo", false, Map.of("Col1", "Col1", "c", "c"), query,
        "Unknown columnName 'b' found in the query");
    testNonExistingColumns("foo", false, Map.of("Col1", "Col1", "b", "b", "C", "C"), query,
        "Unknown columnName 'c' found in the query");
    testExistingColumns("foo", false, Map.of("Col1", "Col1", "b", "b", "c", "c"), query);
    testExistingColumns("foo", true, Map.of("col1", "col1", "b", "b", "c", "c"), query);
    testExistingColumns("foo", true, Map.of("col1", "COL1", "b", "B", "c", "C"), query);
    query = "SELECT sum(Col1) AS sum_col1, b AS B, c as D FROM foo GROUP BY 2, 3";
    testNonExistingColumns("foo", false, Map.of("col1", "col1", "B", "B", "c", "c", "D", "D"), query,
        "Unknown columnName 'Col1' found in the query");
    testNonExistingColumns("foo", false, Map.of("col1", "col1", "b", "b", "c", "c"), query,
        "Unknown columnName 'Col1' found in the query");
    testNonExistingColumns("foo", false, Map.of("Col1", "Col1", "B", "B", "c", "c"), query,
        "Unknown columnName 'b' found in the query");
    testNonExistingColumns("foo", false, Map.of("Col1", "Col1", "c", "c"), query,
        "Unknown columnName 'b' found in the query");
    testNonExistingColumns("foo", false, Map.of("Col1", "Col1", "b", "b", "C", "C"), query,
        "Unknown columnName 'c' found in the query");
    testExistingColumns("foo", false, Map.of("Col1", "Col1", "b", "b", "c", "c", "D", "D"), query);
    testExistingColumns("foo", true, Map.of("col1", "col1", "b", "b", "c", "c", "d", "d"), query);
    testExistingColumns("foo", true, Map.of("col1", "COL1", "b", "B", "c", "C"), query);
  }

  @Test
  public void testValidateGroovyQuery() {
    testValidateGroovyQuery(
        "SELECT groovy('{\"returnType\":\"INT\",\"isSingleValue\":true}', 'arg0 + arg1', colA, colB) FROM foo", true);
    testValidateGroovyQuery(
        "SELECT GROOVY('{\"returnType\":\"INT\",\"isSingleValue\":true}', 'arg0 + arg1', colA, colB) FROM foo", true);
    testValidateGroovyQuery(
        "SELECT groo_vy('{\"returnType\":\"INT\",\"isSingleValue\":true}', 'arg0 + arg1', colA, colB) FROM foo", true);
    testValidateGroovyQuery(
        "SELECT foo FROM bar WHERE GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', 'arg0 + arg1', colA,"
            + " colB) = 'foobarval'", true);
    testValidateGroovyQuery(
        "SELECT COUNT(colA) FROM bar GROUP BY GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', "
            + "'arg0 + arg1', colA, colB)", true);
    // HAVING imposes grouping semantics, so both the predicate and the SELECT list have to be aggregated.
    testValidateGroovyQuery(
        "SELECT COUNT(*) FROM bar HAVING GROOVY('{\"returnType\":\"STRING\",\"isSingleValue\":true}', "
            + "'arg0 + arg1', MAX(colA), MIN(colB)) = 'foobarval'", true);

    testValidateGroovyQuery("SELECT foo FROM bar", false);
  }

  @Test
  public void testQueryGroovyPolicyIsIndependentOfIngestion() {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(
        "SELECT groovy('{\"returnType\":\"INT\",\"isSingleValue\":true}', 'arg0 + 1', colA) FROM foo");
    GroovyFunctionEvaluator.setIngestionGroovyDisabled(true);
    try {
      BaseSingleStageBrokerRequestHandler.validateGroovyScript(query, false);
      assertEquals(expectThrows(BadQueryRequestException.class,
          () -> BaseSingleStageBrokerRequestHandler.validateGroovyScript(query, true)).getMessage(),
          "Groovy transform functions are disabled for queries");
    } finally {
      GroovyFunctionEvaluator.setIngestionGroovyDisabled(false);
    }
  }

  @Test
  public void testGroovyScripts()
      throws JsonProcessingException {
    // setup secure groovy config
    GroovyFunctionEvaluator.setGroovyStaticAnalyzerConfig(GroovyStaticAnalyzerConfig.createDefault());

    String inValidGroovyQuery = "SELECT groovy('{\"returnType\":\"INT\",\"isSingleValue\":true}') FROM foo";
    runUnsupportedGroovy(inValidGroovyQuery, "Groovy transform function must have at least 2 argument");

    String groovyInvalidMethodInvokeQuery =
        "SELECT groovy('{\"returnType\":\"STRING\",\"isSingleValue\":true}', 'return [\"bash\", \"-c\", \"echo Hello,"
            + " World!\"].execute().text') FROM foo";
    runUnsupportedGroovy(groovyInvalidMethodInvokeQuery, "Expression [MethodCallExpression] is not allowed");

    String groovyInvalidImportsQuery =
        "SELECT groovy( '{\"returnType\":\"INT\",\"isSingleValue\":true}', 'def args = [\"QuickStart\", \"-type\", "
            + "\"REALTIME\"] as String[]; org.apache.pinot.tools.admin.PinotAdministrator.main(args); 2') FROM foo";
    runUnsupportedGroovy(groovyInvalidImportsQuery, "Indirect import checks prevents usage of expression");

    String groovyInOrderByClause =
        "SELECT colA, colB FROM foo ORDER BY groovy('{\"returnType\":\"STRING\",\"isSingleValue\":true}', 'return "
            + "[\"bash\", \"-c\", \"echo Hello, World!\"].execute().text') DESC";
    runUnsupportedGroovy(groovyInOrderByClause, "Expression [MethodCallExpression] is not allowed");

    String groovyInHavingClause =
        "SELECT colA, SUM(colB) AS totalB, groovy('{\"returnType\":\"DOUBLE\",\"isSingleValue\":true}', 'arg0 / "
            + "arg1', SUM(colB), COUNT(*)) AS avgB FROM foo GROUP BY colA HAVING groovy('{\"returnType\":\"BOOLEAN\","
            + "\"isSingleValue\":true}', 'System.metaClass.methods.each { method -> if (method.name.md5() == "
            + "\"f24f62eeb789199b9b2e467df3b1876b\") {method.invoke(System, 10)} }', SUM(colB))";
    runUnsupportedGroovy(groovyInHavingClause, "Indirect import checks prevents usage of expression");

    String groovyInWhereClause =
        "SELECT colA, colB FROM foo WHERE groovy('{\"returnType\":\"BOOLEAN\",\"isSingleValue\":true}', 'System.exit"
            + "(10)', colA)";
    runUnsupportedGroovy(groovyInWhereClause, "Indirect import checks prevents usage of expression");

    // Reset groovy config for rest of the testing
    GroovyFunctionEvaluator.setGroovyStaticAnalyzerConfig(null);
  }

  @Test
  public void testReplicaGroupToQueryInvalidQuery() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("SET numReplicaGroupsToQuery='illegal'; SELECT COUNT(*) FROM MY_TABLE");
    Assert.assertThrows(IllegalArgumentException.class,
        () -> BaseSingleStageBrokerRequestHandler.validateRequest(pinotQuery, 10));
  }

  /// Restores the default query and result rewriters, which the EXPR_MIN/EXPR_MAX tests change.
  @AfterMethod
  public void resetRewriters() {
    QueryRewriterFactory.init(null);
    ResultRewriterFactory.init(null);
  }

  @Test
  public void testExprMinMaxWithoutRewriters() {
    testUnsupportedQuery("SELECT EXPR_MIN(a, b) FROM testTable",
        "EXPR_MIN is not enabled on this broker. To enable it, add '" + ExprMinMaxRewriter.class.getName()
            + "' to 'pinot.broker.query.rewriter.class.names' and '" + ParentAggregationResultRewriter.class.getName()
            + "' to 'pinot.broker.result.rewriter.class.names'");
  }

  @Test
  public void testExprMinMaxWithRewriters() {
    enableExprMinMaxQueryRewriter();
    ResultRewriterFactory.init(ParentAggregationResultRewriter.class.getName());
    validQuery("SELECT EXPR_MIN(a, b), EXPR_MAX(c, b, d), COUNT(*) FROM testTable");
    testUnsupportedExprMinMaxPosition("SELECT COUNT(*), EXPR_MIN(a, b) AS minA FROM testTable", "EXPR_MIN");
    testUnsupportedExprMinMaxPosition("SELECT EXPR_MAX(a, b) + 1 FROM testTable", "EXPR_MAX");
    testUnsupportedExprMinMaxPosition("SELECT COUNT(*) FROM testTable WHERE EXPR_MIN(a, b) > 1", "EXPR_MIN");
    testUnsupportedExprMinMaxPosition("SELECT c, COUNT(*) FROM testTable GROUP BY c HAVING EXPR_MAX(a, b) > 1",
        "EXPR_MAX");
    testUnsupportedExprMinMaxPosition("SELECT c, COUNT(*) FROM testTable GROUP BY c ORDER BY EXPR_MIN(a, b)",
        "EXPR_MIN");
  }

  @Test
  public void testExprMinMaxWithoutResultRewriter() {
    enableExprMinMaxQueryRewriter();
    testUnsupportedQuery("SELECT EXPR_MIN(a, b) FROM testTable",
        "EXPR_MIN and EXPR_MAX require the result rewriter '" + ParentAggregationResultRewriter.class.getName()
            + "'. To enable it, add it to 'pinot.broker.result.rewriter.class.names'");
  }

  private void testUnsupportedExprMinMaxPosition(String query, String functionName) {
    testUnsupportedQuery(query, functionName + " is only supported as a top-level expression in the select list, "
        + "without an alias. It is not supported in other expressions or in the WHERE, GROUP BY, HAVING or ORDER BY "
        + "clauses");
  }

  private static void enableExprMinMaxQueryRewriter() {
    QueryRewriterFactory.init(String.join(",", QueryRewriterFactory.DEFAULT_QUERY_REWRITERS_CLASS_NAMES) + ","
        + ExprMinMaxRewriter.class.getName());
  }

  private static void validQuery(String query) {
    BaseSingleStageBrokerRequestHandler.validateRequest(CalciteSqlParser.compileToPinotQuery(query), 1000);
  }

  private void testValidateGroovyQuery(String query, boolean queryContainsGroovy) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseSingleStageBrokerRequestHandler.validateGroovyScript(pinotQuery, queryContainsGroovy);
      if (queryContainsGroovy) {
        fail("Query should have failed since groovy was found in query: " + pinotQuery);
      }
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), "Groovy transform functions are disabled for queries");
    }
  }

  private static void runUnsupportedGroovy(String query, String errorMsg) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseSingleStageBrokerRequestHandler.validateGroovyScript(pinotQuery, false);
      fail("Query should have failed since malicious groovy was found in query");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(errorMsg));
    }
  }

  private void testUnsupportedQuery(String query, String errorMessage) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseSingleStageBrokerRequestHandler.validateRequest(pinotQuery, 1000);
      fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(e.getMessage(), errorMessage);
    }
  }

  private void testNonExistingColumns(String rawTableName, boolean isCaseInsensitive, Map<String, String> columnNameMap,
      String query, String errorMessage) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseSingleStageBrokerRequestHandler.updateColumnNames(rawTableName, pinotQuery, isCaseInsensitive, columnNameMap);
      fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  private void testExistingColumns(String rawTableName, boolean isCaseInsensitive, Map<String, String> columnNameMap,
      String query) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      BaseSingleStageBrokerRequestHandler.updateColumnNames(rawTableName, pinotQuery, isCaseInsensitive, columnNameMap);
    } catch (Exception e) {
      fail("Query should have succeeded");
    }
  }
}
