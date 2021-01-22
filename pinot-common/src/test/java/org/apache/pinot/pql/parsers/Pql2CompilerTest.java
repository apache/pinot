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
package org.apache.pinot.pql.parsers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.pql.parsers.pql2.ast.TopAstNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Some tests for the PQL 2 compiler.
 */
public class Pql2CompilerTest {
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final Logger LOGGER = LoggerFactory.getLogger(Pql2Compiler.class);

  @BeforeClass
  public void setUp() {
    Pql2Compiler.ENABLE_PINOT_QUERY = true;
    Pql2Compiler.VALIDATE_CONVERTER = true;
    Pql2Compiler.FAIL_ON_CONVERSION_ERROR = true;
  }

  @AfterClass
  public void tearDown() {
    Pql2Compiler.ENABLE_PINOT_QUERY = false;
    Pql2Compiler.VALIDATE_CONVERTER = false;
    Pql2Compiler.FAIL_ON_CONVERSION_ERROR = false;
  }

  @Test
  public void testQuotedStrings() {
    BrokerRequest brokerRequest =
        COMPILER.compileToBrokerRequest("select * from vegetables where origin = 'Martha''s Vineyard'");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha's Vineyard");
    // Test PinotQuery
    Assert.assertEquals(
        brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral()
            .getStringValue(), "Martha's Vineyard");

    brokerRequest = COMPILER.compileToBrokerRequest("select * from vegetables where origin = 'Martha\"\"s Vineyard'");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha\"\"s Vineyard");
    // Test PinotQuery
    Assert.assertEquals(
        brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral()
            .getStringValue(), "Martha\"\"s Vineyard");

    brokerRequest = COMPILER.compileToBrokerRequest("select * from vegetables where origin = \"Martha\"\"s Vineyard\"");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha\"s Vineyard");
    // Test PinotQuery
    Assert.assertEquals(
        brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral()
            .getStringValue(), "Martha\"s Vineyard");

    brokerRequest = COMPILER.compileToBrokerRequest("select * from vegetables where origin = \"Martha''s Vineyard\"");
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "Martha''s Vineyard");
    // Test PinotQuery
    Assert.assertEquals(
        brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral()
            .getStringValue(), "Martha''s Vineyard");
  }

  @Test
  public void testFilterCaluses() {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest("select * from vegetables where a > 1");
    Assert.assertEquals(brokerRequest.getFilterQuery().getColumn(), "a");
    Assert.assertEquals(brokerRequest.getFilterQuery().getOperator(), FilterOperator.RANGE);
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "(1\000*)");
    // Test PinotQuery
    Function func = brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.GREATER_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "a");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 1);

    brokerRequest = COMPILER.compileToBrokerRequest("select * from vegetables where b < 100");
    Assert.assertEquals(brokerRequest.getFilterQuery().getColumn(), "b");
    Assert.assertEquals(brokerRequest.getFilterQuery().getOperator(), FilterOperator.RANGE);
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "(*\000100)");
    // Test PinotQuery
    func = brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.LESS_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "b");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 100);

    brokerRequest = COMPILER.compileToBrokerRequest("select * from vegetables where c >= 10");
    Assert.assertEquals(brokerRequest.getFilterQuery().getColumn(), "c");
    Assert.assertEquals(brokerRequest.getFilterQuery().getOperator(), FilterOperator.RANGE);
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "[10\000*)");
    // Test PinotQuery
    func = brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.GREATER_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 10);

    brokerRequest = COMPILER.compileToBrokerRequest("select * from vegetables where d <= 50");
    Assert.assertEquals(brokerRequest.getFilterQuery().getColumn(), "d");
    Assert.assertEquals(brokerRequest.getFilterQuery().getOperator(), FilterOperator.RANGE);
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "(*\00050]");
    // Test PinotQuery
    func = brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.LESS_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "d");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 50);

    brokerRequest = COMPILER.compileToBrokerRequest("select * from vegetables where e BETWEEN 70 AND 80");
    Assert.assertEquals(brokerRequest.getFilterQuery().getColumn(), "e");
    Assert.assertEquals(brokerRequest.getFilterQuery().getOperator(), FilterOperator.RANGE);
    Assert.assertEquals(brokerRequest.getFilterQuery().getValue().get(0), "[70\00080]");
    // Test PinotQuery
    func = brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.BETWEEN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "e");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 70);
    Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 80);
  }

  @Test
  public void testDuplicateClauses() {
    assertCompilationFails("select top 5 count(*) from a top 8");
    assertCompilationFails("select count(*) from a where a = 1 limit 5 where b = 2");
    assertCompilationFails("select count(*) from a group by b limit 5 group by b");
    assertCompilationFails("select count(*) from a order by b limit 5 order by c");
    assertCompilationFails("select count(*) from a limit 5 limit 5");
  }

  @Test
  public void testTopZero() {
    testTopZeroFor("select COUNT(*) from someTable where c = 5 group by X top 0", TopAstNode.DEFAULT_TOP_N, false);
    testTopZeroFor("select count(*) from someTable where c = 5 group by X top 1", 1, false);
    testTopZeroFor("select count(*) from someTable where c = 5 group by X top -1", TopAstNode.DEFAULT_TOP_N, true);
  }

  private void assertCompilationFails(String query) {
    try {
      COMPILER.compileToBrokerRequest(query);
    } catch (Pql2CompilationException e) {
      // Expected
      return;
    }

    Assert.fail("Query " + query + " compiled successfully but was expected to fail compilation");
  }

  private void testTopZeroFor(String s, final int expectedTopN, boolean parseException) {
    BrokerRequest req;
    try {
      req = COMPILER.compileToBrokerRequest(s);
    } catch (Pql2CompilationException e) {
      if (parseException) {
        return;
      }
      throw e;
    }
    Assert.assertTrue(req.isSetGroupBy());
    GroupBy groupBy = req.getGroupBy();
    Assert.assertTrue(groupBy.isSetTopN());
    Assert.assertEquals(expectedTopN, groupBy.getTopN());

    // Test PinotQuery
    Assert.assertTrue(req.getPinotQuery().isSetGroupByList());
    Assert.assertTrue(req.getPinotQuery().isSetLimit());
    Assert.assertEquals(expectedTopN, req.getPinotQuery().getLimit());
  }

  @Test
  public void testGroupByTopLimitBehavior() {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest("select count(*) from myTable group by dimA top 200");
    Assert.assertEquals(brokerRequest.getGroupBy().getTopN(), 200);
    brokerRequest = COMPILER.compileToBrokerRequest("select count(*) from myTable group by dimA limit 300");
    Assert.assertEquals(brokerRequest.getGroupBy().getTopN(), 300);
    brokerRequest = COMPILER.compileToBrokerRequest("select count(*) from myTable group by dimA");
    Assert.assertEquals(brokerRequest.getGroupBy().getTopN(), 10);
    brokerRequest = COMPILER.compileToBrokerRequest("select count(*) from myTable group by dimA top 200 LIMIT 300");
    Assert.assertEquals(brokerRequest.getGroupBy().getTopN(), 200);
    brokerRequest = COMPILER.compileToBrokerRequest("select count(*) from myTable group by dimA LIMIT 0");
    Assert.assertEquals(brokerRequest.getGroupBy().getTopN(), 10);
  }

  @Test
  public void testGroupByOrderBy() {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(
        "select sum(rsvp_count), count(*) from meetupRsvp group by group_city order by sum(rsvp_count) DESC limit 200");
    Assert.assertEquals(brokerRequest.getGroupBy().getTopN(), 200);
    Assert.assertEquals(brokerRequest.getGroupBy().getExpressions().get(0), "group_city");
    Assert.assertEquals(brokerRequest.getPinotQuery().getGroupByList().get(0).getIdentifier().getName(), "group_city");
    Assert.assertEquals(brokerRequest.getOrderBy().get(0).getColumn(), "sum(rsvp_count)");
    Assert.assertEquals(
        brokerRequest.getPinotQuery().getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier()
            .getName(), "sum(rsvp_count)");
  }

  @Test
  public void testRejectInvalidLexerToken() {
    assertCompilationFails("select foo from bar where baz ?= 2");
    assertCompilationFails("select foo from bar where baz =! 2");
  }

  @Test
  public void testRejectInvalidParses() {
    assertCompilationFails("select foo from bar where baz < > 2");
    assertCompilationFails("select foo from bar where baz ! = 2");
  }

  @Test
  public void testParseExceptionHasCharacterPosition() {
    final String query = "select foo from bar where baz ? 2";

    try {
      COMPILER.compileToBrokerRequest(query);
    } catch (Pql2CompilationException e) {
      // Expected
      Assert.assertTrue(e.getMessage().startsWith("1:30: "),
          "Compilation exception should contain line and character for error message. Error message is " + e
              .getMessage());
      return;
    }

    Assert.fail("Query " + query + " compiled successfully but was expected to fail compilation");
  }

  @Test
  public void testCStyleInequalityOperator() {
    BrokerRequest brokerRequest =
        COMPILER.compileToBrokerRequest("select * from vegetables where name != 'Brussels sprouts'");
    Assert.assertEquals(brokerRequest.getFilterQuery().getOperator(), FilterOperator.NOT);
  }

  @Test
  public void testQueryOptions() {
    BrokerRequest brokerRequest =
        COMPILER.compileToBrokerRequest("select * from vegetables where name != 'Brussels sprouts'");
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 0);
    Assert.assertNull(brokerRequest.getQueryOptions());
    // Test PinotQuery
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptionsSize(), 0);
    Assert.assertNull(brokerRequest.getPinotQuery().getQueryOptions());

    brokerRequest = COMPILER
        .compileToBrokerRequest("select * from vegetables where name != 'Brussels sprouts' OPTION (delicious=yes)");
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 1);
    Assert.assertTrue(brokerRequest.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(brokerRequest.getQueryOptions().get("delicious"), "yes");
    // Test PinotQuery
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptionsSize(), 1);
    Assert.assertTrue(brokerRequest.getPinotQuery().getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptions().get("delicious"), "yes");

    brokerRequest = COMPILER.compileToBrokerRequest(
        "select * from vegetables where name != 'Brussels sprouts' OPTION (delicious=yes, foo=1234, bar='potato')");
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 3);
    Assert.assertTrue(brokerRequest.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(brokerRequest.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(brokerRequest.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(brokerRequest.getQueryOptions().get("bar"), "potato");
    // Test PinotQuery
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptionsSize(), 3);
    Assert.assertTrue(brokerRequest.getPinotQuery().getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptions().get("bar"), "potato");

    brokerRequest = COMPILER.compileToBrokerRequest(
        "select * from vegetables where name != 'Brussels sprouts' OPTION (delicious=yes) option(foo=1234) option(bar='potato')");
    Assert.assertEquals(brokerRequest.getQueryOptionsSize(), 3);
    Assert.assertTrue(brokerRequest.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(brokerRequest.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(brokerRequest.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(brokerRequest.getQueryOptions().get("bar"), "potato");
    // Test PinotQuery
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptionsSize(), 3);
    Assert.assertTrue(brokerRequest.getPinotQuery().getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(brokerRequest.getPinotQuery().getQueryOptions().get("bar"), "potato");
  }

  @Test
  public void testIdentifierQuoteCharacter() {
    TransformExpressionTree expTree = COMPILER.compileToExpressionTree("`a.b.c`");
    Assert.assertEquals(expTree.getExpressionType(), TransformExpressionTree.ExpressionType.IDENTIFIER);
    Assert.assertEquals(expTree.getValue(), "a.b.c");

    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(
        "select avg(`attributes.age`) as `avg_age` from `person` group by `attributes.address_city`");

    Assert.assertEquals(brokerRequest.getAggregationsInfo().get(0).getExpressions().get(0), "attributes.age");
    Assert.assertEquals(brokerRequest.getGroupBy().getExpressions(),
        Collections.singletonList("attributes.address_city"));

    // Test PinotQuery
    Assert.assertEquals(
        brokerRequest.getPinotQuery().getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier()
            .getName(), "attributes.age");
    Assert.assertEquals(brokerRequest.getPinotQuery().getGroupByList().get(0).getIdentifier().getName(),
        "attributes.address_city");
  }

  @Test
  public void testStringLiteral() {
    // Do not allow string literal column in selection query
    assertCompilationFails("SELECT 'foo' FROM table");

    // Allow string literal column in aggregation and group-by query
    BrokerRequest brokerRequest =
        COMPILER.compileToBrokerRequest("SELECT SUM('foo'), MAX(\"bar\") FROM table GROUP BY 'foo', \"bar\"");
    List<AggregationInfo> aggregationInfos = brokerRequest.getAggregationsInfo();
    Assert.assertEquals(aggregationInfos.size(), 2);
    Assert.assertEquals(aggregationInfos.get(0).getExpressions().get(0), "foo");
    Assert.assertEquals(aggregationInfos.get(1).getExpressions().get(0), "bar");
    List<String> expressions = brokerRequest.getGroupBy().getExpressions();
    Assert.assertEquals(expressions.size(), 2);
    Assert.assertEquals(expressions.get(0), "foo");
    Assert.assertEquals(expressions.get(1), "bar");

    // Test PinotQuery
    List<Expression> selectFunctionList = brokerRequest.getPinotQuery().getSelectList();
    Assert.assertEquals(selectFunctionList.size(), 2);
    Assert.assertEquals(selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getLiteral().getStringValue(),
        "foo");
    Assert.assertEquals(selectFunctionList.get(1).getFunctionCall().getOperands().get(0).getLiteral().getStringValue(),
        "bar");
    List<Expression> groupbyList = brokerRequest.getPinotQuery().getGroupByList();
    Assert.assertEquals(groupbyList.size(), 2);
    Assert.assertEquals(groupbyList.get(0).getLiteral().getStringValue(), "foo");
    Assert.assertEquals(groupbyList.get(1).getLiteral().getStringValue(), "bar");

    // For UDF, string literal won't be treated as column but as LITERAL
    brokerRequest =
        COMPILER.compileToBrokerRequest("SELECT SUM(ADD(foo, 'bar')) FROM table GROUP BY SUB(\"foo\", bar)");
    aggregationInfos = brokerRequest.getAggregationsInfo();
    Assert.assertEquals(aggregationInfos.size(), 1);
    Assert.assertEquals(aggregationInfos.get(0).getExpressions().get(0), "add(foo,'bar')");
    expressions = brokerRequest.getGroupBy().getExpressions();
    Assert.assertEquals(expressions.size(), 1);
    Assert.assertEquals(expressions.get(0), "sub('foo',bar)");

    // Test PinotQuery
    selectFunctionList = brokerRequest.getPinotQuery().getSelectList();
    Assert.assertEquals(selectFunctionList.size(), 1);
    Assert.assertEquals(selectFunctionList.get(0).getFunctionCall().getOperator(), "SUM");
    Assert.assertEquals(selectFunctionList.get(0).getFunctionCall().getOperands().size(), 1);

    Assert
        .assertEquals(selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
            "ADD");
    Assert.assertEquals(
        selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().size(), 2);
    Assert.assertEquals(
        selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "foo");
    Assert.assertEquals(
        selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getStringValue(), "bar");
    groupbyList = brokerRequest.getPinotQuery().getGroupByList();
    Assert.assertEquals(groupbyList.size(), 1);
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperator(), "SUB");
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperands().size(), 2);
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperands().get(0).getLiteral().getStringValue(), "foo");
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "bar");
  }

  @Test
  public void testConverter()
      throws IOException {
    COMPILER.compileToBrokerRequest("Select * from T where a IN (1,2,2,3,4)");

    COMPILER.compileToBrokerRequest("SELECT MIN(div(DaysSinceEpoch,2)) FROM mytable");
    COMPILER.compileToBrokerRequest(
        "SELECT SUM(DepDelayMinutes), SUM(ArrDel15), SUM(DepDelay), SUM(DepDel15) FROM myStarTable WHERE Carrier IN ('UA', 'WN', 'FL', 'F9') AND Carrier NOT IN ('EV', 'AS', 'FL') AND DayofMonth > 5 AND DayofMonth <= 17 AND Diverted > 0 AND OriginCityName > 'Detroit, MI' GROUP BY CRSDepTime");
    COMPILER.compileToBrokerRequest("Select * from T where a > 1 and a < 10");
    COMPILER.compileToBrokerRequest("Select * from T where a between 1 and 10");

    BrokerRequest brokerRequest =
        COMPILER.compileToBrokerRequest("select * from vegetables where name != 'Brussels sprouts'");
    Assert.assertEquals(brokerRequest.getPinotQuery().getFilterExpression().getFunctionCall().getOperator(),
        FilterKind.NOT_EQUALS.name());
    final BufferedReader br = new BufferedReader(
        new InputStreamReader(Pql2CompilerTest.class.getClassLoader().getResourceAsStream("pql_queries.list")));
    String pql;
    int seqId = 0;
    while ((pql = br.readLine()) != null) {
      try {
        LOGGER.info("Trying to compile PQL Id - {}, PQL: {}", seqId++, pql);
        COMPILER.compileToBrokerRequest(pql);
      } catch (Exception e) {
        LOGGER.error("Failed to compile pql {} to BrokerRequest.", pql, e);
        throw e;
      }
    }
  }

  /**
   * Unit test for order-by clause in PQL.
   */
  @Test
  public void testOrderBy() {
    testOrderBy("select * from table order by d2 asc, d3 desc", Arrays.asList("d2", "d3"), Arrays.asList(true, false));

    testOrderBy("select * from table order by d2, d3 desc", Arrays.asList("d2", "d3"), Arrays.asList(true, false));

    testOrderBy("select * from table order by d2 asc, d2 desc", Collections.singletonList("d2"),
        Collections.singletonList(true));

    testOrderBy("select * from table order by d2, d2 desc", Collections.singletonList("d2"),
        Collections.singletonList(true));

    testOrderBy("select * from table order by d2, add(d3, 1) desc, add(d3, 1)", Arrays.asList("d2", "add(d3,'1')"),
        Arrays.asList(true, false));

    testOrderBy("select sum(m1) from table group by d2, d3 order by d2 desc, d3", Arrays.asList("d2", "d3"),
        Arrays.asList(false, true));

    testOrderBy("select sum(m1) from table group by d2, d3 order by sum(m1), d3 desc", Arrays.asList("sum(m1)", "d3"),
        Arrays.asList(true, false));

    testOrderBy("select sum(m1), sum(m2) from table group by d1 order by sum(m1) desc, sum(m2) asc",
        Arrays.asList("sum(m1)", "sum(m2)"), Arrays.asList(false, true));

    testOrderBy(
        "select sum(m1), max(foo(bar(x, y), z)) from table group by d1 order by sum(m1) desc, max(foo(bar(x, y), z))",
        Arrays.asList("sum(m1)", "max(foo(bar(x,y),z))"), Arrays.asList(false, true));

    testOrderBy("select sum(m1) from table group by foo(bar(x, y), z) order by sum(m1) desc, foo(bar(x, y), z) asc",
        Arrays.asList("sum(m1)", "foo(bar(x,y),z)"), Arrays.asList(false, true));
  }

  /**
   * Utility method to compile a PQL and test expected values for order-by
   *
   * @param pql PQL to compile
   * @param orderBys Expected order-by's
   * @param isAscs Expected isAsc boolean values
   */
  private void testOrderBy(String pql, List<String> orderBys, List<Boolean> isAscs) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(pql);
    List<SelectionSort> orderByList = brokerRequest.getOrderBy();

    for (int i = 0; i < orderByList.size(); i++) {
      SelectionSort orderBy = orderByList.get(i);

      Assert.assertEquals(orderBy.getColumn(), orderBys.get(i));
      Assert.assertEquals(orderBy.isIsAsc(), isAscs.get(i).booleanValue());
    }
  }

  @Test
  public void testTextMatch() {
    String query = "SELECT text_col FROM foo WHERE TEXT_MATCH(text_col, '\"Foo bar\"')";
    BrokerRequest request = COMPILER.compileToBrokerRequest(query);
    FilterQuery filterQuery = request.getFilterQuery();
    Assert.assertEquals(filterQuery.getColumn(), "text_col");
    Assert.assertEquals(filterQuery.getValue().size(), 1);
    Assert.assertEquals(filterQuery.getValue().get(0), "\"Foo bar\"");
    Assert.assertEquals(filterQuery.getOperator(), FilterOperator.TEXT_MATCH);
  }
}
