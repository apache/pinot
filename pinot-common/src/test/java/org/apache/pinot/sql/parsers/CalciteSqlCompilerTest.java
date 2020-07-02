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
package org.apache.pinot.sql.parsers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.pql.parsers.PinotQuery2BrokerRequestConverter;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Some tests for the SQL compiler.
 */
public class CalciteSqlCompilerTest {

  @Test
  public void testQuotedStrings() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where origin = 'Martha''s Vineyard'");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "Martha''s Vineyard");

    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where origin = 'Martha\"\"s Vineyard'");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "Martha\"\"s Vineyard");

    pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where origin = \"Martha\"\"s Vineyard\"");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "Martha\"s Vineyard");

    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where origin = \"Martha''s Vineyard\"");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "Martha''s Vineyard");
  }

  @Test
  public void testFilterCaluses() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where a > 1.5");
    Function func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.GREATER_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "a");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getDoubleValue(), 1.5);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where b < 100");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LESS_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "b");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 100L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where c >= 10");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.GREATER_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 10L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where d <= 50");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LESS_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "d");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 50L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where e BETWEEN 70 AND 80");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.BETWEEN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "e");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 70L);
    Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 80L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where regexp_like(E, '^U.*')");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), "regexp_like");
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "E");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getStringValue(), "^U.*");
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where f LIKE '%potato%'");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LIKE.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "f");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getStringValue(), "%potato%");
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where g IN (12, 13, 15.2, 17)");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.IN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "g");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 12L);
    Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 13L);
    Assert.assertEquals(func.getOperands().get(3).getLiteral().getDoubleValue(), 15.2);
    Assert.assertEquals(func.getOperands().get(4).getLiteral().getLongValue(), 17L);
  }

  @Test
  public void testFilterCalusesWithRightExpression() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where a > b");
    Function func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.GREATER_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "MINUS");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "a");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "b");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 0L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where 0 < a-b");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.GREATER_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "MINUS");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "a");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "b");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 0L);

    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where b < 100 + c");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LESS_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "MINUS");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "b");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(),
            "PLUS");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getLiteral().getLongValue(), 100L);
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 0L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where b -(100+c)< 0");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LESS_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "MINUS");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "b");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(),
            "PLUS");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getLiteral().getLongValue(), 100L);
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 0L);

    pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where foo1(bar1(a-b)) <= foo2(bar2(c+d))");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LESS_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "MINUS");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
            "foo1");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(),
            "foo2");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "bar1");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "bar2");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "MINUS");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "PLUS");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "a");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "b");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "c");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "d");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 0L);
    pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where foo1(bar1(a-b)) - foo2(bar2(c+d)) <= 0");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.LESS_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "MINUS");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
            "foo1");
    Assert
        .assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(),
            "foo2");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "bar1");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "bar2");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "MINUS");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "PLUS");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "a");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "b");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "c");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "d");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 0L);

    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where c >= 10");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.GREATER_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 10L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where 10 <= c");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.GREATER_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 10L);
  }

  @Test
  public void testBrokerConverter() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where g IN (12, 13, 15.2, 17)");
    Function func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.IN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "g");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 12L);
    Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 13L);
    Assert.assertEquals(func.getOperands().get(3).getLiteral().getDoubleValue(), 15.2);
    Assert.assertEquals(func.getOperands().get(4).getLiteral().getLongValue(), 17L);
    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    BrokerRequest tempBrokerRequest = converter.convert(pinotQuery);
    Assert.assertEquals(tempBrokerRequest.getQuerySource().getTableName(), "vegetables");
    Assert.assertEquals(tempBrokerRequest.getSelections().getSelectionColumns().get(0), "*");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getOperator(), FilterOperator.IN);
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getColumn(), "g");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().size(), 4);
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(0), "12");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(1), "13");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(2), "15.2");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(3), "17");
  }

  @Test
  public void testBrokerConverterWithLiteral() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select now() from mytable");
    Literal literal = pinotQuery.getSelectList().get(0).getLiteral();
    Assert.assertNotNull(literal);
    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    BrokerRequest tempBrokerRequest = converter.convert(pinotQuery);
    Assert.assertEquals(tempBrokerRequest.getQuerySource().getTableName(), "mytable");
    Assert.assertEquals(tempBrokerRequest.getSelections().getSelectionColumns().get(0),
        literal.getFieldValue().toString());
  }

  @Test
  public void testSelectAs() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select sum(A) as sum_A, count(B) as count_B  from vegetables where g IN (12, 13, 15.2, 17)");
    Function func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), SqlKind.IN.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "g");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 12L);
    Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 13L);
    Assert.assertEquals(func.getOperands().get(3).getLiteral().getDoubleValue(), 15.2);
    Assert.assertEquals(func.getOperands().get(4).getLiteral().getLongValue(), 17L);
    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    BrokerRequest tempBrokerRequest = converter.convert(pinotQuery);
    Assert.assertEquals(tempBrokerRequest.getQuerySource().getTableName(), "vegetables");
    Assert.assertNull(tempBrokerRequest.getSelections());
    Assert.assertEquals(tempBrokerRequest.getAggregationsInfo().get(0).getAggregationType(), "SUM");
    Assert.assertEquals(tempBrokerRequest.getAggregationsInfo().get(1).getAggregationType(), "COUNT");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getColumn(), "g");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().size(), 4);
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(0), "12");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(1), "13");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(2), "15.2");
    Assert.assertEquals(tempBrokerRequest.getFilterQuery().getValue().get(3), "17");
  }

  @Test
  public void testDuplicateClauses() {
    assertCompilationFails("select top 5 count(*) from a top 8");
    assertCompilationFails("select count(*) from a where a = 1 limit 5 where b = 2");
    assertCompilationFails("select count(*) from a group by b limit 5 group by b");
    assertCompilationFails("select count(*) from a having sum(a) = 8 limit 5 having sum(a) = 9");
    assertCompilationFails("select count(*) from a order by b limit 5 order by c");
    assertCompilationFails("select count(*) from a limit 5 limit 5");
  }

  @Test
  public void testTopZero() {
    testTopZeroFor("select count(*) from someTable where c = 5 group by X ORDER BY $1 LIMIT 100", 100, false);
    testTopZeroFor("select count(*) from someTable where c = 5 group by X ORDER BY $1 LIMIT 0", 0, false);
    testTopZeroFor("select count(*) from someTable where c = 5 group by X ORDER BY $1 LIMIT 1", 1, false);
    testTopZeroFor("select count(*) from someTable where c = 5 group by X ORDER BY $1 LIMIT -1", -1, true);
  }

  @Test
  public void testLimitOffsets() {
    PinotQuery pinotQuery;
    try {
      pinotQuery =
          CalciteSqlParser.compileToPinotQuery("select a, b, c from meetupRsvp order by a, b, c limit 100 offset 200");
    } catch (SqlCompilationException e) {
      throw e;
    }
    // Test PinotQuery
    Assert.assertTrue(pinotQuery.isSetLimit());
    Assert.assertEquals(100, pinotQuery.getLimit());
    Assert.assertTrue(pinotQuery.isSetOffset());
    Assert.assertEquals(200, pinotQuery.getOffset());

    try {
      pinotQuery =
          CalciteSqlParser.compileToPinotQuery("select a, b, c from meetupRsvp order by a, b, c limit 200,100");
    } catch (SqlCompilationException e) {
      throw e;
    }
    // Test PinotQuery
    Assert.assertTrue(pinotQuery.isSetLimit());
    Assert.assertEquals(100, pinotQuery.getLimit());
    Assert.assertTrue(pinotQuery.isSetOffset());
    Assert.assertEquals(200, pinotQuery.getOffset());
  }

  @Test
  public void testGroupbys() {
    PinotQuery pinotQuery;
    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(
          "select sum(rsvp_count), count(*), group_city from meetupRsvp group by group_city order by sum(rsvp_count) limit 10");
    } catch (SqlCompilationException e) {
      throw e;
    }
    // Test PinotQuery
    Assert.assertTrue(pinotQuery.isSetGroupByList());
    Assert.assertTrue(pinotQuery.isSetLimit());
    Assert.assertTrue(pinotQuery.isSetOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getType(), ExpressionType.FUNCTION);
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "SUM");
    Assert.assertEquals(10, pinotQuery.getLimit());

    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(
          "select sum(rsvp_count), count(*) from meetupRsvp group by group_city order by sum(rsvp_count) limit 10");
    } catch (SqlCompilationException e) {
      throw e;
    }
    // Test PinotQuery
    Assert.assertTrue(pinotQuery.isSetGroupByList());
    Assert.assertTrue(pinotQuery.isSetLimit());
    Assert.assertTrue(pinotQuery.isSetOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getType(), ExpressionType.FUNCTION);
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "SUM");
    Assert.assertEquals(10, pinotQuery.getLimit());

    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(
          "select group_city, sum(rsvp_count), count(*) from meetupRsvp group by group_city order by sum(rsvp_count), count(*) limit 10");
    } catch (SqlCompilationException e) {
      throw e;
    }
    // Test PinotQuery
    Assert.assertTrue(pinotQuery.isSetGroupByList());
    Assert.assertTrue(pinotQuery.isSetLimit());
    Assert.assertTrue(pinotQuery.isSetOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getType(), ExpressionType.FUNCTION);
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "SUM");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "rsvp_count");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "COUNT");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "*");
    Assert.assertEquals(10, pinotQuery.getLimit());
  }

  private void assertCompilationFails(String query) {
    try {
      CalciteSqlParser.compileToPinotQuery(query);
    } catch (SqlCompilationException e) {
      // Expected
      return;
    }

    Assert.fail("Query " + query + " compiled successfully but was expected to fail compilation");
  }

  private void testTopZeroFor(String s, final int expectedTopN, boolean parseException) {
    PinotQuery pinotQuery;
    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(s);
    } catch (SqlCompilationException e) {
      if (parseException) {
        return;
      }
      throw e;
    }

    // Test PinotQuery
    Assert.assertTrue(pinotQuery.isSetGroupByList());
    Assert.assertTrue(pinotQuery.isSetLimit());
    Assert.assertEquals(expectedTopN, pinotQuery.getLimit());
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
      CalciteSqlParser.compileToPinotQuery(query);
    } catch (SqlCompilationException e) {
      // Expected
      Assert.assertTrue(e.getMessage().contains("at line 1, column 31."),
          "Compilation exception should contain line and character for error message. Error message is " + e
              .getMessage());
      return;
    }

    Assert.fail("Query " + query + " compiled successfully but was expected to fail compilation");
  }

  @Test
  public void testCStyleInequalityOperator() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where name <> 'Brussels sprouts'");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "NOT_EQUALS");

    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where name != 'Brussels sprouts'");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "NOT_EQUALS");
  }

  @Test
  public void testQueryOptions() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where name <> 'Brussels sprouts'");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 0);
    Assert.assertNull(pinotQuery.getQueryOptions());

    pinotQuery = CalciteSqlParser
        .compileToPinotQuery("select * from vegetables where name <> 'Brussels sprouts' OPTION (delicious=yes)");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 1);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select * from vegetables where name <> 'Brussels sprouts' OPTION (delicious=yes, foo=1234, bar='potato')");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 3);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("bar"), "'potato'");

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select * from vegetables where name <> 'Brussels sprouts' OPTION (delicious=yes) option(foo=1234) option(bar='potato')");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 3);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("bar"), "'potato'");
  }

  @Test
  public void testIdentifierQuoteCharacter() {
    PinotQuery pinotQuery = CalciteSqlParser
        .compileToPinotQuery("select avg(attributes.age) as avg_age from person group by attributes.address_city");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "attributes.age");
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getIdentifier().getName(), "attributes.address_city");
  }

  @Test
  public void testStringLiteral() {
    // Do not allow string literal column in selection query
    assertCompilationFails("SELECT 'foo' FROM table");

    // Allow string literal column in aggregation and group-by query
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("SELECT SUM('foo'), MAX(bar) FROM myTable GROUP BY 'foo', bar");
    List<Expression> selectFunctionList = pinotQuery.getSelectList();
    Assert.assertEquals(selectFunctionList.size(), 2);
    Assert.assertEquals(selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getLiteral().getStringValue(),
        "foo");
    Assert.assertEquals(selectFunctionList.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "bar");
    List<Expression> groupbyList = pinotQuery.getGroupByList();
    Assert.assertEquals(groupbyList.size(), 2);
    Assert.assertEquals(groupbyList.get(0).getLiteral().getStringValue(), "foo");
    Assert.assertEquals(groupbyList.get(1).getIdentifier().getName(), "bar");

    // For UDF, string literal won't be treated as column but as LITERAL
    pinotQuery = CalciteSqlParser
        .compileToPinotQuery("SELECT SUM(ADD(foo, 'bar')) FROM myTable GROUP BY sub(foo, bar), SUB(BAR, FOO)");
    selectFunctionList = pinotQuery.getSelectList();
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
    groupbyList = pinotQuery.getGroupByList();
    Assert.assertEquals(groupbyList.size(), 2);
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperator(), "sub");
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperands().size(), 2);
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "foo");
    Assert.assertEquals(groupbyList.get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "bar");

    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperator(), "SUB");
    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperands().size(), 2);
    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "BAR");
    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "FOO");
  }

  @Test
  public void testPqlAndSqlCompatible()
      throws IOException {
    try (BufferedReader sqlReader = new BufferedReader(
        new InputStreamReader(CalciteSqlCompilerTest.class.getClassLoader().getResourceAsStream("sql_queries.list")));
        BufferedReader pqlReader = new BufferedReader(new InputStreamReader(
            CalciteSqlCompilerTest.class.getClassLoader().getResourceAsStream("pql_queries.list")))) {
      PinotQuery2BrokerRequestConverter pinotQuery2BrokerRequestConverter = new PinotQuery2BrokerRequestConverter();
      Pql2Compiler pqlCompiler = new Pql2Compiler();
      String sql;
      while ((sql = sqlReader.readLine()) != null) {
        // Compilation should not throw exception
        PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
        BrokerRequest sqlBrokerRequest = pinotQuery2BrokerRequestConverter.convert(pinotQuery);

        BrokerRequest pqlBrokerRequest = pqlCompiler.compileToBrokerRequest(pqlReader.readLine());
        // TODO: compare SQL/PQL broker request
      }
    }
  }

  @Test
  public void testFilterUdf() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select count(*) from baseballStats where DIV(numberOfGames,10) = 100");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "COUNT");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "DIV");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "numberOfGames");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getLongValue(), 10);
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 100);

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT count(*) FROM mytable WHERE timeConvert(DaysSinceEpoch,'DAYS','SECONDS') = 1394323200");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "COUNT");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "timeConvert");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "DaysSinceEpoch");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getStringValue(), "DAYS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(2)
            .getLiteral().getStringValue(), "SECONDS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getLongValue(),
        1394323200);
  }

  @Test
  public void testSelectionTransformFunction() {
    PinotQuery pinotQuery = CalciteSqlParser
        .compileToPinotQuery("  select mapKey(mapField,k1) from baseballStats where mapKey(mapField,k1) = 'v1'");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "mapKey");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "mapField");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "k1");

    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "mapKey");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "mapField");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "k1");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "v1");
  }

  @Test
  public void testSqlDistinctQueryCompilation() {
    // test single column DISTINCT
    String sql = "SELECT DISTINCT c1 FROM foo";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    List<Expression> selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    Function distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 1);

    Identifier c1 = distinctFunction.getOperands().get(0).getIdentifier();
    Assert.assertEquals(c1.getName(), "c1");

    PinotQuery2BrokerRequestConverter converter = new PinotQuery2BrokerRequestConverter();
    BrokerRequest brokerRequest = converter.convert(pinotQuery);
    List<AggregationInfo> aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    AggregationInfo aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getExpressions().get(0), "c1");

    // test multi column DISTINCT
    sql = "SELECT DISTINCT c1, c2 FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 2);

    c1 = distinctFunction.getOperands().get(0).getIdentifier();
    Identifier c2 = distinctFunction.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "c1");
    Assert.assertEquals(c2.getName(), "c2");

    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getExpressions(), Arrays.asList("c1", "c2"));

    // test multi column DISTINCT with filter
    sql = "SELECT DISTINCT c1, c2, c3 FROM foo WHERE c3 > 100";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);

    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 3);

    final Expression filter = pinotQuery.getFilterExpression();
    Assert.assertEquals(filter.getFunctionCall().getOperator(), "GREATER_THAN");
    Assert.assertEquals(filter.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "c3");
    Assert.assertEquals(filter.getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 100);

    c1 = distinctFunction.getOperands().get(0).getIdentifier();
    c2 = distinctFunction.getOperands().get(1).getIdentifier();
    Identifier c3 = distinctFunction.getOperands().get(2).getIdentifier();
    Assert.assertEquals(c1.getName(), "c1");
    Assert.assertEquals(c2.getName(), "c2");
    Assert.assertEquals(c3.getName(), "c3");

    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getExpressions(), Arrays.asList("c1", "c2", "c3"));

    // not supported by Calcite SQL (this is in compliance with SQL standard)
    try {
      sql = "SELECT sum(c1), DISTINCT c2 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getCause().getMessage().contains("Encountered \", DISTINCT\" at line 1, column 15."));
    }

    // not supported by Calcite SQL (this is in compliance with SQL standard)
    try {
      sql = "SELECT c1, DISTINCT c2 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getCause().getMessage().contains("Encountered \", DISTINCT\" at line 1, column 10."));
    }

    // not supported by Calcite SQL (this is in compliance with SQL standard)
    try {
      sql = "SELECT DIV(c1,c2), DISTINCT c3 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getCause().getMessage().contains("Encountered \", DISTINCT\" at line 1, column 18."));
    }

    // The following query although a valid SQL syntax is not
    // very helpful since the result will be one row -- probably a
    // a single random value from c1 and sum of c2.
    // we can support this if underlying engine
    // implements sum as a transform function which is not the case today
    // this is a multi column distinct so we cant treat this query
    // as having 2 independent functions -- sum(c2) should be an input
    // into distinct and that can't happen unless it is implemented as a
    // transform
    try {
      sql = "SELECT DISTINCT c1, sum(c2) FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(
          e.getMessage().contains("Syntax error: Use of DISTINCT with aggregation functions is not supported"));
    }

    // same reason as above
    try {
      sql = "SELECT DISTINCT sum(c1) FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(
          e.getMessage().contains("Syntax error: Use of DISTINCT with aggregation functions is not supported"));
    }

    // Pinot currently does not support DISTINCT * syntax
    try {
      sql = "SELECT DISTINCT * FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains(
          "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name after DISTINCT keyword"));
    }

    // Pinot currently does not support DISTINCT * syntax
    try {
      sql = "SELECT DISTINCT *, C1 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains(
          "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name after DISTINCT keyword"));
    }

    // Pinot currently does not support GROUP BY with DISTINCT
    try {
      sql = "SELECT DISTINCT C1, C2 FROM foo GROUP BY C1";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains("DISTINCT with GROUP BY is not supported"));
    }

    // distinct with transform is supported since the output of
    // transform can be piped into distinct function

    // test DISTINCT with single transform function
    sql = "SELECT DISTINCT add(col1,col2) FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 1);

    Function add = distinctFunction.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(add.getOperator(), "add");
    Assert.assertEquals(add.getOperands().size(), 2);
    c1 = add.getOperands().get(0).getIdentifier();
    c2 = add.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col1");
    Assert.assertEquals(c2.getName(), "col2");

    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getExpressions().get(0), "add(col1,col2)");

    // multi-column distinct with multiple transform functions
    sql = "SELECT DISTINCT add(div(col1, col2), mul(col3, col4)), sub(col3, col4) FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 2);

    // check for DISTINCT's first operand ADD(....)
    add = distinctFunction.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(add.getOperator(), "add");
    Assert.assertEquals(add.getOperands().size(), 2);
    Function div = add.getOperands().get(0).getFunctionCall();
    Function mul = add.getOperands().get(1).getFunctionCall();

    // check for ADD's first operand DIV(...)
    Assert.assertEquals(div.getOperator(), "div");
    Assert.assertEquals(div.getOperands().size(), 2);
    c1 = div.getOperands().get(0).getIdentifier();
    c2 = div.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col1");
    Assert.assertEquals(c2.getName(), "col2");

    // check for ADD's second operand MUL(...)
    Assert.assertEquals(mul.getOperator(), "mul");
    Assert.assertEquals(mul.getOperands().size(), 2);
    c1 = mul.getOperands().get(0).getIdentifier();
    c2 = mul.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col3");
    Assert.assertEquals(c2.getName(), "col4");

    // check for DISTINCT's second operand SUB(...)
    Function sub = distinctFunction.getOperands().get(1).getFunctionCall();
    Assert.assertEquals(sub.getOperator(), "sub");
    Assert.assertEquals(sub.getOperands().size(), 2);
    c1 = sub.getOperands().get(0).getIdentifier();
    c2 = sub.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col3");
    Assert.assertEquals(c2.getName(), "col4");

    // check conversion to broker request
    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getExpressions(),
        Arrays.asList("add(div(col1,col2),mul(col3,col4))", "sub(col3,col4)"));

    // multi-column distinct with multiple transform columns and additional identifiers
    sql = "SELECT DISTINCT add(div(col1, col2), mul(col3, col4)), sub(col3, col4), col5, col6 FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(distinctFunction.getOperands().size(), 4);

    // check for DISTINCT's first operand ADD(....)
    add = distinctFunction.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(add.getOperator(), "add");
    Assert.assertEquals(add.getOperands().size(), 2);
    div = add.getOperands().get(0).getFunctionCall();
    mul = add.getOperands().get(1).getFunctionCall();

    // check for ADD's first operand DIV(...)
    Assert.assertEquals(div.getOperator(), "div");
    Assert.assertEquals(div.getOperands().size(), 2);
    c1 = div.getOperands().get(0).getIdentifier();
    c2 = div.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col1");
    Assert.assertEquals(c2.getName(), "col2");

    // check for ADD's second operand MUL(...)
    Assert.assertEquals(mul.getOperator(), "mul");
    Assert.assertEquals(mul.getOperands().size(), 2);
    c1 = mul.getOperands().get(0).getIdentifier();
    c2 = mul.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col3");
    Assert.assertEquals(c2.getName(), "col4");

    // check for DISTINCT's second operand SUB(...)
    sub = distinctFunction.getOperands().get(1).getFunctionCall();
    Assert.assertEquals(sub.getOperator(), "sub");
    Assert.assertEquals(sub.getOperands().size(), 2);
    c1 = sub.getOperands().get(0).getIdentifier();
    c2 = sub.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col3");
    Assert.assertEquals(c2.getName(), "col4");

    // check for DISTINCT's third operand col5
    c1 = distinctFunction.getOperands().get(2).getIdentifier();
    Assert.assertEquals(c1.getName(), "col5");

    // check for DISTINCT's fourth operand col6
    c2 = distinctFunction.getOperands().get(3).getIdentifier();
    Assert.assertEquals(c2.getName(), "col6");

    converter = new PinotQuery2BrokerRequestConverter();
    brokerRequest = converter.convert(pinotQuery);
    aggregationInfos = brokerRequest.getAggregationsInfo();

    Assert.assertEquals(aggregationInfos.size(), 1);
    aggregationInfo = aggregationInfos.get(0);
    Assert.assertEquals(aggregationInfo.getAggregationType(), AggregationFunctionType.DISTINCT.getName());
    Assert.assertEquals(aggregationInfo.getExpressions(),
        Arrays.asList("add(div(col1,col2),mul(col3,col4))", "sub(col3,col4)", "col5", "col6"));
  }

  @Test
  public void testQueryValidation() {
    // Valid: Selection fields are part of group by identifiers.
    String sql =
        "select group_country, sum(rsvp_count), count(*) from meetupRsvp group by group_city, group_country ORDER BY sum(rsvp_count), count(*) limit 50";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 2);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);

    // Invalid: Selection field 'group_city' is not part of group by identifiers.
    try {
      sql =
          "select group_city, group_country, sum(rsvp_count), count(*) from meetupRsvp group by group_country ORDER BY sum(rsvp_count), count(*) limit 50";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains("'group_city' should appear in GROUP BY clause."));
    }

    // Valid groupBy non-aggregate function should pass.
    sql =
        "select dateConvert(secondsSinceEpoch), sum(rsvp_count), count(*) from meetupRsvp group by dateConvert(secondsSinceEpoch) limit 50";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);

    // Invalid: secondsSinceEpoch should be in groupBy clause.
    try {
      sql =
          "select secondsSinceEpoch, dateConvert(secondsSinceEpoch), sum(rsvp_count), count(*) from meetupRsvp group by dateConvert(secondsSinceEpoch) limit 50";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains("'secondsSinceEpoch' should appear in GROUP BY clause."));
    }

    // Invalid groupBy clause shouldn't contain aggregate expression, like sum(rsvp_count), count(*).
    try {
      sql =
          "select  sum(rsvp_count), count(*) from meetupRsvp group by group_country, sum(rsvp_count), count(*) limit 50";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains("is not allowed in GROUP BY clause."));
    }
  }

  @Test
  public void testAliasQuery() {
    String sql;
    PinotQuery pinotQuery;
    // Valid alias in query.
    sql =
        "select secondsSinceEpoch, sum(rsvp_count) as sum_rsvp_count, count(*) as cnt from meetupRsvp group by secondsSinceEpoch order by cnt, sum_rsvp_count DESC limit 50";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 2);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "ASC");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "COUNT");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperator(), "DESC");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "SUM");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "rsvp_count");

    // Valid mixed alias expressions in query.
    sql =
        "select secondsSinceEpoch, sum(rsvp_count), count(*) as cnt from meetupRsvp group by secondsSinceEpoch order by cnt, sum(rsvp_count) DESC limit 50";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 2);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "ASC");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "COUNT");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperator(), "DESC");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "SUM");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "rsvp_count");

    sql =
        "select secondsSinceEpoch/86400 AS daysSinceEpoch, sum(rsvp_count) as sum_rsvp_count, count(*) as cnt from meetupRsvp where daysSinceEpoch = 18523 group by daysSinceEpoch order by cnt, sum_rsvp_count DESC limit 50";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "DIVIDE");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "secondsSinceEpoch");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getLongValue(), 86400);
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 18523);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 1);
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getFunctionCall().getOperator(), "DIVIDE");
    Assert.assertEquals(
        pinotQuery.getGroupByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "secondsSinceEpoch");
    Assert.assertEquals(
        pinotQuery.getGroupByList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 86400);
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 2);

    // Invalid groupBy clause shouldn't contain aggregate expression, like sum(rsvp_count), count(*).
    try {
      sql = "select  sum(rsvp_count), count(*) as cnt from meetupRsvp group by group_country, cnt limit 50";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains("is not allowed in GROUP BY clause."));
    }
  }

  @Test
  public void testAliasInSelection() {
    String sql;
    PinotQuery pinotQuery;
    sql = "SELECT C1 AS ALIAS_C1, C2 AS ALIAS_C2, ADD(C1, C2) FROM Foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), SqlKind.AS.toString());
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "C1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "ALIAS_C1");

    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), SqlKind.AS.toString());
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "C2");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "ALIAS_C2");

    Assert.assertEquals(pinotQuery.getSelectList().get(2).getFunctionCall().getOperator(), "ADD");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "C1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "C2");

    // Invalid groupBy clause shouldn't contain aggregate expression, like sum(rsvp_count), count(*).
    try {
      sql = "SELECT C1 AS ALIAS_C1, C2 AS ALIAS_C2, ADD(alias_c1, alias_c2) FROM Foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains("cannot be referred in SELECT Clause"));
    }
  }

  @Test
  public void testArithmeticOperator() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select a,b+2,c*5,(d+5)*2 from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 4);
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), "PLUS");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "b");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(2).getFunctionCall().getOperator(), "TIMES");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "c");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(3).getFunctionCall().getOperator(), "TIMES");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "PLUS");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "d");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getLongValue(), 5);
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 2);

    pinotQuery = CalciteSqlParser.compileToPinotQuery("select a % 200 + b * 5  from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "PLUS");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "MOD");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "a");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getLongValue(), 200);
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(),
        "TIMES");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "b");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .getLiteral().getLongValue(), 5);
  }

  /**
   * SqlConformanceLevel BABEL allows most reserved keywords in the query.
   * Some exceptions are time related keywords (date, timestamp, time), table, group, which need to be escaped
   */
  @Test
  public void testReservedKeywords() {

    // min, max, avg, sum, value, count, groups
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select max(value) as max, min(value) as min, sum(value) as sum, count(*) as count, avg(value) as avg from myTable where groups = 'foo'");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "AS");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "MAX");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "value");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "max");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), "AS");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "MIN");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "value");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "min");
    Assert.assertEquals(pinotQuery.getSelectList().get(2).getFunctionCall().getOperator(), "AS");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "SUM");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "value");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "sum");
    Assert.assertEquals(pinotQuery.getSelectList().get(3).getFunctionCall().getOperator(), "AS");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "COUNT");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "*");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "count");
    Assert.assertEquals(pinotQuery.getSelectList().get(4).getFunctionCall().getOperator(), "AS");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(4).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "AVG");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(4).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "value");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(4).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "avg");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    Assert
        .assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getIdentifier().getName(),
            "groups");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "foo");

    // language, module, return, position, system
    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select * from myTable where (language = 'en' or return > 100) and position < 10 order by module, system desc");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "AND");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "position");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "language");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "return");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "module");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "system");

    // table - need to escape
    try {
      CalciteSqlParser.compileToPinotQuery("select count(*) from myTable where table = 'foo'");
      Assert.fail("Query should have failed to compile");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getMessage().contains("Encountered") && e.getMessage().contains("table"));
    }
    // date - need to escape
    try {
      CalciteSqlParser.compileToPinotQuery("select count(*) from myTable group by Date");
      Assert.fail("Query should have failed to compile");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getMessage().contains("Encountered") && e.getMessage().contains("Date"));
    }

    // timestamp - need to escape
    try {
      CalciteSqlParser.compileToPinotQuery("select count(*) from myTable where timestamp < 1000");
      Assert.fail("Query should have failed to compile");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getMessage().contains("Encountered") && e.getMessage().contains("timestamp"));
    }

    // time - need to escape
    try {
      CalciteSqlParser.compileToPinotQuery("select count(*) from myTable where time > 100");
      Assert.fail("Query should have failed to compile");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getMessage().contains("Encountered") && e.getMessage().contains("time"));
    }

    // group - need to escape
    try {
      CalciteSqlParser.compileToPinotQuery("select group from myTable where bar = 'foo'");
      Assert.fail("Query should have failed to compile");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getCause() instanceof SqlParseException);
      Assert.assertTrue(e.getMessage().contains("Encountered") && e.getMessage().contains("group"));
    }

    // escaping the above works
    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select sum(foo) from \"table\" where \"Date\" = 2019 and (\"timestamp\" < 100 or \"time\" > 200) group by \"group\"");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getDataSource().getTableName(), "table");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "Date");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "timestamp");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "time");
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getIdentifier().getName(), "group");
  }

  @Test
  public void testCastTransformation() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select CAST(25.65 AS int) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals("CAST", pinotQuery.getSelectList().get(0).getFunctionCall().getOperator());
    Assert.assertEquals(25.65,
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getLiteral().getDoubleValue());
    Assert.assertEquals("INTEGER",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue());

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST('20170825' AS LONG) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals("CAST", pinotQuery.getSelectList().get(0).getFunctionCall().getOperator());
    Assert.assertEquals("20170825",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getLiteral().getStringValue());
    Assert.assertEquals("LONG",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue());

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST(20170825.0 AS Float) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals("CAST", pinotQuery.getSelectList().get(0).getFunctionCall().getOperator());
    Assert.assertEquals(20170825.0,
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getLiteral().getDoubleValue());
    Assert.assertEquals("FLOAT",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue());

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST(20170825.0 AS dOuble) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals("CAST", pinotQuery.getSelectList().get(0).getFunctionCall().getOperator());
    Assert.assertEquals(20170825.0,
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getLiteral().getDoubleValue());
    Assert.assertEquals("DOUBLE",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue());

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST(column1 AS STRING) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals("CAST", pinotQuery.getSelectList().get(0).getFunctionCall().getOperator());
    Assert.assertEquals("column1",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName());
    Assert.assertEquals("STRING",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue());

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST(column1 AS varchar) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals("CAST", pinotQuery.getSelectList().get(0).getFunctionCall().getOperator());
    Assert.assertEquals("column1",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName());
    Assert.assertEquals("VARCHAR",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue());

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT SUM(CAST(CAST(ArrTime AS STRING) AS LONG)) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals("SUM", pinotQuery.getSelectList().get(0).getFunctionCall().getOperator());
    Assert.assertEquals("CAST",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator());
    Assert.assertEquals("CAST",
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator());
  }

  @Test
  public void testDistinctCountRewrite() {
    String query = "SELECT count(distinct bar) FROM foo";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctCount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT count(distinct bar) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctCount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT count(distinct bar), distinctCount(bar) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctCount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), "distinctCount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT count(distinct bar), count(*), sum(a),min(a),max(b) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctCount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT count(distinct bar) AS distinct_bar, count(*), sum(a),min(a),max(b) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "AS");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "distinctCount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "bar");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "distinct_bar");
  }

  @Test
  public void testNoArgFunction() {
    String query = "SELECT noArgFunc() FROM foo ";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "noArgFunc");

    query = "SELECT a FROM foo where time_col > noArgFunc()";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Function greaterThan = pinotQuery.getFilterExpression().getFunctionCall();
    Function minus = greaterThan.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(minus.getOperands().get(1).getFunctionCall().getOperator(), "noArgFunc");

    query = "SELECT sum(a), noArgFunc() FROM foo group by noArgFunc()";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getFunctionCall().getOperator(), "noArgFunc");
  }

  @Test
  public void testCompilationInvokedFunction() {
    String query = "SELECT now() FROM foo ";
    long lowerBound = System.currentTimeMillis();
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    long nowTs = pinotQuery.getSelectList().get(0).getLiteral().getLongValue();
    long upperBound = System.currentTimeMillis();
    Assert.assertTrue(nowTs >= lowerBound);
    Assert.assertTrue(nowTs <= upperBound);

    query = "SELECT a FROM foo where time_col > now()";
    lowerBound = System.currentTimeMillis();
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Function greaterThan = pinotQuery.getFilterExpression().getFunctionCall();
    nowTs = greaterThan.getOperands().get(1).getLiteral().getLongValue();
    upperBound = System.currentTimeMillis();
    Assert.assertTrue(nowTs >= lowerBound);
    Assert.assertTrue(nowTs <= upperBound);

    query = "SELECT a FROM foo where time_col > fromDateTime('2020-01-01 UTC', 'yyyy-MM-dd z')";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    greaterThan = pinotQuery.getFilterExpression().getFunctionCall();
    nowTs = greaterThan.getOperands().get(1).getLiteral().getLongValue();
    Assert.assertEquals(nowTs, 1577836800000L);
  }

  @Test
  public void testCompilationInvokedNestedFunctions() {
    String query = "SELECT a FROM foo where time_col > toDateTime(now(), 'yyyy-MM-dd z')";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Function greaterThan = pinotQuery.getFilterExpression().getFunctionCall();
    String today = greaterThan.getOperands().get(1).getLiteral().getStringValue();
    String expectedTodayStr =
        "'" + Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd z")) + "'";
    Assert.assertEquals(today, expectedTodayStr);
  }

  @Test
  public void testCompileTimeExpression()
      throws SqlParseException {
    // True
    long lowerBound = System.currentTimeMillis();
    Expression expression = CalciteSqlParser.compileToExpression("now()");
    Assert.assertTrue(expression.getFunctionCall() != null);
    expression = CalciteSqlParser.invokeCompileTimeFunctionExpression(expression);
    Assert.assertTrue(expression.getLiteral() != null);
    long nowTs = expression.getLiteral().getLongValue();
    long upperBound = System.currentTimeMillis();
    Assert.assertTrue(nowTs >= lowerBound);
    Assert.assertTrue(nowTs <= upperBound);
    expression = CalciteSqlParser.compileToExpression("toDateTime(now(), 'yyyy-MM-dd z')");
    Assert.assertTrue(expression.getFunctionCall() != null);
    expression = CalciteSqlParser.invokeCompileTimeFunctionExpression(expression);
    Assert.assertTrue(expression.getLiteral() != null);
    String today = expression.getLiteral().getStringValue();
    String expectedTodayStr =
        "'" + Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd z")) + "'";
    Assert.assertEquals(today, expectedTodayStr);
    expression = CalciteSqlParser.compileToExpression("toDateTime(playerName)");
    Assert.assertTrue(expression.getFunctionCall() != null);
    expression = CalciteSqlParser.invokeCompileTimeFunctionExpression(expression);
    Assert.assertTrue(expression.getFunctionCall() != null);
    Assert.assertEquals(expression.getFunctionCall().getOperator(), "toDateTime");
    Assert.assertEquals(expression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "playerName");
    expression = CalciteSqlParser.compileToExpression("reverse(playerName)");
    Assert.assertTrue(expression.getFunctionCall() != null);
    expression = CalciteSqlParser.invokeCompileTimeFunctionExpression(expression);
    Assert.assertTrue(expression.getFunctionCall() != null);
    Assert.assertEquals(expression.getFunctionCall().getOperator(), "reverse");
    Assert.assertEquals(expression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "playerName");
    expression = CalciteSqlParser.compileToExpression("reverse('playerName')");
    Assert.assertTrue(expression.getFunctionCall() != null);
    expression = CalciteSqlParser.invokeCompileTimeFunctionExpression(expression);
    Assert.assertTrue(expression.getLiteral() != null);
    Assert.assertEquals(expression.getLiteral().getFieldValue(), "'emaNreyalp'");
    expression = CalciteSqlParser.compileToExpression("count(*)");
    Assert.assertTrue(expression.getFunctionCall() != null);
    expression = CalciteSqlParser.invokeCompileTimeFunctionExpression(expression);
    Assert.assertTrue(expression.getFunctionCall() != null);
    Assert.assertEquals(expression.getFunctionCall().getOperator(), "COUNT");
    Assert.assertEquals(expression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "*");
  }
}