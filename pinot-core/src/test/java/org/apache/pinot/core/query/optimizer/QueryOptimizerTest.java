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
package org.apache.pinot.core.query.optimizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class QueryOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("int", DataType.INT)
          .addSingleValueDimension("long", DataType.LONG).addSingleValueDimension("float", DataType.FLOAT)
          .addSingleValueDimension("double", DataType.DOUBLE).addSingleValueDimension("string", DataType.STRING)
          .addSingleValueDimension("bytes", DataType.BYTES).addMultiValueDimension("mvInt", DataType.INT).build();

  @Test
  public void testNoFilter() {
    String query = "SELECT * FROM testTable";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    assertNull(pinotQuery.getFilterExpression());
  }

  @Test
  public void testFlattenAndOrFilter() {
    String query =
        "SELECT * FROM testTable WHERE ((int = 4 OR (long = 5 AND (float = 9 AND double = 7.5))) OR string = 'foo') "
            + "OR bytes = 'abc'";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.OR.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 4);
    assertEquals(children.get(0), getEqFilterExpression("int", 4));
    assertEquals(children.get(2), getEqFilterExpression("string", "foo"));
    assertEquals(children.get(3), getEqFilterExpression("bytes", "abc"));

    Function secondChildFunction = children.get(1).getFunctionCall();
    assertEquals(secondChildFunction.getOperator(), FilterKind.AND.name());
    List<Expression> secondChildChildren = secondChildFunction.getOperands();
    assertEquals(secondChildChildren.size(), 3);
    assertEquals(secondChildChildren.get(0), getEqFilterExpression("long", 5L));
    assertEquals(secondChildChildren.get(1), getEqFilterExpression("float", 9f));
    assertEquals(secondChildChildren.get(2), getEqFilterExpression("double", 7.5));
  }

  private static Expression getEqFilterExpression(String column, Object value) {
    Expression eqFilterExpression = RequestUtils.getFunctionExpression(FilterKind.EQUALS.name());
    eqFilterExpression.getFunctionCall().setOperands(
        Arrays.asList(RequestUtils.getIdentifierExpression(column), RequestUtils.getLiteralExpression(value)));
    return eqFilterExpression;
  }

  @Test
  public void testMergeEqInFilter() {
    String query =
        "SELECT * FROM testTable WHERE int IN (1, 1) AND (long IN (2, 3) OR long IN (3, 4) OR long = 2) AND (float = "
            + "3.5 OR double IN (1.1, 1.2) OR float = 4.5 OR float > 5.5 OR double = 1.3)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> children = filterFunction.getOperands();
    assertEquals(children.size(), 3);
    assertEquals(children.get(0), getEqFilterExpression("int", 1));
    checkInFilterFunction(children.get(1).getFunctionCall(), "long", Arrays.asList(2L, 3L, 4L));

    Function thirdChildFunction = children.get(2).getFunctionCall();
    assertEquals(thirdChildFunction.getOperator(), FilterKind.OR.name());
    List<Expression> thirdChildChildren = thirdChildFunction.getOperands();
    assertEquals(thirdChildChildren.size(), 3);
    assertEquals(thirdChildChildren.get(0).getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());

    // Order of second and third child is not deterministic
    Function secondGrandChildFunction = thirdChildChildren.get(1).getFunctionCall();
    assertEquals(secondGrandChildFunction.getOperator(), FilterKind.IN.name());
    Function thirdGrandChildFunction = thirdChildChildren.get(2).getFunctionCall();
    assertEquals(thirdGrandChildFunction.getOperator(), FilterKind.IN.name());
    if (secondGrandChildFunction.getOperands().get(0).getIdentifier().getName().equals("float")) {
      checkInFilterFunction(secondGrandChildFunction, "float", Arrays.asList(3.5, 4.5));
      checkInFilterFunction(thirdGrandChildFunction, "double", Arrays.asList(1.1, 1.2, 1.3));
    } else {
      checkInFilterFunction(secondGrandChildFunction, "double", Arrays.asList(1.1, 1.2, 1.3));
      checkInFilterFunction(thirdGrandChildFunction, "float", Arrays.asList(3.5, 4.5));
    }
  }

  private static void checkInFilterFunction(Function inFilterFunction, String column, List<Object> values) {
    assertEquals(inFilterFunction.getOperator(), FilterKind.IN.name());
    List<Expression> operands = inFilterFunction.getOperands();
    int numOperands = operands.size();
    assertEquals(numOperands, values.size() + 1);
    assertEquals(operands.get(0).getIdentifier().getName(), column);
    Set<Expression> valueExpressions = new HashSet<>();
    for (Object value : values) {
      valueExpressions.add(RequestUtils.getLiteralExpression(value));
    }
    for (int i = 1; i < numOperands; i++) {
      assertTrue(valueExpressions.contains(operands.get(i)));
    }
  }

  @Test
  public void testMergeRangeFilter() {
    String query =
        "SELECT * FROM testTable WHERE (int > 10 AND int <= 100 AND int BETWEEN 10 AND 20) OR (float BETWEEN 5.5 AND "
            + "7.5 AND float = 6 AND float < 6.5 AND float BETWEEN 6 AND 8) OR (string > '123' AND string > '23') OR "
            + "(mvInt > 5 AND mvInt < 0)";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.OR.name());
    List<Expression> operands = filterFunction.getOperands();
    assertEquals(operands.size(), 4);
    assertEquals(operands.get(0), getRangeFilterExpression("int", "(10\00020]"));
    // Alphabetical order for STRING column ('23' > '123')
    assertEquals(operands.get(2), getRangeFilterExpression("string", "(23\000*)"));

    Function secondChildFunction = operands.get(1).getFunctionCall();
    assertEquals(secondChildFunction.getOperator(), FilterKind.AND.name());
    List<Expression> secondChildChildren = secondChildFunction.getOperands();
    assertEquals(secondChildChildren.size(), 2);
    assertEquals(secondChildChildren.get(0), getEqFilterExpression("float", 6f));
    assertEquals(secondChildChildren.get(1), getRangeFilterExpression("float", "[6.0\0006.5)"));

    // Range filter on multi-value column should not be merged ([-5, 10] can match this filter)
    Function fourthChildFunction = operands.get(3).getFunctionCall();
    assertEquals(fourthChildFunction.getOperator(), FilterKind.AND.name());
    List<Expression> fourthChildChildren = fourthChildFunction.getOperands();
    assertEquals(fourthChildChildren.size(), 2);
    assertEquals(fourthChildChildren.get(0).getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
    assertEquals(fourthChildChildren.get(1).getFunctionCall().getOperator(), FilterKind.LESS_THAN.name());
  }

  @Test
  public void testMergeTextMatchFilter() {
    String query =
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar') OR TEXT_MATCH(string, "
            + "'baz')";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    assertEquals(filterFunction.getOperator(), FilterKind.TEXT_MATCH.name());
    List<Expression> operands = filterFunction.getOperands();
    assertEquals(operands.size(), 2);
    assertEquals(operands.get(0), RequestUtils.getIdentifierExpression("string"));
    assertEquals(operands.get(1), RequestUtils.getLiteralExpression("foo AND bar OR baz"));
  }

  private static Expression getRangeFilterExpression(String column, String rangeString) {
    Expression rangeFilterExpression = RequestUtils.getFunctionExpression(FilterKind.RANGE.name());
    rangeFilterExpression.getFunctionCall().setOperands(
        Arrays.asList(RequestUtils.getIdentifierExpression(column), RequestUtils.getLiteralExpression(rangeString)));
    return rangeFilterExpression;
  }

  @Test
  public void testQueries() {
    // MergeEqInFilter
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int = 2 OR int = 3",
        "SELECT * FROM testTable WHERE int IN (1, 2, 3)");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int = 2 OR int = 3 AND long = 4",
        "SELECT * FROM testTable WHERE int IN (1, 2) OR (int = 3 AND long = 4)");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int = 2 OR int = 3 OR long = 4 OR long = 5 OR long = 6",
        "SELECT * FROM testTable WHERE int IN (1, 2, 3) OR long IN (4, 5, 6)");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR long = 4 OR int = 2 OR long = 5 OR int = 3 OR long = 6",
        "SELECT * FROM testTable WHERE int IN (1, 2, 3) OR long IN (4, 5, 6)");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int = 1", "SELECT * FROM testTable WHERE int = 1");
    testQuery("SELECT * FROM testTable WHERE (int = 1 OR int = 1) AND long = 2",
        "SELECT * FROM testTable WHERE int = 1 AND long = 2");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR int IN (2, 3, 4, 5)",
        "SELECT * FROM testTable WHERE int IN (1, 2, 3, 4, 5)");
    testQuery("SELECT * FROM testTable WHERE int IN (1, 1) OR int = 1", "SELECT * FROM testTable WHERE int = 1");
    testQuery("SELECT * FROM testTable WHERE string = 'foo' OR string = 'bar' OR string = 'foobar'",
        "SELECT * FROM testTable WHERE string IN ('foo', 'bar', 'foobar')");
    testQuery("SELECT * FROM testTable WHERE bytes = 'dead' OR bytes = 'beef' OR bytes = 'deadbeef'",
        "SELECT * FROM testTable WHERE bytes IN ('dead', 'beef', 'deadbeef')");

    // MergeRangeFilter
    testQuery("SELECT * FROM testTable WHERE int >= 10 AND int <= 20",
        "SELECT * FROM testTable WHERE int BETWEEN 10 AND 20");
    testQuery("SELECT * FROM testTable WHERE int BETWEEN 10 AND 20 AND int > 7 AND int <= 17 OR int > 20",
        "SELECT * FROM testTable WHERE int BETWEEN 10 AND 17 OR int > 20");
    testQuery("SELECT * FROM testTable WHERE long BETWEEN 10 AND 20 AND long > 7 AND long <= 17 OR long > 20",
        "SELECT * FROM testTable WHERE long BETWEEN 10 AND 17 OR long > 20");
    testQuery("SELECT * FROM testTable WHERE float BETWEEN 10.5 AND 20 AND float > 7 AND float <= 17.5 OR float > 20",
        "SELECT * FROM testTable WHERE float BETWEEN 10.5 AND 17.5 OR float > 20");
    testQuery(
        "SELECT * FROM testTable WHERE double BETWEEN 10.5 AND 20 AND double > 7 AND double <= 17.5 OR double > 20",
        "SELECT * FROM testTable WHERE double BETWEEN 10.5 AND 17.5 OR double > 20");
    testQuery(
        "SELECT * FROM testTable WHERE string BETWEEN '10' AND '20' AND string > '7' AND string <= '17' OR string > "
            + "'20'", "SELECT * FROM testTable WHERE string > '7' AND string <= '17' OR string > '20'");
    testQuery(
        "SELECT * FROM testTable WHERE bytes BETWEEN '10' AND '20' AND bytes > '07' AND bytes <= '17' OR bytes > '20'",
        "SELECT * FROM testTable WHERE bytes BETWEEN '10' AND '17' OR bytes > '20'");
    testQuery(
        "SELECT * FROM testTable WHERE int > 10 AND long > 20 AND int <= 30 AND long <= 40 AND int >= 15 AND long >= "
            + "25", "SELECT * FROM testTable WHERE int BETWEEN 15 AND 30 AND long BETWEEN 25 AND 40");
    testQuery("SELECT * FROM testTable WHERE int > 10 AND int > 20 OR int < 30 AND int < 40",
        "SELECT * FROM testTable WHERE int > 20 OR int < 30");
    testQuery("SELECT * FROM testTable WHERE int > 10 AND int > 20 OR long < 30 AND long < 40",
        "SELECT * FROM testTable WHERE int > 20 OR long < 30");

    // Mixed
    testQuery(
        "SELECT * FROM testTable WHERE int >= 20 AND (int > 10 AND (int IN (1, 2) OR (int = 2 OR int = 3)) AND int <="
            + " 30)", "SELECT * FROM testTable WHERE int BETWEEN 20 AND 30 AND int IN (1, 2, 3)");

    // IdenticalPredicateOptimizer
    testQuery("SELECT * FROM testTable WHERE 1=1", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1!=1", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE 1=1 AND 1!=1", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE 1=1 OR 1!=1", "SELECT * FROM testTable WHERE true");

    testQuery("SELECT * FROM testTable WHERE \"a\"=\"a\"", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE \"a\"!=\"a\"", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE \"a\"=\"a\" AND \"a\"!=\"a\"", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE \"a\"=\"a\" OR \"a\"!=\"a\"", "SELECT * FROM testTable WHERE true");

    testQuery("SELECT * FROM testTable WHERE 1=1 AND \"a\"=\"a\"", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1=1 OR \"a\"=\"a\"", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1!=1 AND \"a\"=\"a\"", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE 1=1 AND \"a\"!=\"a\"", "SELECT * FROM testTable WHERE false");
    testQuery("SELECT * FROM testTable WHERE 1!=1 OR \"a\"=\"a\"", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1=1 OR \"a\"!=\"a\"", "SELECT * FROM testTable WHERE true");

    testQuery("SELECT * FROM testTable WHERE 1.0=1.0", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1.0=1", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE 1.01=1", "SELECT * FROM testTable WHERE false");

    testQuery("SELECT * FROM testTable WHERE 1=1 AND true", "SELECT * FROM testTable WHERE true");
    testQuery("SELECT * FROM testTable WHERE \"a\"=\"a\" AND true", "SELECT * FROM testTable WHERE true");

    // TextMatchFilterOptimizer
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo AND bar')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, '\"foo bar\"') AND TEXT_MATCH(string, 'baz')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, '\"foo bar\" AND baz')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, '\"foo bar\"') AND TEXT_MATCH(string, '/.*ooba.*/')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, '\"foo bar\" AND /.*ooba.*/')");
    testQuery("SELECT * FROM testTable WHERE int = 1 AND TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE int = 1 AND TEXT_MATCH(string, 'foo AND bar')");
    testQuery("SELECT * FROM testTable WHERE int = 1 OR TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE int = 1 OR TEXT_MATCH(string, 'foo AND bar')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') AND NOT TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo AND NOT bar')");
    testQuery("SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string, 'NOT foo AND bar')");
    testQuery("SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, 'foo') AND NOT TEXT_MATCH(string, 'bar')",
        "SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, 'foo AND bar')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo') AND TEXT_MATCH(string, 'bar') OR "
        + "TEXT_MATCH(string, 'baz')", "SELECT * FROM testTable WHERE TEXT_MATCH(string, 'foo AND bar OR baz')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string1, 'foo1') AND TEXT_MATCH(string1, 'bar1') OR "
            + "TEXT_MATCH(string1, 'baz1') AND TEXT_MATCH(string2, 'foo')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string1, 'foo1 AND bar1') OR TEXT_MATCH(string1, 'baz1') AND "
            + "TEXT_MATCH(string2, 'foo')");
    testQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string1, 'foo1') AND TEXT_MATCH(string1, 'bar1')"
            + "AND TEXT_MATCH(string2, 'foo2') AND TEXT_MATCH(string2, 'bar2')",
        "SELECT * FROM testTable WHERE TEXT_MATCH(string1, 'foo1 AND bar1') AND TEXT_MATCH(string2, 'foo2 AND bar2')");
    testCannotOptimizeQuery("SELECT * FROM testTable WHERE TEXT_MATCH(string1, 'foo') OR TEXT_MATCH(string2, 'bar')");
    testCannotOptimizeQuery(
        "SELECT * FROM testTable WHERE int = 1 AND TEXT_MATCH(string, 'foo') OR TEXT_MATCH(string, 'bar')");
    testCannotOptimizeQuery("SELECT * FROM testTable WHERE NOT TEXT_MATCH(string, 'foo')");
  }

  private static void testQuery(String actual, String expected) {
    assertNotEquals(actual, expected, "You must provide different queries to test");
    PinotQuery actualPinotQuery = CalciteSqlParser.compileToPinotQuery(actual);
    OPTIMIZER.optimize(actualPinotQuery, SCHEMA);
    // Also optimize the expected query because the expected range can only be generate via optimizer
    PinotQuery expectedPinotQuery = CalciteSqlParser.compileToPinotQuery(expected);
    OPTIMIZER.optimize(expectedPinotQuery, SCHEMA);
    comparePinotQuery(actualPinotQuery, expectedPinotQuery);
  }

  private static void testCannotOptimizeQuery(String query) {
    PinotQuery actualPinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    OPTIMIZER.optimize(actualPinotQuery, SCHEMA);
    PinotQuery expectedPinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    comparePinotQuery(actualPinotQuery, expectedPinotQuery);
  }

  private static void comparePinotQuery(PinotQuery actual, PinotQuery expected) {
    if (expected.getFilterExpression() == null) {
      assertNull(actual.getFilterExpression());
      return;
    }
    compareFilterExpression(actual.getFilterExpression(), expected.getFilterExpression());
  }

  private static void compareFilterExpression(Expression actual, Expression expected) {
    if (actual.isSetLiteral()) {
      assertNull(actual.getFunctionCall());
      assertNull(expected.getFunctionCall());
      assertTrue(expected.isSetLiteral());
      assertEquals(actual.getLiteral(), expected.getLiteral());
    } else {
      Function actualFilterFunction = actual.getFunctionCall();
      Function expectedFilterFunction = expected.getFunctionCall();
      FilterKind actualFilterKind = FilterKind.valueOf(actualFilterFunction.getOperator());
      FilterKind expectedFilterKind = FilterKind.valueOf(expectedFilterFunction.getOperator());
      List<Expression> actualOperands = actualFilterFunction.getOperands();
      List<Expression> expectedOperands = expectedFilterFunction.getOperands();
      if (!actualFilterKind.isRange()) {
        assertEquals(actualFilterKind, expectedFilterKind);
        assertEquals(actualOperands.size(), expectedOperands.size());
        if (actualFilterKind == FilterKind.AND || actualFilterKind == FilterKind.OR) {
          compareFilterExpressionChildren(actualOperands, expectedOperands);
        } else {
          assertEquals(actualOperands.get(0), expectedOperands.get(0));
          if (actualFilterKind == FilterKind.IN || actualFilterKind == FilterKind.NOT_IN) {
            // Handle different order of values
            assertEqualsNoOrder(actualOperands.toArray(), expectedOperands.toArray());
          } else {
            assertEquals(actualOperands, expectedOperands);
          }
        }
      } else {
        assertTrue(expectedFilterKind.isRange());
        assertEquals(getRangeString(actualFilterKind, actualOperands),
            getRangeString(expectedFilterKind, expectedOperands));
      }
    }
  }

  /**
   * Handles different order of children under AND/OR filter.
   */
  private static void compareFilterExpressionChildren(List<Expression> actual, List<Expression> expected) {
    assertEquals(actual.size(), expected.size());
    List<Expression> unmatchedExpectedChildren = new ArrayList<>(expected);
    for (Expression actualChild : actual) {
      Iterator<Expression> iterator = unmatchedExpectedChildren.iterator();
      boolean findMatchingChild = false;
      while (iterator.hasNext()) {
        try {
          compareFilterExpression(actualChild, iterator.next());
          iterator.remove();
          findMatchingChild = true;
          break;
        } catch (AssertionError e) {
          // Ignore
        }
      }
      if (!findMatchingChild) {
        fail("Failed to find matching child");
      }
    }
  }

  private static String getRangeString(FilterKind filterKind, List<Expression> operands) {
    switch (filterKind) {
      case GREATER_THAN:
        return Range.LOWER_EXCLUSIVE + operands.get(1).getLiteral().getFieldValue().toString() + Range.UPPER_UNBOUNDED;
      case GREATER_THAN_OR_EQUAL:
        return Range.LOWER_INCLUSIVE + operands.get(1).getLiteral().getFieldValue().toString() + Range.UPPER_UNBOUNDED;
      case LESS_THAN:
        return Range.LOWER_UNBOUNDED + operands.get(1).getLiteral().getFieldValue().toString() + Range.UPPER_EXCLUSIVE;
      case LESS_THAN_OR_EQUAL:
        return Range.LOWER_UNBOUNDED + operands.get(1).getLiteral().getFieldValue().toString() + Range.UPPER_INCLUSIVE;
      case BETWEEN:
        return Range.LOWER_INCLUSIVE + operands.get(1).getLiteral().getFieldValue().toString() + Range.DELIMITER
            + operands.get(2).getLiteral().getFieldValue().toString() + Range.UPPER_INCLUSIVE;
      case RANGE:
        return operands.get(1).getLiteral().getStringValue();
      default:
        throw new IllegalStateException();
    }
  }
}
