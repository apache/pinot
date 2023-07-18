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

import java.io.StringReader;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Join;
import org.apache.pinot.common.request.JoinType;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.parser.ParseException;
import org.apache.pinot.sql.parsers.parser.SqlInsertFromFile;
import org.apache.pinot.sql.parsers.parser.SqlParserImpl;
import org.apache.pinot.sql.parsers.rewriter.CompileTimeFunctionsInvoker;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Some tests for the SQL compiler.
 */
public class CalciteSqlCompilerTest {
  private static final long ONE_HOUR_IN_MS = TimeUnit.HOURS.toMillis(1);

  @Test
  public void testCanonicalFunctionName() {
    Expression expression = CalciteSqlParser.compileToExpression("dIsTiNcT_cOuNt(AbC)");
    Function function = expression.getFunctionCall();
    Assert.assertEquals(function.getOperator(), AggregationFunctionType.DISTINCTCOUNT.name().toLowerCase());
    Assert.assertEquals(function.getOperands().size(), 1);
    Assert.assertEquals(function.getOperands().get(0).getIdentifier().getName(), "AbC");

    expression = CalciteSqlParser.compileToExpression("ReGeXpLiKe(AbC)");
    function = expression.getFunctionCall();
    Assert.assertEquals(function.getOperator(), FilterKind.REGEXP_LIKE.name());
    Assert.assertEquals(function.getOperands().size(), 1);
    Assert.assertEquals(function.getOperands().get(0).getIdentifier().getName(), "AbC");

    expression = CalciteSqlParser.compileToExpression("aBc > DeF");
    function = expression.getFunctionCall();
    Assert.assertEquals(function.getOperator(), FilterKind.GREATER_THAN.name());
    Assert.assertEquals(function.getOperands().size(), 2);
    Assert.assertEquals(function.getOperands().get(0).getIdentifier().getName(), "aBc");
    Assert.assertEquals(function.getOperands().get(1).getIdentifier().getName(), "DeF");
  }

  @Test
  public void testCaseWhenStatements() {
    //@formatter:off
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT OrderID, Quantity,\n"
            + "CASE\n"
            + "    WHEN Quantity > 30 THEN 'The quantity is greater than 30'\n"
            + "    WHEN Quantity = 30 THEN 'The quantity is 30'\n"
            + "    ELSE 'The quantity is under 30'\n"
            + "END AS QuantityText\n"
            + "FROM OrderDetails");
    //@formatter:on
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "OrderID");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getIdentifier().getName(), "Quantity");
    Function asFunc = pinotQuery.getSelectList().get(2).getFunctionCall();
    Assert.assertEquals(asFunc.getOperator(), "as");
    Function caseFunc = asFunc.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(caseFunc.getOperator(), "case");
    Assert.assertEquals(caseFunc.getOperandsSize(), 5);
    Function greatThanFunc = caseFunc.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(greatThanFunc.getOperator(), FilterKind.GREATER_THAN.name());
    Assert.assertEquals(greatThanFunc.getOperands().get(0).getIdentifier().getName(), "Quantity");
    Assert.assertEquals(greatThanFunc.getOperands().get(1).getLiteral().getFieldValue(), 30L);
    Function equalsFunc = caseFunc.getOperands().get(1).getFunctionCall();
    Assert.assertEquals(equalsFunc.getOperator(), FilterKind.EQUALS.name());
    Assert.assertEquals(equalsFunc.getOperands().get(0).getIdentifier().getName(), "Quantity");
    Assert.assertEquals(equalsFunc.getOperands().get(1).getLiteral().getFieldValue(), 30L);
    Assert.assertEquals(caseFunc.getOperands().get(2).getLiteral().getFieldValue(), "The quantity is greater than 30");
    Assert.assertEquals(caseFunc.getOperands().get(3).getLiteral().getFieldValue(), "The quantity is 30");
    Assert.assertEquals(caseFunc.getOperands().get(4).getLiteral().getFieldValue(), "The quantity is under 30");

    //@formatter:off
    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT Quantity,\n"
            + "SUM(CASE\n"
            + "    WHEN Quantity > 30 THEN 3\n"
            + "    WHEN Quantity > 20 THEN 2\n"
            + "    WHEN Quantity > 10 THEN 1\n"
            + "    ELSE 0\n"
            + "END) AS new_sum_quant\n"
            + "FROM OrderDetails GROUP BY Quantity");
    //@formatter:on
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "Quantity");
    asFunc = pinotQuery.getSelectList().get(1).getFunctionCall();
    Assert.assertEquals(asFunc.getOperator(), "as");
    Function sumFunc = asFunc.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(sumFunc.getOperator(), "sum");
    caseFunc = sumFunc.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(caseFunc.getOperator(), "case");
    Assert.assertEquals(caseFunc.getOperandsSize(), 7);
    greatThanFunc = caseFunc.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(greatThanFunc.getOperator(), FilterKind.GREATER_THAN.name());
    Assert.assertEquals(greatThanFunc.getOperands().get(0).getIdentifier().getName(), "Quantity");
    Assert.assertEquals(greatThanFunc.getOperands().get(1).getLiteral().getFieldValue(), 30L);
    greatThanFunc = caseFunc.getOperands().get(1).getFunctionCall();
    Assert.assertEquals(greatThanFunc.getOperator(), FilterKind.GREATER_THAN.name());
    Assert.assertEquals(greatThanFunc.getOperands().get(0).getIdentifier().getName(), "Quantity");
    Assert.assertEquals(greatThanFunc.getOperands().get(1).getLiteral().getFieldValue(), 20L);
    greatThanFunc = caseFunc.getOperands().get(2).getFunctionCall();
    Assert.assertEquals(greatThanFunc.getOperator(), FilterKind.GREATER_THAN.name());
    Assert.assertEquals(greatThanFunc.getOperands().get(0).getIdentifier().getName(), "Quantity");
    Assert.assertEquals(greatThanFunc.getOperands().get(1).getLiteral().getFieldValue(), 10L);
    Assert.assertEquals(caseFunc.getOperands().get(3).getLiteral().getFieldValue(), 3L);
    Assert.assertEquals(caseFunc.getOperands().get(4).getLiteral().getFieldValue(), 2L);
    Assert.assertEquals(caseFunc.getOperands().get(5).getLiteral().getFieldValue(), 1L);
    Assert.assertEquals(caseFunc.getOperands().get(6).getLiteral().getFieldValue(), 0L);
  }

  @Test(expectedExceptions = SqlCompilationException.class)
  public void testInvalidCaseWhenStatements() {
    // Not support Aggregation functions in case statements.
    try {
      //@formatter:off
      CalciteSqlParser.compileToPinotQuery(
          "SELECT OrderID, Quantity,\n"
              + "CASE\n"
              + "    WHEN sum(Quantity) > 30 THEN 'The quantity is greater than 30'\n"
              + "    WHEN sum(Quantity) = 30 THEN 'The quantity is 30'\n"
              + "    ELSE 'The quantity is under 30'\n"
              + "END AS QuantityText\n"
              + "FROM OrderDetails");
      //@formatter:on
    } catch (SqlCompilationException e) {
      Assert.assertEquals(e.getMessage(),
          "Aggregation functions inside WHEN Clause is not supported - SUM(`Quantity`) > 30");
      throw e;
    }
  }

  @Test
  public void testQuotedStrings() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where origin = 'Martha''s Vineyard'");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "Martha's Vineyard");

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
  public void testExtract() {
    {
      // Case 1 -- Year and date format ('2017-06-15')
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT EXTRACT(YEAR FROM '2017-06-15')");
      Function function = pinotQuery.getSelectList().get(0).getFunctionCall();
      Assert.assertEquals(function.getOperands().get(0).getLiteral().getStringValue(), "YEAR");
      Assert.assertEquals(function.getOperands().get(1).getLiteral().getStringValue(), "2017-06-15");
    }
    {
      // Case 2 -- date format ('2017-06-15 09:34:21')
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT EXTRACT(YEAR FROM '2017-06-15 09:34:21')");
      Function function = pinotQuery.getSelectList().get(0).getFunctionCall();
      Assert.assertEquals(function.getOperands().get(0).getLiteral().getStringValue(), "YEAR");
      Assert.assertEquals(function.getOperands().get(1).getLiteral().getStringValue(), "2017-06-15 09:34:21");
    }
    {
      // Case 3 -- Month
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT EXTRACT(MONTH FROM '2017-06-15')");
      Function function = pinotQuery.getSelectList().get(0).getFunctionCall();
      Assert.assertEquals(function.getOperands().get(0).getLiteral().getStringValue(), "MONTH");
      Assert.assertEquals(function.getOperands().get(1).getLiteral().getStringValue(), "2017-06-15");
    }
    {
      // Case 4 -- Day
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT EXTRACT(DAY FROM '2017-06-15')");
      Function function = pinotQuery.getSelectList().get(0).getFunctionCall();
      Assert.assertEquals(function.getOperands().get(0).getLiteral().getStringValue(), "DAY");
      Assert.assertEquals(function.getOperands().get(1).getLiteral().getStringValue(), "2017-06-15");
    }
  }

  @Test
  public void testFilterClauses() {
    {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where a > 1.5");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.GREATER_THAN.name());
      Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "a");
      Assert.assertEquals(func.getOperands().get(1).getLiteral().getDoubleValue(), 1.5);
    }

    {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where b < 100");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.LESS_THAN.name());
      Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "b");
      Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 100L);
    }

    {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where c >= 10");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.GREATER_THAN_OR_EQUAL.name());
      Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "c");
      Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 10L);
    }

    {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where d <= 50");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.LESS_THAN_OR_EQUAL.name());
      Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "d");
      Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 50L);
    }

    {
      PinotQuery pinotQuery =
          CalciteSqlParser.compileToPinotQuery("select * from vegetables where e BETWEEN 70 AND 80");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.BETWEEN.name());
      Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "e");
      Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 70L);
      Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 80L);
    }

    {
      PinotQuery pinotQuery =
          CalciteSqlParser.compileToPinotQuery("select * from vegetables where regexp_like(E, '^U.*')");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), "REGEXP_LIKE");
      Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "E");
      Assert.assertEquals(func.getOperands().get(1).getLiteral().getStringValue(), "^U.*");
    }

    {
      PinotQuery pinotQuery =
          CalciteSqlParser.compileToPinotQuery("select * from vegetables where g IN (12, 13, 15.2, 17)");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.IN.name());
      Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "g");
      Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 12L);
      Assert.assertEquals(func.getOperands().get(2).getLiteral().getLongValue(), 13L);
      Assert.assertEquals(func.getOperands().get(3).getLiteral().getDoubleValue(), 15.2);
      Assert.assertEquals(func.getOperands().get(4).getLiteral().getLongValue(), 17L);
    }

    {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetable where g");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.EQUALS.name());
      Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "g");
      Assert.assertEquals(func.getOperands().get(1).getLiteral(), Literal.boolValue(true));
    }

    {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetable where g or f = true");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.OR.name());
      List<Expression> operands = func.getOperands();
      Assert.assertEquals(operands.size(), 2);
      Assert.assertEquals(operands.get(0).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      List<Expression> eqOperands = operands.get(0).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getIdentifier().getName(), "g");
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));
      eqOperands = operands.get(1).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getIdentifier().getName(), "f");
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));
    }

    {
      PinotQuery pinotQuery =
          CalciteSqlParser.compileToPinotQuery("select * from vegetable where startsWith(g, 'str')");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.EQUALS.name());
      Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "startswith");
      Assert.assertEquals(func.getOperands().get(1).getLiteral(), Literal.boolValue(true));
    }

    {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
          "select * from vegetable where startsWith(g, 'str')=true and startsWith(f, 'str')");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.AND.name());
      List<Expression> operands = func.getOperands();
      Assert.assertEquals(operands.size(), 2);

      Assert.assertEquals(operands.get(0).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      List<Expression> eqOperands = operands.get(0).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getFunctionCall().getOperator(), "startswith");
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));

      Assert.assertEquals(operands.get(1).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      eqOperands = operands.get(1).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getFunctionCall().getOperator(), "startswith");
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));
    }

    {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
          "select * from vegetable where (startsWith(g, 'str')=true and startsWith(f, 'str')) AND (e and d=true)");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.AND.name());
      List<Expression> operands = func.getOperands();
      Assert.assertEquals(operands.size(), 4);

      Assert.assertEquals(operands.get(0).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      List<Expression> eqOperands = operands.get(0).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getFunctionCall().getOperator(), "startswith");
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));

      Assert.assertEquals(operands.get(1).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      eqOperands = operands.get(1).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getFunctionCall().getOperator(), "startswith");
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));

      Assert.assertEquals(operands.get(2).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      eqOperands = operands.get(2).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getIdentifier().getName(), "e");
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));

      Assert.assertEquals(operands.get(3).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      eqOperands = operands.get(3).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getIdentifier().getName(), "d");
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));
    }

    {
      PinotQuery pinotQuery =
          CalciteSqlParser.compileToPinotQuery("select * from vegetable where isSubnetOf('192.168.0.1/24', foo)");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.EQUALS.name());
      List<Expression> operands = func.getOperands();
      Assert.assertEquals(operands.size(), 2);
      Assert.assertEquals(operands.get(0).getFunctionCall().getOperator(), "issubnetof");
      Assert.assertEquals(operands.get(1).getLiteral(), Literal.boolValue(true));
    }

    {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
          "select * from vegetable where isSubnetOf('192.168.0.1/24', foo)=true AND isSubnetOf('192.168.0.1/24', "
              + "foo)");
      Function func = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(func.getOperator(), FilterKind.AND.name());
      List<Expression> operands = func.getOperands();
      Assert.assertEquals(operands.size(), 2);
      Assert.assertEquals(operands.get(0).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      Assert.assertEquals(operands.get(1).getFunctionCall().getOperator(), FilterKind.EQUALS.name());

      List<Expression> lhs = operands.get(0).getFunctionCall().getOperands();
      Assert.assertEquals(lhs.size(), 2);
      Assert.assertEquals(lhs.get(0).getFunctionCall().getOperator(), "issubnetof");
      Assert.assertEquals(lhs.get(1).getLiteral(), Literal.boolValue(true));

      List<Expression> rhs = operands.get(1).getFunctionCall().getOperands();
      Assert.assertEquals(rhs.size(), 2);
      Assert.assertEquals(rhs.get(0).getFunctionCall().getOperator(), "issubnetof");
      Assert.assertEquals(rhs.get(1).getLiteral(), Literal.boolValue(true));
    }
  }

  @Test
  public void testFilterClausesWithRightExpression() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where a > b");
    Function func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.GREATER_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "minus");
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "a");
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "b");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 0L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where 0 < a-b");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.GREATER_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "minus");
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "a");
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "b");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 0L);

    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where b < 100 + c");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.LESS_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "minus");
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "b");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(), "plus");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getLiteral().getLongValue(), 100L);
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 0L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where b -(100+c)< 0");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.LESS_THAN.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "minus");
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "b");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(), "plus");
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
    Assert.assertEquals(func.getOperator(), FilterKind.LESS_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "minus");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "foo1");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(), "foo2");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "bar1");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "bar2");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "minus");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "plus");
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
    Assert.assertEquals(func.getOperator(), FilterKind.LESS_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getFunctionCall().getOperator(), "minus");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "foo1");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(), "foo2");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "bar1");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "bar2");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "minus");
    Assert.assertEquals(
        func.getOperands().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "plus");
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
    Assert.assertEquals(func.getOperator(), FilterKind.GREATER_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 10L);
    pinotQuery = CalciteSqlParser.compileToPinotQuery("select * from vegetables where 10 <= c");
    func = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertEquals(func.getOperator(), FilterKind.GREATER_THAN_OR_EQUAL.name());
    Assert.assertEquals(func.getOperands().get(0).getIdentifier().getName(), "c");
    Assert.assertEquals(func.getOperands().get(1).getLiteral().getLongValue(), 10L);
  }

  @Test
  public void testInvalidFilterClauses() {
    // Only support regexp_like
    testInvalidFilterClause("a like b");
    // Only support literals in IN/NOT_IN/REGEXP_LIKE/TEXT_MATCH/JSON_MATCH predicate
    testInvalidFilterClause("a in (\"b\")");
    testInvalidFilterClause("a not in ('b', c)");
    testInvalidFilterClause("regexp_like(a, b)");
    testInvalidFilterClause("text_match(a, \"b\")");
    testInvalidFilterClause("json_match(a, b");
    // Nested invalid filter
    testInvalidFilterClause("a = 1 and c in (\"d\")");
  }

  private void testInvalidFilterClause(String filter) {
    try {
      CalciteSqlParser.compileToPinotQuery("select * from vegetables where " + filter);
    } catch (SqlCompilationException e) {
      // Expected
      return;
    }
    Assert.fail("Should fail on invalid filter: " + filter);
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
          "select sum(rsvp_count), count(*), group_city from meetupRsvp group by group_city order by sum(rsvp_count) "
              + "limit 10");
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
        "sum");
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
        "sum");
    Assert.assertEquals(10, pinotQuery.getLimit());

    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(
          "select group_city, sum(rsvp_count), count(*) from meetupRsvp group by group_city order by sum(rsvp_count),"
              + " count(*) limit 10");
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
        "sum");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "rsvp_count");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "count");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "*");
    Assert.assertEquals(10, pinotQuery.getLimit());

    // nested functions in group by
    try {
      pinotQuery = CalciteSqlParser.compileToPinotQuery(
          "select concat(upper(playerName), lower(teamID), '-') playerTeam, "
              + "upper(league) leagueUpper, count(playerName) cnt from baseballStats group by playerTeam, lower"
              + "(teamID), leagueUpper having cnt > 1 order by cnt desc limit 10");
    } catch (SqlCompilationException e) {
      throw e;
    }
    Assert.assertTrue(pinotQuery.isSetGroupByList());
    Assert.assertEquals(pinotQuery.getGroupByList().size(), 3);
    Assert.assertTrue(pinotQuery.isSetLimit());
    Assert.assertTrue(pinotQuery.isSetOrderByList());
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getType(), ExpressionType.FUNCTION);
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "count");
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
      Assert.assertTrue(e.getCause().getMessage().contains("at line 1, column 31."),
          "Compilation exception should contain line and character for error message. Error message is "
              + e.getMessage());
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
  @Deprecated
  // TODO: to be removed once OPTIONS REGEX match is deprecated
  public void testQueryOptions() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where name <> 'Brussels sprouts'");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 0);
    Assert.assertNull(pinotQuery.getQueryOptions());

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select * from vegetables where name <> 'Brussels sprouts' OPTION (delicious=yes)");
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

    // Assert that wrongly inserted query option will not be parsed.
    try {
      CalciteSqlParser.compileToPinotQuery(
          "select * from vegetables where name <> 'Brussels sprouts' OPTION (delicious=yes) option(foo=1234) option"
              + "(bar='potato')");
    } catch (SqlCompilationException e) {
      Assert.assertTrue(e.getCause() instanceof ParseException);
      Assert.assertTrue(e.getCause().getMessage().contains("OPTION"));
    }
    try {
      CalciteSqlParser.compileToPinotQuery("select * from vegetables where name <> 'Brussels OPTION (delicious=yes)");
    } catch (SqlCompilationException e) {
      Assert.assertTrue(e.getCause() instanceof ParseException);
    }
  }

  @Test
  public void testQuerySetOptions() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select * from vegetables where name <> 'Brussels sprouts'");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 0);
    Assert.assertNull(pinotQuery.getQueryOptions());

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SET delicious='yes'; select * from vegetables where name <> 'Brussels sprouts'");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 1);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SET delicious='yes'; SET foo='1234'; SET bar='''potato''';"
        + "select * from vegetables where name <> 'Brussels sprouts' ");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 3);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("bar"), "'potato'");

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SET delicious='yes'; SET foo='1234'; "
        + "SET bar='''potato'''; select * from vegetables where name <> 'Brussels sprouts' ");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 3);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("bar"), "'potato'");

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SET delicious='yes'; SET foo='1234'; "
        + "select * from vegetables where name <> 'Brussels sprouts'; SET bar='''potato'''; ");
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 3);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("delicious"));
    Assert.assertEquals(pinotQuery.getQueryOptions().get("delicious"), "yes");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("foo"), "1234");
    Assert.assertEquals(pinotQuery.getQueryOptions().get("bar"), "'potato'");

    // test invalid options
    try {
      CalciteSqlParser.compileToPinotQuery(
          "select * from vegetables SET delicious='yes', foo='1234' where name <> 'Brussels sprouts'");
      Assert.fail("SQL should not be compiled");
    } catch (SqlCompilationException sce) {
      // expected.
    }

    try {
      CalciteSqlParser.compileToPinotQuery(
          "select * from vegetables where name <> 'Brussels sprouts'; SET (delicious='yes', foo=1234)");
      Assert.fail("SQL should not be compiled");
    } catch (SqlCompilationException sce) {
      // expected.
    }

    try {
      CalciteSqlParser.compileToPinotQuery(
          "select * from vegetables where name <> 'Brussels sprouts'; SET (delicious='yes', foo=1234); select * from "
              + "meat");
      Assert.fail("SQL should not be compiled");
    } catch (SqlCompilationException sce) {
      // expected.
    }
  }

  @Test
  public void testRemoveComments() {
    testRemoveComments("select * from myTable", "select * from myTable");
    testRemoveComments("select * from myTable--hello", "select * from myTable");
    testRemoveComments("select * from myTable--hello\n", "select * from myTable");
    testRemoveComments("select * from--hello\nmyTable", "select * from myTable");
    testRemoveComments("select * from/*hello*/myTable", "select * from myTable");
    testRemoveComments("select * from myTable--", "select * from myTable");
    testRemoveComments("select * from myTable--\n", "select * from myTable");
    testRemoveComments("select * from--\nmyTable", "select * from myTable");
    testRemoveComments("select * from/**/myTable", "select * from myTable");
    testRemoveComments("select * from\nmyTable", "select * from\nmyTable");

    // Mix of single line and multi-line comment indicators
    testRemoveComments("select * from myTable--hello--world", "select * from myTable");
    testRemoveComments("select * from myTable--hello/*world", "select * from myTable");
    testRemoveComments("select * from myTable--hello\n--world", "select * from myTable");
    testRemoveComments("select * from myTable--hello\n/*--world*/", "select * from myTable");
    testRemoveComments("select * from myTable/*hello--world*/", "select * from myTable");
    testRemoveComments("select * from myTable/*hello--\nworld*/", "select * from myTable");
    testRemoveComments("select * from myTable/*hello*/--world", "select * from myTable");
    testRemoveComments("select * from myTable/*hello*/--world\n", "select * from myTable");

    // Comment indicator within quotes
    testRemoveComments("select * from \"myTable--hello\"", "select * from \"myTable--hello\"");
    testRemoveComments("select * from \"myTable/*hello*/\"", "select * from \"myTable/*hello*/\"");
    testRemoveComments("select '--' from myTable", "select '--' from myTable");
    testRemoveComments("select '/*' from myTable", "select '/*' from myTable");
    testRemoveComments("select '/**/' from myTable", "select '/**/' from myTable");
    testRemoveComments("select * from \"my\"\"Table--hello\"", "select * from \"my\"\"Table--hello\"");
    testRemoveComments("select * from \"my\"\"Table/*hello*/\"", "select * from \"my\"\"Table/*hello*/\"");
    testRemoveComments("select '''--' from myTable", "select '''--' from myTable");
    testRemoveComments("select '''/*' from myTable", "select '''/*' from myTable");
    testRemoveComments("select '''/**/' from myTable", "select '''/**/' from myTable");

    // Comment indicator outside of quotes
    testRemoveComments("select * from \"myTable\"--hello", "select * from \"myTable\"");
    testRemoveComments("select * from \"myTable\"/*hello*/", "select * from \"myTable\"");
    testRemoveComments("select ''--from myTable", "select ''");
    testRemoveComments("select ''/**/from myTable", "select '' from myTable");
  }

  private void testRemoveComments(String sqlWithComments, String expectedSqlWithoutComments) {
    PinotQuery commentedResult = CalciteSqlParser.compileToPinotQuery(sqlWithComments);
    PinotQuery expectedResult = CalciteSqlParser.compileToPinotQuery(expectedSqlWithoutComments);
    Assert.assertEquals(commentedResult, expectedResult);
  }

  @Test
  public void testIdentifierQuoteCharacter() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select avg(attributes.age) as avg_age from person group by attributes.address_city");
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
    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT SUM(ADD(foo, 'bar')) FROM myTable GROUP BY sub(foo, bar), SUB(BAR, FOO)");
    selectFunctionList = pinotQuery.getSelectList();
    Assert.assertEquals(selectFunctionList.size(), 1);
    Assert.assertEquals(selectFunctionList.get(0).getFunctionCall().getOperator(), "sum");
    Assert.assertEquals(selectFunctionList.get(0).getFunctionCall().getOperands().size(), 1);
    Assert.assertEquals(
        selectFunctionList.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "add");
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

    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperator(), "sub");
    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperands().size(), 2);
    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "BAR");
    Assert.assertEquals(groupbyList.get(1).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "FOO");
  }

  @Test
  public void testFilterUdf() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("select count(*) from baseballStats where DIV(numberOfGames,10) = 100");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "count");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), FilterKind.EQUALS.name());
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "div");
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
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "count");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), FilterKind.EQUALS.name());
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "timeconvert");
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
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "  select mapKey(mapField,k1) from baseballStats where mapKey(mapField,k1) = 'v1'");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "mapkey");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "mapField");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "k1");

    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), FilterKind.EQUALS.name());
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "mapkey");
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
  public void testTimeTransformFunction() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("  select hour(ts), d1, sum(m1) from baseballStats group by hour(ts), d1");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "hour");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "ts");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getIdentifier().getName(), "d1");
    Assert.assertEquals(pinotQuery.getSelectList().get(2).getFunctionCall().getOperator(), "sum");
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getFunctionCall().getOperator(), "hour");
    Assert.assertEquals(
        pinotQuery.getGroupByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "ts");
    Assert.assertEquals(pinotQuery.getGroupByList().get(1).getIdentifier().getName(), "d1");
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
    Assert.assertEquals(distinctFunction.getOperator(), "distinct");
    Assert.assertEquals(distinctFunction.getOperands().size(), 1);

    Identifier c1 = distinctFunction.getOperands().get(0).getIdentifier();
    Assert.assertEquals(c1.getName(), "c1");

    // test multi column DISTINCT
    sql = "SELECT DISTINCT c1, c2 FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), "distinct");
    Assert.assertEquals(distinctFunction.getOperands().size(), 2);

    c1 = distinctFunction.getOperands().get(0).getIdentifier();
    Identifier c2 = distinctFunction.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "c1");
    Assert.assertEquals(c2.getName(), "c2");

    // test multi column DISTINCT with filter
    sql = "SELECT DISTINCT c1, c2, c3 FROM foo WHERE c3 > 100";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);

    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), "distinct");
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

    // not supported by Calcite SQL (this is in compliance with SQL standard)
    try {
      sql = "SELECT sum(c1), DISTINCT c2 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
    }

    // not supported by Calcite SQL (this is in compliance with SQL standard)
    try {
      sql = "SELECT c1, DISTINCT c2 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
    }

    // not supported by Calcite SQL (this is in compliance with SQL standard)
    try {
      sql = "SELECT DIV(c1,c2), DISTINCT c3 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
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
          "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name after "
              + "DISTINCT keyword"));
    }

    // Pinot currently does not support DISTINCT * syntax
    try {
      sql = "SELECT DISTINCT *, C1 FROM foo";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains(
          "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name after "
              + "DISTINCT keyword"));
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
    Assert.assertEquals(distinctFunction.getOperator(), "distinct");
    Assert.assertEquals(distinctFunction.getOperands().size(), 1);

    Function add = distinctFunction.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(add.getOperator(), "add");
    Assert.assertEquals(add.getOperands().size(), 2);
    c1 = add.getOperands().get(0).getIdentifier();
    c2 = add.getOperands().get(1).getIdentifier();
    Assert.assertEquals(c1.getName(), "col1");
    Assert.assertEquals(c2.getName(), "col2");

    // multi-column distinct with multiple transform functions
    sql = "SELECT DISTINCT add(div(col1, col2), mul(col3, col4)), sub(col3, col4) FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), "distinct");
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

    // multi-column distinct with multiple transform columns and additional identifiers
    sql = "SELECT DISTINCT add(div(col1, col2), mul(col3, col4)), sub(col3, col4), col5, col6 FROM foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    selectListExpressions = pinotQuery.getSelectList();
    Assert.assertEquals(selectListExpressions.size(), 1);
    Assert.assertEquals(selectListExpressions.get(0).getType(), ExpressionType.FUNCTION);

    distinctFunction = selectListExpressions.get(0).getFunctionCall();
    Assert.assertEquals(distinctFunction.getOperator(), "distinct");
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
  }

  @Test
  public void testQueryValidation() {
    // Valid: Selection fields are part of group by identifiers.
    String sql =
        "select group_country, sum(rsvp_count), count(*) from meetupRsvp group by group_city, group_country ORDER BY "
            + "sum(rsvp_count), count(*) limit 50";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 2);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);

    // Invalid: Selection field 'group_city' is not part of group by identifiers.
    try {
      sql = "select group_city, group_country, sum(rsvp_count), count(*) from meetupRsvp group by group_country ORDER "
          + "BY sum(rsvp_count), count(*) limit 50";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains("'group_city' should appear in GROUP BY clause."));
    }

    // Valid groupBy non-aggregate function should pass.
    sql = "select dateConvert(secondsSinceEpoch), sum(rsvp_count), count(*) from meetupRsvp group by dateConvert"
        + "(secondsSinceEpoch) limit 50";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);

    // Invalid: secondsSinceEpoch should be in groupBy clause.
    try {
      sql = "select secondsSinceEpoch, dateConvert(secondsSinceEpoch), sum(rsvp_count), count(*) from meetupRsvp"
          + " group by dateConvert(secondsSinceEpoch) limit 50";
      CalciteSqlParser.compileToPinotQuery(sql);
      Assert.fail("Query should have failed compilation");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertTrue(e.getMessage().contains("'secondsSinceEpoch' should appear in GROUP BY clause."));
    }

    // Invalid groupBy clause shouldn't contain aggregate expression, like sum(rsvp_count), count(*).
    try {
      sql = "select  sum(rsvp_count), count(*) from meetupRsvp group by group_country, sum(rsvp_count), count(*) limit "
          + "50";
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
    sql = "select secondsSinceEpoch, sum(rsvp_count) as sum_rsvp_count, count(*) as cnt from meetupRsvp"
        + " group by secondsSinceEpoch order by cnt, sum_rsvp_count DESC limit 50";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 2);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "count");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperator(), "desc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "sum");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "rsvp_count");

    // Valid mixed alias expressions in query.
    sql = "select secondsSinceEpoch, sum(rsvp_count), count(*) as cnt from meetupRsvp group by secondsSinceEpoch"
        + " order by cnt, sum(rsvp_count) DESC limit 50";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 1);
    Assert.assertEquals(pinotQuery.getOrderByListSize(), 2);
    Assert.assertEquals(pinotQuery.getOrderByList().get(0).getFunctionCall().getOperator(), "asc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "count");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "*");
    Assert.assertEquals(pinotQuery.getOrderByList().get(1).getFunctionCall().getOperator(), "desc");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "sum");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "rsvp_count");

    sql = "select secondsSinceEpoch/86400 AS daysSinceEpoch, sum(rsvp_count) as sum_rsvp_count, count(*) as cnt"
        + " from meetupRsvp where daysSinceEpoch = 18523 group by daysSinceEpoch order by cnt, sum_rsvp_count DESC"
        + " limit 50";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 3);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), FilterKind.EQUALS.name());
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "divide");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "secondsSinceEpoch");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getLongValue(), 86400);
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 18523);
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 1);
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getFunctionCall().getOperator(), "divide");
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
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "C1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "ALIAS_C1");

    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "C2");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "ALIAS_C2");

    Assert.assertEquals(pinotQuery.getSelectList().get(2).getFunctionCall().getOperator(), "add");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "C1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "C2");

    // Invalid groupBy clause shouldn't contain aggregate expression, like sum(rsvp_count), count(*).
    try {
      sql = "SELECT C1 AS ALIAS_C1, C2 AS ALIAS_C2, ADD(alias_c1, alias_c2) FROM Foo";
      CalciteSqlParser.compileToPinotQuery(sql);
    } catch (Exception e) {
      Assert.fail("Query compilation shouldn't fail");
    }
  }

  @Test
  public void testSameAliasInSelection() {
    String sql;
    PinotQuery pinotQuery;
    sql = "SELECT C1 AS C1, C2 AS C2 FROM Foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "C1");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getIdentifier().getName(), "C2");
  }

  @Test
  public void testArithmeticOperator() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery("select a,b+2,c*5,(d+5)*2 from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 4);
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), "plus");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "b");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(2).getFunctionCall().getOperator(), "times");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "c");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(3).getFunctionCall().getOperator(), "times");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "plus");
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
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "plus");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "mod");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "a");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getLiteral().getLongValue(), 200);
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(),
        "times");
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
        "select max(value) as max, min(value) as min, sum(value) as sum, count(*) as count, avg(value) as avg from "
            + "myTable where groups = 'foo'");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "max");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "value");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "max");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "min");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "value");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "min");
    Assert.assertEquals(pinotQuery.getSelectList().get(2).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "sum");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "value");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(2).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "sum");
    Assert.assertEquals(pinotQuery.getSelectList().get(3).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "count");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "*");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(3).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "count");
    Assert.assertEquals(pinotQuery.getSelectList().get(4).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(4).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "avg");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(4).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "value");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(4).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "avg");
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), FilterKind.EQUALS.name());
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getIdentifier().getName(), "groups");
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
      String message = e.getCause().getMessage();
    }
    // date - need to escape
    try {
      CalciteSqlParser.compileToPinotQuery("select count(*) from myTable group by Date");
      Assert.fail("Query should have failed to compile");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
    }

    // timestamp - need to escape
    try {
      CalciteSqlParser.compileToPinotQuery("select count(*) from myTable where timestamp < 1000");
      Assert.fail("Query should have failed to compile");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
    }

    // time - need to escape
    try {
      CalciteSqlParser.compileToPinotQuery("select count(*) from myTable where time > 100");
      Assert.fail("Query should have failed to compile");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
    }

    // group - need to escape
    try {
      CalciteSqlParser.compileToPinotQuery("select group from myTable where bar = 'foo'");
      Assert.fail("Query should have failed to compile");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
    }

    // escaping the above works
    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "select sum(foo) from \"table\" where \"Date\" = 2019 and (\"timestamp\" < 100 or \"time\" > 200) group by "
            + "\"group\"");
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
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getLongValue(), 25);

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST('20170825' AS LONG) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getLiteral().getLongValue(), 20170825);

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST(20170825.0 AS Float) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals((float) pinotQuery.getSelectList().get(0).getLiteral().getDoubleValue(), 20170825.0F);

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST(20170825.0 AS dOuble) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals((float) pinotQuery.getSelectList().get(0).getLiteral().getDoubleValue(), 20170825.0F);

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST(column1 AS STRING) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "cast");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "column1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "STRING");

    pinotQuery = CalciteSqlParser.compileToPinotQuery("SELECT CAST(column1 AS varchar) from myTable");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "cast");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "column1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "VARCHAR");

    pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT SUM(CAST(CAST(ArrTime AS STRING) AS LONG)) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = "
            + "'DL'");
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "sum");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "cast");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "cast");
  }

  @Test
  public void testDistinctCountRewrite() {
    String query = "SELECT count(distinct bar) FROM foo";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT count(distinct bar) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT count(distinct bar), distinctCount(bar) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), "distinctcount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT count(distinct bar), count(*), sum(a),min(a),max(b) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctcount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT count(distinct bar) AS distinct_bar, count(*), sum(a),min(a),max(b) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "distinctcount");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "bar");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "distinct_bar");
  }

  @Test
  public void testDistinctSumRewrite() {
    String query = "SELECT sum(distinct bar) FROM foo";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctsum");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT sum(distinct bar) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctsum");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT sum(distinct bar), distinctSum(bar) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctsum");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), "distinctsum");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT sum(distinct bar), count(*), sum(a),min(a),max(b) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctsum");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT sum(distinct bar) AS distinct_bar, count(*), sum(a),min(a),max(b) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "distinctsum");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "bar");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "distinct_bar");

    query = "SELECT sum(distinct bar) AS distinct_bar, count(*), sum(a),min(a),max(b) FROM foo GROUP BY city ORDER BY "
        + "distinct_bar";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Function selectFunctionCall = pinotQuery.getSelectList().get(0).getFunctionCall();
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(selectFunctionCall.getOperands().get(0).getFunctionCall().getOperator(), "distinctsum");
    Assert.assertEquals(
        selectFunctionCall.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "bar");
    Assert.assertEquals(selectFunctionCall.getOperands().get(1).getIdentifier().getName(), "distinct_bar");
    Assert.assertEquals(pinotQuery.getOrderByList().size(), 1);
    Function orderbyFunctionCall = pinotQuery.getOrderByList().get(0).getFunctionCall();
    Assert.assertEquals(orderbyFunctionCall.getOperator(), "asc");
    Assert.assertEquals(orderbyFunctionCall.getOperands().get(0).getFunctionCall().getOperator(), "distinctsum");
    Assert.assertEquals(
        orderbyFunctionCall.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "bar");
  }

  @Test
  public void testDistinctAvgRewrite() {
    String query = "SELECT avg(distinct bar) FROM foo";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctavg");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT avg(distinct bar) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctavg");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT avg(distinct bar), distinctAvg(bar) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctavg");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    Assert.assertEquals(pinotQuery.getSelectList().get(1).getFunctionCall().getOperator(), "distinctavg");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT avg(distinct bar), count(*), avg(a),min(a),max(b) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "distinctavg");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT avg(distinct bar) AS distinct_bar, count(*), avg(a),min(a),max(b) FROM foo GROUP BY city";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "distinctavg");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "bar");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "distinct_bar");

    query = "SELECT avg(distinct bar) AS distinct_bar, count(*), avg(a),min(a),max(b) FROM foo GROUP BY city ORDER BY"
        + " distinct_bar";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 5);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "distinctavg");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "bar");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(),
        "distinct_bar");
    Assert.assertEquals(pinotQuery.getOrderByList().size(), 1);
    Function orderbyFunctionCall = pinotQuery.getOrderByList().get(0).getFunctionCall();
    Assert.assertEquals(orderbyFunctionCall.getOperator(), "asc");
    Assert.assertEquals(orderbyFunctionCall.getOperands().get(0).getFunctionCall().getOperator(), "distinctavg");
    Assert.assertEquals(
        orderbyFunctionCall.getOperands().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "bar");
  }

  @Test
  public void testInvalidDistinctAggregationRewrite() {
    String query = "SELECT max(distinct bar) FROM foo";
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SqlCompilationException);
      Assert.assertEquals(e.getMessage(), "Function 'max' on DISTINCT is not supported.");
    }
  }

  @Test
  public void testOrdinalsQueryRewrite() {
    String query = "SELECT foo, bar, count(*) FROM t GROUP BY 1, 2 ORDER BY 1, 2 DESC";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "foo");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getIdentifier().getName(), "bar");
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getIdentifier().getName(), "foo");
    Assert.assertEquals(pinotQuery.getGroupByList().get(1).getIdentifier().getName(), "bar");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "foo");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");

    query = "SELECT foo, bar, count(*) FROM t GROUP BY 2, 1 ORDER BY 2, 1 DESC";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "foo");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getIdentifier().getName(), "bar");
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getIdentifier().getName(), "bar");
    Assert.assertEquals(pinotQuery.getGroupByList().get(1).getIdentifier().getName(), "foo");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "foo");

    query = "SELECT foo as f, bar as b, count(*) FROM t GROUP BY 2, 1 ORDER BY 2, 1 DESC";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getIdentifier().getName(), "bar");
    Assert.assertEquals(pinotQuery.getGroupByList().get(1).getIdentifier().getName(), "foo");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "bar");
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "foo");

    query = "select a, b + 2, array_sum(c) as array_sum_c, count(*) from data group by a, 2, array_sum_c";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getIdentifier().getName(), "a");
    Assert.assertEquals(pinotQuery.getGroupByList().get(1).getFunctionCall().getOperator(), "plus");
    Assert.assertEquals(
        pinotQuery.getGroupByList().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "b");
    Assert.assertEquals(
        pinotQuery.getGroupByList().get(1).getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 2L);
    Assert.assertEquals(pinotQuery.getGroupByList().get(2).getFunctionCall().getOperator(), "arraysum");
    Assert.assertEquals(
        pinotQuery.getGroupByList().get(2).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "c");

    Assert.expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT foo, bar, count(*) FROM t GROUP BY 0"));
    Assert.expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT foo, bar, count(*) FROM t GROUP BY 3"));
  }

  @Test
  public void testOrdinalsQueryRewriteWithDistinctOrderBy() {
    String query =
        "SELECT baseballStats.playerName AS playerName FROM baseballStats GROUP BY baseballStats.playerName ORDER BY "
            + "1 ASC";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "baseballStats.playerName");
    Assert.assertNull(pinotQuery.getGroupByList());
    Assert.assertEquals(
        pinotQuery.getOrderByList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "baseballStats.playerName");
  }

  @Test
  public void testNoArgFunction() {
    String query = "SELECT noArgFunc() FROM foo ";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "noargfunc");

    query = "SELECT a FROM foo where time_col > noArgFunc()";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Function greaterThan = pinotQuery.getFilterExpression().getFunctionCall();
    Function minus = greaterThan.getOperands().get(0).getFunctionCall();
    Assert.assertEquals(minus.getOperands().get(1).getFunctionCall().getOperator(), "noargfunc");

    query = "SELECT sum(a), noArgFunc() FROM foo group by noArgFunc()";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getFunctionCall().getOperator(), "noargfunc");
  }

  @Test
  public void testCompilationInvokedFunction() {
    String query = "SELECT now() FROM foo";
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

    query = "SELECT ago('PT1H') FROM foo";
    lowerBound = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    nowTs = pinotQuery.getSelectList().get(0).getLiteral().getLongValue();
    upperBound = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    Assert.assertTrue(nowTs >= lowerBound);
    Assert.assertTrue(nowTs <= upperBound);

    query = "SELECT a FROM foo where time_col > ago('PT1H')";
    lowerBound = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    greaterThan = pinotQuery.getFilterExpression().getFunctionCall();
    nowTs = greaterThan.getOperands().get(1).getLiteral().getLongValue();
    upperBound = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    Assert.assertTrue(nowTs >= lowerBound);
    Assert.assertTrue(nowTs <= upperBound);

    query = "select encodeUrl('key1=value 1&key2=value@!$2&key3=value%3'), "
        + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    String encoded = pinotQuery.getSelectList().get(0).getLiteral().getStringValue();
    String decoded = pinotQuery.getSelectList().get(1).getLiteral().getStringValue();
    Assert.assertEquals(encoded, "key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    Assert.assertEquals(decoded, "key1=value 1&key2=value@!$2&key3=value%3");

    query = "select concat('https://www.google.com/search?q=',"
        + "encodeUrl('key1=val1 key2=45% key3=#47 key4={''key'':[3,5]} + key5=1;2;3;4 key6=(a|b)&c key7= "
        + "key8=5*(6/4) key9=https://pinot@pinot.com key10=CFLAGS=\"-O2 -mcpu=pentiumpro\" key12=$JAVA_HOME'),'') "
        + "from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    encoded = pinotQuery.getSelectList().get(0).getLiteral().getStringValue();
    Assert.assertEquals(encoded, "https://www.google.com/search?q=key1%3Dval1+key2%3D45%25+key3%3D%2347+"
        + "key4%3D%7B%27key%27%3A%5B3%2C5%5D%7D+%2B+key5%3D1%3B2%3B3%3B4+"
        + "key6%3D%28a%7Cb%29%26c+key7%3D+key8%3D5*%286%2F4%29+"
        + "key9%3Dhttps%3A%2F%2Fpinot%40pinot.com+key10%3DCFLAGS%3D%22-O2+-mcpu%3Dpentiumpro%22+key12%3D%24JAVA_HOME");

    query = "select decodeUrl('https://www.google.com/search?q=key1%3Dval1+key2%3D45%25+key3%3D%2347+"
        + "key4%3D%7B%27key%27%3A%5B3%2C5%5D%7D+%2B+key5%3D1%3B2%3B3%3B4+key6%3D%28a%7Cb%29%26c+"
        + "key7%3D+key8%3D5*%286%2F4%29+key9%3Dhttps%3A%2F%2Fpinot%40pinot.com+"
        + "key10%3DCFLAGS%3D%22-O2+-mcpu%3Dpentiumpro%22+key12%3D%24JAVA_HOME') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    decoded = pinotQuery.getSelectList().get(0).getLiteral().getStringValue();
    Assert.assertEquals(decoded, "https://www.google.com/search?q=key1=val1 key2=45% key3=#47 "
        + "key4={'key':[3,5]} + key5=1;2;3;4 key6=(a|b)&c key7= "
        + "key8=5*(6/4) key9=https://pinot@pinot.com key10=CFLAGS=\"-O2 -mcpu=pentiumpro\" key12=$JAVA_HOME");

    query = "select a from mytable where foo=encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') and"
        + " bar=decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253')";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Function and = pinotQuery.getFilterExpression().getFunctionCall();
    encoded = and.getOperands().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue();
    decoded = and.getOperands().get(1).getFunctionCall().getOperands().get(1).getLiteral().getStringValue();
    Assert.assertEquals(encoded, "key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    Assert.assertEquals(decoded, "key1=value 1&key2=value@!$2&key3=value%3");

    query = "select toBase64(toUtf8('hello!')), fromUtf8(fromBase64('aGVsbG8h')) from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    String encodedBase64 = pinotQuery.getSelectList().get(0).getLiteral().getStringValue();
    String decodedBase64 = pinotQuery.getSelectList().get(1).getLiteral().getStringValue();
    Assert.assertEquals(encodedBase64, "aGVsbG8h");
    Assert.assertEquals(decodedBase64, "hello!");

    query = "select toBase64(fromBase64('aGVsbG8h')), fromUtf8(fromBase64(toBase64(toUtf8('hello!')))) from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    encodedBase64 = pinotQuery.getSelectList().get(0).getLiteral().getStringValue();
    decodedBase64 = pinotQuery.getSelectList().get(1).getLiteral().getStringValue();
    Assert.assertEquals(encodedBase64, "aGVsbG8h");
    Assert.assertEquals(decodedBase64, "hello!");

    query = "select toBase64(toUtf8(upper('hello!'))), fromUtf8(fromBase64(toBase64(toUtf8(upper('hello!'))))) from "
        + "mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    encodedBase64 = pinotQuery.getSelectList().get(0).getLiteral().getStringValue();
    decodedBase64 = pinotQuery.getSelectList().get(1).getLiteral().getStringValue();
    Assert.assertEquals(encodedBase64, "SEVMTE8h");
    Assert.assertEquals(decodedBase64, "HELLO!");

    query = "select reverse(fromUtf8(fromBase64(toBase64(toUtf8(upper('hello!')))))) from mytable where "
        + "fromUtf8(fromBase64(toBase64(toUtf8(upper('hello!'))))) = bar";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    String arg1 = pinotQuery.getSelectList().get(0).getLiteral().getStringValue();
    String leftOp =
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getStringValue();
    Assert.assertEquals(arg1, "!OLLEH");
    Assert.assertEquals(leftOp, "HELLO!");

    query = "select a from mytable where foo = toBase64(toUtf8('hello!')) and bar = fromUtf8(fromBase64('aGVsbG8h'))";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    and = pinotQuery.getFilterExpression().getFunctionCall();
    encoded = and.getOperands().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue();
    decoded = and.getOperands().get(1).getFunctionCall().getOperands().get(1).getLiteral().getStringValue();
    Assert.assertEquals(encoded, "aGVsbG8h");
    Assert.assertEquals(decoded, "hello!");

    query = "select fromBase64('hello') from mytable";
    Exception expectedError = null;
    try {
      CalciteSqlParser.compileToPinotQuery(query);
    } catch (Exception e) {
      expectedError = e;
    }
    Assert.assertNotNull(expectedError);
    Assert.assertTrue(expectedError instanceof SqlCompilationException);

    query = "select toBase64('hello!') from mytable";
    expectedError = null;
    try {
      CalciteSqlParser.compileToPinotQuery(query);
    } catch (Exception e) {
      expectedError = e;
    }
    Assert.assertNotNull(expectedError);
    Assert.assertTrue(expectedError instanceof SqlCompilationException);

    query = "select isSubnetOf('192.168.0.1/24', '192.168.0.225') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    boolean result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('192.168.0.1/24', '192.168.0.1') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('130.191.23.32/27', '130.191.23.40') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('130.191.23.32/26', '130.192.23.33') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertFalse(result);

    query = "select isSubnetOf('153.87.199.160/28', '153.87.199.166') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('2001:4800:7825:103::/64', '2001:4800:7825:103::2050') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('130.191.23.32/26', '130.191.23.33') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('2001:4801:7825:103:be76:4efe::/96', '2001:4801:7825:103:be76:4efe::e15') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('122.152.15.0/26', '122.152.15.28') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('96.141.228.254/26', '96.141.228.254') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('3.175.47.128/26', '3.175.48.178') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertFalse(result);

    query = "select isSubnetOf('192.168.0.1/24', '192.168.0.0') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('10.3.128.1/22', '10.3.128.123') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('10.3.128.1/22', '10.3.131.255') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('10.3.128.1/22', '1.2.3.1') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertFalse(result);

    query = "select isSubnetOf('1.2.3.128/1', '127.255.255.255') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('1.2.3.128/0', '192.168.5.1') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('2001:db8:85a3::8a2e:370:7334/62', '2001:0db8:85a3:0003:ffff:ffff:ffff:ffff') from "
        + "mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);

    query = "select isSubnetOf('123:db8:85a3::8a2e:370:7334/72', '124:db8:85a3::8a2e:370:7334') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertFalse(result);

    query = "select isSubnetOf('7890:db8:113::8a2e:370:7334/127', '7890:db8:113::8a2e:370:7336') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertFalse(result);

    query = "select isSubnetOf('7890:db8:113::8a2e:370:7334/127', '7890:db8:113::8a2e:370:7335') from mytable";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    result = pinotQuery.getSelectList().get(0).getLiteral().getBoolValue();
    Assert.assertTrue(result);
  }

  @Test
  public void testCompilationInvokedNestedFunctions() {
    String query = "SELECT a FROM foo where time_col > toDateTime(now(), 'yyyy-MM-dd z')";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Function greaterThan = pinotQuery.getFilterExpression().getFunctionCall();
    String today = greaterThan.getOperands().get(1).getLiteral().getStringValue();
    String expectedTodayStr =
        Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    Assert.assertEquals(today, expectedTodayStr);
  }

  @Test
  public void testCompileTimeExpression() {
    final CompileTimeFunctionsInvoker compileTimeFunctionsInvoker = new CompileTimeFunctionsInvoker();
    long lowerBound = System.currentTimeMillis();
    Expression expression = CalciteSqlParser.compileToExpression("now()");
    Assert.assertNotNull(expression.getFunctionCall());
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    long upperBound = System.currentTimeMillis();
    long result = expression.getLiteral().getLongValue();
    Assert.assertTrue(result >= lowerBound && result <= upperBound);

    lowerBound = TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis()) + 1;
    expression = CalciteSqlParser.compileToExpression("to_epoch_hours(now() + 3600000)");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    upperBound = TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis()) + 1;
    result = expression.getLiteral().getLongValue();
    Assert.assertTrue(result >= lowerBound && result <= upperBound);

    lowerBound = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    expression = CalciteSqlParser.compileToExpression("ago('PT1H')");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    upperBound = System.currentTimeMillis() - ONE_HOUR_IN_MS;
    result = expression.getLiteral().getLongValue();
    Assert.assertTrue(result >= lowerBound && result <= upperBound);

    lowerBound = System.currentTimeMillis() + ONE_HOUR_IN_MS;
    expression = CalciteSqlParser.compileToExpression("ago('PT-1H')");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    upperBound = System.currentTimeMillis() + ONE_HOUR_IN_MS;
    result = expression.getLiteral().getLongValue();
    Assert.assertTrue(result >= lowerBound && result <= upperBound);

    expression = CalciteSqlParser.compileToExpression("toDateTime(millisSinceEpoch)");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getFunctionCall());
    Assert.assertEquals(expression.getFunctionCall().getOperator(), "todatetime");
    Assert.assertEquals(expression.getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "millisSinceEpoch");

    expression = CalciteSqlParser.compileToExpression("encodeUrl('key1=value 1&key2=value@!$2&key3=value%3')");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    Assert.assertEquals(expression.getLiteral().getFieldValue(),
        "key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");

    expression =
        CalciteSqlParser.compileToExpression("decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253')");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    Assert.assertEquals(expression.getLiteral().getFieldValue(), "key1=value 1&key2=value@!$2&key3=value%3");

    expression = CalciteSqlParser.compileToExpression("reverse(playerName)");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getFunctionCall());
    Assert.assertEquals(expression.getFunctionCall().getOperator(), "reverse");
    Assert.assertEquals(expression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "playerName");

    expression = CalciteSqlParser.compileToExpression("reverse('playerName')");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    Assert.assertEquals(expression.getLiteral().getFieldValue(), "emaNreyalp");

    expression = CalciteSqlParser.compileToExpression("reverse(123)");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    Assert.assertEquals(expression.getLiteral().getFieldValue(), "321");

    expression = CalciteSqlParser.compileToExpression("count(*)");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getFunctionCall());
    Assert.assertEquals(expression.getFunctionCall().getOperator(), "count");
    Assert.assertEquals(expression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "*");

    expression = CalciteSqlParser.compileToExpression("toBase64(toUtf8('hello!'))");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    Assert.assertEquals(expression.getLiteral().getFieldValue(), "aGVsbG8h");

    expression = CalciteSqlParser.compileToExpression("fromUtf8(fromBase64('aGVsbG8h'))");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getLiteral());
    Assert.assertEquals(expression.getLiteral().getFieldValue(), "hello!");

    expression = CalciteSqlParser.compileToExpression("fromBase64(foo)");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getFunctionCall());
    Assert.assertEquals(expression.getFunctionCall().getOperator(), "frombase64");
    Assert.assertEquals(expression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "foo");

    expression = CalciteSqlParser.compileToExpression("toBase64(foo)");
    Assert.assertNotNull(expression.getFunctionCall());
    pinotQuery.setFilterExpression(expression);
    pinotQuery = compileTimeFunctionsInvoker.rewrite(pinotQuery);
    expression = pinotQuery.getFilterExpression();
    Assert.assertNotNull(expression.getFunctionCall());
    Assert.assertEquals(expression.getFunctionCall().getOperator(), "tobase64");
    Assert.assertEquals(expression.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "foo");
  }

  @Test
  public void testLiteralExpressionCheck() {
    Assert.assertTrue(CalciteSqlParser.isLiteralOnlyExpression(CalciteSqlParser.compileToExpression("1123")));
    Assert.assertTrue(CalciteSqlParser.isLiteralOnlyExpression(CalciteSqlParser.compileToExpression("'ab'")));
    Assert.assertTrue(
        CalciteSqlParser.isLiteralOnlyExpression(CalciteSqlParser.compileToExpression("AS('ab', randomStr)")));
    Assert.assertTrue(
        CalciteSqlParser.isLiteralOnlyExpression(CalciteSqlParser.compileToExpression("AS(123, randomTime)")));
    Assert.assertFalse(CalciteSqlParser.isLiteralOnlyExpression(CalciteSqlParser.compileToExpression("sum(abc)")));
    Assert.assertFalse(CalciteSqlParser.isLiteralOnlyExpression(CalciteSqlParser.compileToExpression("count(*)")));
    Assert.assertFalse(CalciteSqlParser.isLiteralOnlyExpression(CalciteSqlParser.compileToExpression("a+B")));
    Assert.assertFalse(CalciteSqlParser.isLiteralOnlyExpression(CalciteSqlParser.compileToExpression("c+1")));
  }

  @Test
  public void testCaseInsensitiveFilter() {
    String query = "SELECT count(*) FROM foo where text_match(col, 'expr')";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "TEXT_MATCH");

    query = "SELECT count(*) FROM foo where TEXT_MATCH(col, 'expr')";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "TEXT_MATCH");

    query = "SELECT count(*) FROM foo where regexp_like(col, 'expr')";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "REGEXP_LIKE");

    query = "SELECT count(*) FROM foo where REGEXP_LIKE(col, 'expr')";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "REGEXP_LIKE");

    query = "SELECT count(*) FROM foo where col is not null";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "IS_NOT_NULL");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col");

    query = "SELECT count(*) FROM foo where col IS NOT NULL";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "IS_NOT_NULL");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col");

    query = "SELECT count(*) FROM foo where col is null";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "IS_NULL");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col");

    query = "SELECT count(*) FROM foo where col IS NULL";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "IS_NULL");
    Assert.assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col");
  }

  @Test
  public void testNonAggregationGroupByQuery() {
    String query = "SELECT col1 FROM foo GROUP BY col1";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator().toUpperCase(), "DISTINCT");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");

    query = "SELECT col1, col2 FROM foo GROUP BY col1, col2";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator().toUpperCase(), "DISTINCT");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "col2");

    query = "SELECT col1+col2*5 FROM foo GROUP BY col1+col2*5";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator().toUpperCase(), "DISTINCT");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "plus");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "col1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getFunctionCall().getOperator(), "times");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col2");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 5L);

    query = "SELECT col1+col2*5 AS col3 FROM foo GROUP BY col3";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator().toUpperCase(), "DISTINCT");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "as");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "col3");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "plus");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(), "times");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "col2");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1).getLiteral().getLongValue(),
        5L);
  }

  @Test
  public void testInvalidNonAggregationGroupBy() {
    Assert.assertThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT col1 FROM foo GROUP BY col1, col2"));
    Assert.assertThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT col1, col2 FROM foo GROUP BY col1"));
    Assert.assertThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT col1 + col2 FROM foo GROUP BY col1"));
    Assert.assertThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT col1+col2 FROM foo GROUP BY col1,col2"));
  }

  @Test
  public void testFlattenAndOr() {
    {
      String query = "SELECT * FROM foo WHERE col1 > 0 AND (col2 > 0 AND col3 > 0) AND col4 > 0";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Function functionCall = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.AND.name());
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 4);
      for (Expression operand : operands) {
        Assert.assertEquals(operand.getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
      }
    }
    {
      String query = "SELECT * FROM foo WHERE col1 > 0 AND (col2 AND col3 > 0) AND startsWith(col4, 'myStr')";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Function functionCall = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.AND.name());
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 4);
      Assert.assertEquals(operands.get(0).getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
      Assert.assertEquals(operands.get(1).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      List<Expression> eqOperands = operands.get(1).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getIdentifier(), new Identifier("col2"));
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));
      Assert.assertEquals(operands.get(2).getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
      Assert.assertEquals(operands.get(3).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      eqOperands = operands.get(3).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getFunctionCall().getOperator(), "startswith");
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));
    }
    {
      String query = "SELECT * FROM foo WHERE col1 > 0 AND (col2 AND col3 > 0) AND col4 = true";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Function functionCall = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.AND.name());
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 4);
      Assert.assertEquals(operands.get(0).getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
      Assert.assertEquals(operands.get(1).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      List<Expression> eqOperands = operands.get(1).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getIdentifier(), new Identifier("col2"));
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));
      Assert.assertEquals(operands.get(2).getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
      Assert.assertEquals(operands.get(3).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      eqOperands = operands.get(3).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getIdentifier(), new Identifier("col4"));
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));
    }
    {
      String query = "SELECT * FROM foo WHERE col1 <= 0 OR col2 <= 0 OR (col3 <= 0 OR col4 <= 0)";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Function functionCall = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.OR.name());
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 4);
      for (Expression operand : operands) {
        Assert.assertEquals(operand.getFunctionCall().getOperator(), FilterKind.LESS_THAN_OR_EQUAL.name());
      }
    }
    {
      String query = "SELECT * FROM foo WHERE col1 <= 0 OR col2 OR (col3 <= 0 OR col4)";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Function functionCall = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.OR.name());
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 4);
      Assert.assertEquals(operands.get(0).getFunctionCall().getOperator(), FilterKind.LESS_THAN_OR_EQUAL.name());
      Assert.assertEquals(operands.get(1).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      Assert.assertEquals(operands.get(2).getFunctionCall().getOperator(), FilterKind.LESS_THAN_OR_EQUAL.name());
      Assert.assertEquals(operands.get(3).getFunctionCall().getOperator(), FilterKind.EQUALS.name());
      List<Expression> eqOperands = operands.get(3).getFunctionCall().getOperands();
      Assert.assertEquals(eqOperands.get(0).getIdentifier(), new Identifier("col4"));
      Assert.assertEquals(eqOperands.get(1).getLiteral(), Literal.boolValue(true));
    }
    {
      String query = "SELECT * FROM foo WHERE col1 > 0 AND ((col2 > 0 AND col3 > 0) AND (col1 <= 0 OR (col2 <= 0 OR "
          + "(col3 <= 0 OR col4 <= 0) OR (col3 > 0 AND col4 > 0))))";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Function functionCall = pinotQuery.getFilterExpression().getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.AND.name());
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 4);
      for (int i = 0; i < 3; i++) {
        Assert.assertEquals(operands.get(i).getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
      }
      functionCall = operands.get(3).getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.OR.name());
      operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 5);
      for (int i = 0; i < 4; i++) {
        Assert.assertEquals(operands.get(i).getFunctionCall().getOperator(), FilterKind.LESS_THAN_OR_EQUAL.name());
      }
      functionCall = operands.get(4).getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.AND.name());
      operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 2);
      for (Expression operand : operands) {
        Assert.assertEquals(operand.getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
      }
    }
  }

  @Test
  public void testHavingClause() {
    {
      String query = "SELECT SUM(col1), col2 FROM foo WHERE true GROUP BY col2 HAVING SUM(col1) > 10";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Function functionCall = pinotQuery.getHavingExpression().getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.GREATER_THAN.name());
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 2);
      Assert.assertEquals(operands.get(0).getFunctionCall().getOperator(), "sum");
      Assert.assertEquals(operands.get(1).getLiteral().getFieldValue().toString(), "10");
    }
    {
      String query = "SELECT SUM(col1), col2 FROM foo WHERE true GROUP BY col2 "
          + "HAVING SUM(col1) > 10 AND SUM(col3) > 5 AND SUM(col4) > 15";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Function functionCall = pinotQuery.getHavingExpression().getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.AND.name());
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 3);
      for (Expression operand : operands) {
        Assert.assertEquals(operand.getFunctionCall().getOperator(), FilterKind.GREATER_THAN.name());
      }
    }
  }

  @Test
  public void testPostAggregation() {
    {
      String query = "SELECT SUM(col1) * SUM(col2) FROM foo";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      List<Expression> selectList = pinotQuery.getSelectList();
      Assert.assertEquals(selectList.size(), 1);
      Function functionCall = selectList.get(0).getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), "times");
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 2);
      for (Expression operand : operands) {
        Assert.assertEquals(operand.getFunctionCall().getOperator(), "sum");
      }
    }
    {
      String query = "SELECT SUM(col1), col2 FROM foo GROUP BY col2 ORDER BY MAX(col1) - MAX(col3)";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      List<Expression> orderByList = pinotQuery.getOrderByList();
      Assert.assertEquals(orderByList.size(), 1);
      Function functionCall = orderByList.get(0).getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), "asc");
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 1);
      functionCall = operands.get(0).getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), "minus");
      operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 2);
      for (Expression operand : operands) {
        Assert.assertEquals(operand.getFunctionCall().getOperator(), "max");
      }
    }
    {
      // Having will be rewritten to (SUM(col1) + SUM(col3)) - MAX(col4) > 0
      String query = "SELECT SUM(col1), col2 FROM foo GROUP BY col2 HAVING SUM(col1) + SUM(col3) > MAX(col4)";
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Function functionCall = pinotQuery.getHavingExpression().getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), FilterKind.GREATER_THAN.name());
      List<Expression> operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 2);
      Assert.assertEquals(operands.get(1).getLiteral().getFieldValue().toString(), "0");
      functionCall = operands.get(0).getFunctionCall();
      Assert.assertEquals(functionCall.getOperator(), "minus");
      operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 2);
      Assert.assertEquals(operands.get(1).getFunctionCall().getOperator(), "max");
      functionCall = operands.get(0).getFunctionCall();
      operands = functionCall.getOperands();
      Assert.assertEquals(operands.size(), 2);
      for (Expression operand : operands) {
        Assert.assertEquals(operand.getFunctionCall().getOperator(), "sum");
      }
    }
  }

  @Test
  public void testArrayAggregationRewrite() {
    String sql;
    PinotQuery pinotQuery;
    sql = "SELECT sum(array_sum(a)) FROM Foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "summv");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().size(), 1);
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "a");

    sql = "SELECT MIN(ARRAYMIN(a)) FROM Foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "minmv");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().size(), 1);
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "a");

    sql = "SELECT Max(ArrayMax(a)) FROM Foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "maxmv");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().size(), 1);
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "a");

    sql = "SELECT Max(ArrayMax(a)) + 1 FROM Foo";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperator(), "plus");
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().size(), 2);
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "maxmv");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().size(),
        1);
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "a");
    Assert.assertEquals(
        pinotQuery.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getLongValue(), 1L);
  }

  /**
   * This test shows that Calcite {@link SqlNumericLiteral#isInteger()} throws NPE. The issue has been fixed in
   * Calcite through CALCITE-4199 (https://issues.apache.org/jira/browse/CALCITE-4199), but has not made it into a
   * release yet.
   */
  @Test
  public void testSqlNumericalLiteralisIntegerNPE() {
    CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable WHERE floatColumn > " + Double.MAX_VALUE);
  }

  @Test
  public void testUnsupportedDistinctQueries() {
    String sql = "SELECT DISTINCT col1, col2 FROM foo GROUP BY col1";
    testUnsupportedDistinctQuery(sql, "DISTINCT with GROUP BY is not supported");

    sql = "SELECT DISTINCT col1, col2 FROM foo LIMIT 0";
    testUnsupportedDistinctQuery(sql, "DISTINCT must have positive LIMIT");

    sql = "SELECT DISTINCT col1, col2 FROM foo ORDER BY col3";
    testUnsupportedDistinctQuery(sql, "ORDER-BY columns should be included in the DISTINCT columns");

    sql = "SELECT DISTINCT add(col1, sub(col2, 3)), mod(col2, 10), div(col4, mult(col5, 5)) FROM foo ORDER BY col1, "
        + "col2, col3";
    testUnsupportedDistinctQuery(sql, "ORDER-BY columns should be included in the DISTINCT columns");

    sql = "SELECT DISTINCT add(col1, sub(col2, 3)), mod(col2, 10), div(col4, mult(col5, 5)) FROM foo ORDER BY col1, mod"
        + "(col2, 10)";
    testUnsupportedDistinctQuery(sql, "ORDER-BY columns should be included in the DISTINCT columns");
  }

  @Test
  public void testSupportedDistinctQueries() {
    String sql = "SELECT DISTINCT col1, col2 FROM foo ORDER BY col1, col2";
    testSupportedDistinctQuery(sql);

    sql = "SELECT DISTINCT col1, col2 FROM foo ORDER BY col2, col1";
    testSupportedDistinctQuery(sql);

    sql = "SELECT DISTINCT col1, col2 FROM foo ORDER BY col1 DESC, col2";
    testSupportedDistinctQuery(sql);

    sql = "SELECT DISTINCT col1, col2 FROM foo ORDER BY col1, col2 DESC";
    testSupportedDistinctQuery(sql);

    sql = "SELECT DISTINCT col1, col2 FROM foo ORDER BY col1 DESC, col2 DESC";
    testSupportedDistinctQuery(sql);

    sql = "SELECT DISTINCT add(col1, sub(col2, 3)), mod(col2, 10), div(col4, mult(col5, 5)) FROM foo ORDER BY add(col1,"
        + " sub(col2, 3))";
    testSupportedDistinctQuery(sql);

    sql = "SELECT DISTINCT add(col1, sub(col2, 3)), mod(col2, 10), div(col4, mult(col5, 5)) FROM foo ORDER BY mod(col2,"
        + " 10), add(col1, sub(col2, 3))";
    testSupportedDistinctQuery(sql);

    sql = "SELECT DISTINCT add(col1, sub(col2, 3)), mod(col2, 10), div(col4, mult(col5, 5)) FROM foo ORDER BY"
        + " add(col1, sub(col2, 3)), mod(col2, 10), div(col4, mult(col5, 5)) DESC";
    testSupportedDistinctQuery(sql);
  }

  private void testUnsupportedDistinctQuery(String query, String errorMessage) {
    try {
      PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
      Assert.fail("Query should have failed");
    } catch (Exception e) {
      Assert.assertEquals(errorMessage, e.getMessage());
    }
  }

  private void testSupportedDistinctQuery(String query) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Assert.assertNotNull(pinotQuery);
  }

  @Test
  public void testQueryWithSemicolon() {
    String sql;
    PinotQuery pinotQuery;
    sql = "SELECT col1, col2 FROM foo;";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "col1");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getIdentifier().getName(), "col2");

    // Query having extra white spaces before the semicolon
    sql = "SELECT col1, col2 FROM foo                 ;";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "col1");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getIdentifier().getName(), "col2");

    // Query having leading and trailing whitespaces
    sql = "         SELECT col1, col2 FROM foo;             ";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "col1");
    Assert.assertEquals(pinotQuery.getSelectList().get(1).getIdentifier().getName(), "col2");

    sql = "SELECT col1, count(*) FROM foo group by col1;";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getSelectListSize(), 2);
    Assert.assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "col1");
    Assert.assertEquals(pinotQuery.getGroupByListSize(), 1);
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getIdentifier().getName(), "col1");
    Assert.assertEquals(pinotQuery.getGroupByList().get(0).getIdentifier().getName(), "col1");

    // Check for Option SQL Query
    // TODO: change to SET syntax
    sql = "SELECT col1, count(*) FROM foo group by col1 option(skipUpsert=true);";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 1);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("skipUpsert"));

    // Check for the query where the literal has semicolon
    // TODO: change to SET syntax
    sql = "select col1, count(*) from foo where col1 = 'x;y' GROUP BY col1 option(skipUpsert=true);";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);
    Assert.assertEquals(pinotQuery.getQueryOptionsSize(), 1);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey("skipUpsert"));
  }

  @Test
  public void testCatalogNameResolvedToDefault() {
    // Pinot doesn't support catalog. However, for backward compatibility, if a catalog is provided, we will resolve
    // the table from our default catalog. this means `a.foo` will be equivalent to `foo`.
    PinotQuery randomCatalogQuery = CalciteSqlParser.compileToPinotQuery("SELECT count(*) FROM rand_catalog.foo");
    PinotQuery defaultCatalogQuery = CalciteSqlParser.compileToPinotQuery("SELECT count(*) FROM default.foo");
    Assert.assertEquals(randomCatalogQuery.getDataSource().getTableName(), "rand_catalog.foo");
    Assert.assertEquals(defaultCatalogQuery.getDataSource().getTableName(), "default.foo");
  }

  @Test
  public void testInvalidQueryWithSemicolon() {
    Assert.expectThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(";"));

    Assert.expectThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(";;;;"));

    Assert.expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT col1, count(*) FROM foo GROUP BY ; col1"));

    // Query having multiple SQL statements
    Assert.expectThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(
        "SELECT col1, count(*) FROM foo GROUP BY col1; SELECT col2, count(*) FROM foo GROUP BY col2"));

    // Query having multiple SQL statements with trailing and leading whitespaces
    Assert.expectThrows(SqlCompilationException.class, () -> CalciteSqlParser.compileToPinotQuery(
        "        SELECT col1, count(*) FROM foo GROUP BY col1;   "
            + "SELECT col2, count(*) FROM foo GROUP BY col2             "));
  }

  @Test
  public void testInvalidQueryWithAggregateFunction() {
    Assert.expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT col1, count(*) from foo"));

    Assert.expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT UPPER(col1), count(*) from foo"));

    Assert.expectThrows(SqlCompilationException.class,
        () -> CalciteSqlParser.compileToPinotQuery("SELECT UPPER(col1), avg(col2) from foo"));
  }

  /**
   * Test for customized components in src/main/codegen/parserImpls.ftl file.
   */
  @Test
  public void testParserExtensionImpl()
      throws Exception {
    String customSql = "INSERT INTO db.tbl FROM FILE 'file:///tmp/file1', FILE 'file:///tmp/file2'";
    SqlNodeAndOptions sqlNodeAndOptions = testSqlWithCustomSqlParser(customSql);
    Assert.assertTrue(sqlNodeAndOptions.getSqlNode() instanceof SqlInsertFromFile);
    Assert.assertEquals(sqlNodeAndOptions.getSqlType(), PinotSqlType.DML);
  }

  private static SqlNodeAndOptions testSqlWithCustomSqlParser(String sqlString)
      throws Exception {
    try (StringReader inStream = new StringReader(sqlString)) {
      SqlParserImpl sqlParser = CalciteSqlParser.newSqlParser(inStream);
      SqlNodeList sqlNodeList = sqlParser.SqlStmtsEof();
      // Extract OPTION statements from sql.
      return CalciteSqlParser.extractSqlNodeAndOptions(sqlString, sqlNodeList);
    }
  }

  @Test
  public void shouldParseBasicAtTimeZoneExtension() {
    // Given:
    String sql = "SELECT ts AT TIME ZONE 'pst' FROM myTable;";

    // When:
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);

    // Then:
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Function fun = pinotQuery.getSelectList().get(0).getFunctionCall();
    Assert.assertEquals(fun.operator, "attimezone");
    Assert.assertEquals(fun.operands.size(), 2);
    Assert.assertEquals(fun.operands.get(0).getIdentifier().name, "ts");
    Assert.assertEquals(fun.operands.get(1).getLiteral().getStringValue(), "pst");
  }

  @Test
  public void shouldParseNestedTimeExprAtTimeZoneExtension() {
    // Given:
    String sql = "SELECT ts + 123 AT TIME ZONE 'pst' FROM myTable;";

    // When:
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);

    // Then:
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Function fun = pinotQuery.getSelectList().get(0).getFunctionCall();
    Assert.assertEquals(fun.operator, "attimezone");
    Assert.assertEquals(fun.operands.size(), 2);
    Assert.assertEquals(fun.operands.get(0).getFunctionCall().operator, "plus");
    Assert.assertEquals(fun.operands.get(0).getFunctionCall().operands.size(), 2);
    Assert.assertEquals(fun.operands.get(0).getFunctionCall().operands.get(0).getIdentifier().getName(), "ts");
    Assert.assertEquals(fun.operands.get(0).getFunctionCall().operands.get(1).getLiteral().getLongValue(), 123L);
    Assert.assertEquals(fun.operands.get(1).getLiteral().getStringValue(), "pst");
  }

  @Test
  public void shouldParseOutsideExprAtTimeZoneExtension() {
    // Given:
    String sql = "SELECT ts AT TIME ZONE 'pst' > 123 FROM myTable;";

    // When:
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(sql);

    // Then:
    Assert.assertEquals(pinotQuery.getSelectListSize(), 1);
    Function fun = pinotQuery.getSelectList().get(0).getFunctionCall();
    Assert.assertEquals(fun.operator, "GREATER_THAN");
    Assert.assertEquals(fun.operands.size(), 2);
    Assert.assertEquals(fun.operands.get(0).getFunctionCall().operator, "attimezone");
    Assert.assertEquals(fun.operands.get(0).getFunctionCall().operands.size(), 2);
    Assert.assertEquals(fun.operands.get(0).getFunctionCall().operands.get(0).getIdentifier().getName(), "ts");
    Assert.assertEquals(fun.operands.get(0).getFunctionCall().operands.get(1).getLiteral().getStringValue(), "pst");
  }

  @Test
  public void testJoin() {
    String query = "SELECT T1.a, T2.b FROM T1 JOIN T2";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    DataSource dataSource = pinotQuery.getDataSource();
    Assert.assertNull(dataSource.getTableName());
    Assert.assertNull(dataSource.getSubquery());
    Assert.assertNotNull(dataSource.getJoin());
    Join join = dataSource.getJoin();
    Assert.assertEquals(join.getType(), JoinType.INNER);
    Assert.assertEquals(join.getLeft().getTableName(), "T1");
    Assert.assertEquals(join.getRight().getTableName(), "T2");
    Assert.assertNull(join.getCondition());

    query = "SELECT T1.a, T2.b FROM T1 INNER JOIN T2 ON T1.key = T2.key";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    dataSource = pinotQuery.getDataSource();
    Assert.assertNull(dataSource.getTableName());
    Assert.assertNull(dataSource.getSubquery());
    Assert.assertNotNull(dataSource.getJoin());
    join = dataSource.getJoin();
    Assert.assertEquals(join.getType(), JoinType.INNER);
    Assert.assertEquals(join.getLeft().getTableName(), "T1");
    Assert.assertEquals(join.getRight().getTableName(), "T2");
    Assert.assertEquals(join.getCondition(), CalciteSqlParser.compileToExpression("T1.key = T2.key"));

    query = "SELECT T1.a, T2.b FROM T1 FULL JOIN T2 ON T1.key = T2.key";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    dataSource = pinotQuery.getDataSource();
    Assert.assertNull(dataSource.getTableName());
    Assert.assertNull(dataSource.getSubquery());
    Assert.assertNotNull(dataSource.getJoin());
    join = dataSource.getJoin();
    Assert.assertEquals(join.getType(), JoinType.FULL);
    Assert.assertEquals(join.getLeft().getTableName(), "T1");
    Assert.assertEquals(join.getRight().getTableName(), "T2");
    Assert.assertEquals(join.getCondition(), CalciteSqlParser.compileToExpression("T1.key = T2.key"));

    query = "SELECT T1.a, T2.b FROM T1 LEFT JOIN T2 ON T1.a > T2.b";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    dataSource = pinotQuery.getDataSource();
    Assert.assertNull(dataSource.getTableName());
    Assert.assertNull(dataSource.getSubquery());
    Assert.assertNotNull(dataSource.getJoin());
    join = dataSource.getJoin();
    Assert.assertEquals(join.getType(), JoinType.LEFT);
    Assert.assertEquals(join.getLeft().getTableName(), "T1");
    Assert.assertEquals(join.getRight().getTableName(), "T2");
    Assert.assertEquals(join.getCondition(), CalciteSqlParser.compileToExpression("T1.a > T2.b"));

    query =
        "SELECT T1.a, T2.b FROM T1 RIGHT JOIN (SELECT a, COUNT(*) AS b FROM T3 GROUP BY a) AS T2 ON T1.key = T2.key";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    dataSource = pinotQuery.getDataSource();
    Assert.assertNull(dataSource.getTableName());
    Assert.assertNull(dataSource.getSubquery());
    Assert.assertNotNull(dataSource.getJoin());
    join = dataSource.getJoin();
    Assert.assertEquals(join.getType(), JoinType.RIGHT);
    Assert.assertEquals(join.getLeft().getTableName(), "T1");
    DataSource right = join.getRight();
    Assert.assertEquals(right.getTableName(), "T2");
    PinotQuery rightSubquery = right.getSubquery();
    Assert.assertEquals(rightSubquery,
        CalciteSqlParser.compileToPinotQuery("SELECT a, COUNT(*) AS b FROM T3 GROUP BY a"));
    Assert.assertEquals(join.getCondition(), CalciteSqlParser.compileToExpression("T1.key = T2.key"));

    query = "SELECT T1.a, T2.b FROM T1 JOIN (SELECT key, COUNT(*) AS b FROM T3 JOIN T4 GROUP BY key) AS T2 "
        + "ON T1.key = T2.key";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    dataSource = pinotQuery.getDataSource();
    Assert.assertNull(dataSource.getTableName());
    Assert.assertNull(dataSource.getSubquery());
    Assert.assertNotNull(dataSource.getJoin());
    join = dataSource.getJoin();
    Assert.assertEquals(join.getType(), JoinType.INNER);
    Assert.assertEquals(join.getLeft().getTableName(), "T1");
    right = join.getRight();
    Assert.assertEquals(right.getTableName(), "T2");
    rightSubquery = right.getSubquery();
    Assert.assertEquals(rightSubquery,
        CalciteSqlParser.compileToPinotQuery("SELECT key, COUNT(*) AS b FROM T3 JOIN T4 GROUP BY key"));
    Assert.assertEquals(join.getCondition(), CalciteSqlParser.compileToExpression("T1.key = T2.key"));

    // test for self join queries.
    query = "SELECT T1.a FROM T1 JOIN(SELECT key FROM T1) as self ON T1.key=self.key";
    pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    dataSource = pinotQuery.getDataSource();
    Assert.assertNull(dataSource.getTableName());
    Assert.assertNull(dataSource.getSubquery());
    Assert.assertNotNull(dataSource.getJoin());
    join = dataSource.getJoin();
    Assert.assertEquals(join.getType(), JoinType.INNER);
    Assert.assertEquals(join.getLeft().getTableName(), "T1");
    right = join.getRight();
    Assert.assertEquals(right.getTableName(), "self");
    rightSubquery = right.getSubquery();
    Assert.assertEquals(rightSubquery,
        CalciteSqlParser.compileToPinotQuery("SELECT key FROM T1"));
    Assert.assertEquals(join.getCondition(), CalciteSqlParser.compileToExpression("T1.key = self.key"));
  }
}
