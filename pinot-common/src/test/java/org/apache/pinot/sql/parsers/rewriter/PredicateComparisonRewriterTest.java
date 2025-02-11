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
package org.apache.pinot.sql.parsers.rewriter;

import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.assertTrue;


public class PredicateComparisonRewriterTest {
  PredicateComparisonRewriter _predicateComparisonRewriter = new PredicateComparisonRewriter();

  @Test
  public void testIdentifierPredicateRewrite() {
    // A query like "select * from table where col1" should be rewritten to "select * from table where col1 = true"

    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQueryWithoutRewrites("SELECT * FROM mytable WHERE col1");
    assertTrue(pinotQuery.getFilterExpression().isSetIdentifier());

    PinotQuery rewrittenQuery = _predicateComparisonRewriter.rewrite(pinotQuery);
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().size(), 2);
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "col1");
    Assert.assertTrue(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getBoolValue());
    assertTrue(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getLiteral().getBoolValue());
  }

  @Test
  public void testFunctionPredicateRewrite() {
    // A query like "select col1 from table where startsWith(col1, 'myStr') AND col2 > 10" should be rewritten to
    // "select col1 from table where startsWith(col1, 'myStr') = true AND col2 > 10"

    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT col1 FROM mytable WHERE startsWith(col1, 'myStr') AND col2 > 10;");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "AND");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().size(), 2);
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "startswith");

    PinotQuery rewrittenQuery = _predicateComparisonRewriter.rewrite(pinotQuery);
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperator(), "AND");
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().size(), 2);
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "EQUALS");
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .size(), 2);
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .get(0).getFunctionCall().getOperator(), "startswith");
    assertTrue(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .get(1).getLiteral().getBoolValue());
  }

  @Test
  public void testFilterPredicateLiteralIdentifierSwap() {
    // Filters like '10 = col1' should be rewritten to 'col1 = 10'

    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQueryWithoutRewrites("SELECT * FROM mytable WHERE 10 = col1 OR 10 < col2;");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "OR");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().size(), 2);
    assertTrue(pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).isSetFunctionCall());
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "EQUALS");
    assertTrue(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .isSetLiteral());
    assertTrue(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .isSetIdentifier());

    PinotQuery rewrittenQuery = _predicateComparisonRewriter.rewrite(pinotQuery);
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperator(), "OR");
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().size(), 2);

    assertTrue(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .isSetIdentifier());
    assertTrue(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .isSetLiteral());
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "EQUALS");
    assertTrue(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .isSetIdentifier());
    assertTrue(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .isSetLiteral());
    // Operator should be flipped
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(),
        "GREATER_THAN");
  }

  @Test
  public void testFilterPredicateColumnComparisonRewrite() {
    // Filters like 'col1 = col2' should be rewritten to 'col1 - col2 = 0'

    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQueryWithoutRewrites("SELECT * FROM mytable WHERE col1 = col2 AND col3 < col4;");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "AND");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().size(), 2);
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "EQUALS");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getIdentifier().getName(), "col1");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(1)
            .getIdentifier().getName(), "col2");

    PinotQuery rewrittenQuery = _predicateComparisonRewriter.rewrite(pinotQuery);
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperator(), "AND");
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().size(), 2);
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "EQUALS");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "minus");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(1).getIdentifier().getName(), "col2");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .getLiteral().getIntValue(), 0);
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(),
        "LESS_THAN");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperator(), "minus");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col3");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(0)
            .getFunctionCall().getOperands().get(1).getIdentifier().getName(), "col4");
    assertEquals(
        pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands().get(1)
            .getLiteral().getIntValue(), 0);

    PinotQuery betweenQuery =
        CalciteSqlParser.compileToPinotQueryWithoutRewrites("SELECT * FROM mytable WHERE col1 BETWEEN col2 AND col3");
    assertThrows(SqlCompilationException.class, () -> _predicateComparisonRewriter.rewrite(betweenQuery));
  }
}
