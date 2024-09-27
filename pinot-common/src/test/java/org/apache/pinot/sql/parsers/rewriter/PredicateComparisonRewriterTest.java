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
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
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

    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT * FROM mytable WHERE 10 = col1 AND 10 < col2 AND 10 > col3 AND 10 <= col4 AND 10 >= col5 AND 10 != "
            + "col6;");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "AND");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().size(), 6);
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
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperator(), "AND");
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().size(), 6);

    // Ensure that all filter predicates have been rewritten with LHS as identifier and RHS as literal
    for (int i = 0; i < 5; i++) {
      assertTrue(
          pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(i).getFunctionCall().getOperands().get(0)
              .isSetIdentifier());
      assertTrue(
          pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(i).getFunctionCall().getOperands().get(1)
              .isSetLiteral());
    }
  }

  @Test
  public void testBetweenRewrite() {
    // Filters like 'col1 BETWEEN col2 AND 20' should be rewritten to 'col1 - col2 >= 0 AND col1 <= 20'

    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQueryWithoutRewrites("SELECT * FROM mytable WHERE col1 BETWEEN col2 AND 20;");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "BETWEEN");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().size(), 3);
    assertTrue(pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).isSetIdentifier());
    assertTrue(pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(1).isSetIdentifier());
    assertTrue(pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(2).isSetLiteral());

    PinotQuery rewrittenQuery = _predicateComparisonRewriter.rewrite(pinotQuery);
    assertTrue(rewrittenQuery.getFilterExpression().isSetFunctionCall());
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperator(), "AND");
    assertEquals(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().size(), 2);
    assertTrue(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).isSetFunctionCall());
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(),
        "GREATER_THAN_OR_EQUAL");
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .get(0).getFunctionCall().getOperator(), "minus");
    // Check that LHS of minus is col1 and RHS of minus is col2
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getFunctionCall().getOperands()
            .get(0).getFunctionCall().getOperands().get(1).getIdentifier().getName(), "col2");

    assertTrue(rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(1).isSetFunctionCall());
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperator(),
        "LESS_THAN_OR_EQUAL");
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands()
            .get(0).getIdentifier().getName(), "col1");
    assertEquals(
        rewrittenQuery.getFilterExpression().getFunctionCall().getOperands().get(1).getFunctionCall().getOperands()
            .get(1).getLiteral().getIntValue(), 20);
  }
}
