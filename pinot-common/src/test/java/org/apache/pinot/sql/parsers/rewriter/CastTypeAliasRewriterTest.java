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

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CastTypeAliasRewriterTest {

  private final CastTypeAliasRewriter _rewriter = new CastTypeAliasRewriter();

  /**
   * Calcite / SQL standard uses BIGINT; rewriter maps it to Pinot LONG for server compatibility.
   */
  @Test
  public void testTopLevelCastBigintRewritesToLong() {
    Expression cast = RequestUtils.getFunctionExpression("cast",
        RequestUtils.getIdentifierExpression("event_timestamp"),
        RequestUtils.getLiteralExpression("BIGINT"));
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.addToSelectList(cast);

    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Assert.assertEquals(
        rewritten.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "LONG");
  }

  /**
   * Type literal matching is case-insensitive (switch uses {@code toUpperCase()}).
   */
  @Test
  public void testBigintTypeLiteralCaseInsensitiveRewritesToLong() {
    Expression cast = RequestUtils.getFunctionExpression("cast",
        RequestUtils.getIdentifierExpression("x"),
        RequestUtils.getLiteralExpression("bigint"));
    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.addToSelectList(cast);

    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Assert.assertEquals(
        rewritten.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "LONG");
  }

  /**
   * Compiled SQL with explicit {@code AS BIGINT} must rewrite the cast target to LONG.
   */
  @Test
  public void testCompiledSimpleCastAsBigintRewritesToLong() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT CAST(event_timestamp AS BIGINT) FROM t");
    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Assert.assertEquals(
        rewritten.getSelectList().get(0).getFunctionCall().getOperands().get(1).getLiteral().getStringValue(),
        "LONG");
  }

  /**
   * Nested {@code CAST(CAST(x AS BIGINT) AS BIGINT)} — both CAST type operands must become LONG.
   */
  @Test
  public void testCompiledNestedBothBigintRewritesBothToLong() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT CAST(CAST(event_timestamp AS BIGINT) AS BIGINT) FROM t");
    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Expression outer = rewritten.getSelectList().get(0);
    Assert.assertEquals(outer.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "LONG");
    Expression inner = outer.getFunctionCall().getOperands().get(0);
    Assert.assertEquals(inner.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "LONG");
  }

  /**
   * Nested CAST(... AS DOUBLE) around CAST(col AS BIGINT) must rewrite the inner BIGINT alias so
   * older servers receive LONG (see CastTypeAliasRewriter).
   */
  @Test
  public void testNestedCastRewritesInnerBigintToLong() {
    Expression innerCast = RequestUtils.getFunctionExpression("cast",
        RequestUtils.getIdentifierExpression("event_timestamp"),
        RequestUtils.getLiteralExpression("BIGINT"));
    Expression outerCast = RequestUtils.getFunctionExpression("cast", innerCast,
        RequestUtils.getLiteralExpression("DOUBLE"));

    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.addToSelectList(outerCast);

    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Expression inner = rewritten.getSelectList().get(0).getFunctionCall().getOperands().get(0);
    Assert.assertEquals(inner.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "LONG");
  }

  /**
   * Deeply nested CASTs must all be visited (e.g. outer LONG, middle VARCHAR, inner BIGINT).
   */
  @Test
  public void testTripleNestedCastRewritesAllAliases() {
    Expression innerCast = RequestUtils.getFunctionExpression("cast",
        RequestUtils.getIdentifierExpression("x"),
        RequestUtils.getLiteralExpression("BIGINT"));
    Expression middleCast = RequestUtils.getFunctionExpression("cast", innerCast,
        RequestUtils.getLiteralExpression("VARCHAR"));
    Expression outerCast = RequestUtils.getFunctionExpression("cast", middleCast,
        RequestUtils.getLiteralExpression("BIGINT"));

    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.addToSelectList(outerCast);

    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Expression out = rewritten.getSelectList().get(0);
    Assert.assertEquals(out.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "LONG");

    Expression mid = out.getFunctionCall().getOperands().get(0);
    Assert.assertEquals(mid.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "STRING");

    Expression in = mid.getFunctionCall().getOperands().get(0);
    Assert.assertEquals(in.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "LONG");
  }

  /**
   * CAST around divide(CAST(... AS BIGINT), literal) — inner CAST is not a direct sibling in the tree;
   * it must still be rewritten when reached from the outer CAST's first operand.
   */
  @Test
  public void testCastOverDivideRewritesInnerBigint() {
    Expression innerCast = RequestUtils.getFunctionExpression("cast",
        RequestUtils.getIdentifierExpression("event_timestamp"),
        RequestUtils.getLiteralExpression("BIGINT"));
    Expression divide = RequestUtils.getFunctionExpression("divide", innerCast,
        RequestUtils.getLiteralExpression(1000));
    Expression outerCast = RequestUtils.getFunctionExpression("cast", divide,
        RequestUtils.getLiteralExpression("DOUBLE"));

    PinotQuery pinotQuery = new PinotQuery();
    pinotQuery.addToSelectList(outerCast);

    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Expression innerAfter = rewritten.getSelectList().get(0).getFunctionCall().getOperands().get(0)
        .getFunctionCall().getOperands().get(0);
    Assert.assertEquals(innerAfter.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "LONG");
  }

  /**
   * End-to-end: SQL that Calcite compiles to nested CASTs must rewrite inner BIGINT when present.
   */
  @Test
  public void testCompiledNestedLongInsideDoubleCast() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT CAST(CAST(event_timestamp AS LONG) AS DOUBLE) FROM t");
    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Expression inner = rewritten.getSelectList().get(0).getFunctionCall().getOperands().get(0);
    Assert.assertEquals(inner.getFunctionCall().getOperator(), "cast");
    Assert.assertEquals(inner.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "LONG");
  }

  /**
   * Nested CAST in WHERE must rewrite inner BIGINT — rewriter walks filterExpression the same as select list.
   */
  @Test
  public void testCompiledNestedLongInsideDoubleCastInWhere() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT intMetric1 FROM t WHERE CAST(CAST(event_timestamp AS LONG) AS DOUBLE) >= 0");
    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Function greaterOrEquals = rewritten.getFilterExpression().getFunctionCall();
    Assert.assertEquals(greaterOrEquals.getOperator(), FilterKind.GREATER_THAN_OR_EQUAL.name());
    Expression outerCast = greaterOrEquals.getOperands().get(0);
    Expression inner = outerCast.getFunctionCall().getOperands().get(0);
    Assert.assertEquals(inner.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "LONG");
  }

  /**
   * Nested CAST in GROUP BY must rewrite inner BIGINT.
   */
  @Test
  public void testCompiledNestedLongInsideDoubleCastInGroupBy() {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQueryWithoutRewrites(
        "SELECT COUNT(*) FROM t GROUP BY CAST(CAST(event_timestamp AS LONG) AS DOUBLE)");
    PinotQuery rewritten = _rewriter.rewrite(pinotQuery);
    Expression outerCast = rewritten.getGroupByList().get(0);
    Expression inner = outerCast.getFunctionCall().getOperands().get(0);
    Assert.assertEquals(inner.getFunctionCall().getOperands().get(1).getLiteral().getStringValue(), "LONG");
  }
}
