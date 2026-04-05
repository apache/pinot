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
package org.apache.pinot.query.planner.logical;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


public class LeafStageToPinotQueryTest {

  // --- Basic expression type handling ---

  @Test
  public void testNullInputReturnsNull() {
    assertNull(LeafStageToPinotQuery.ensureFilterIsFunctionExpression(null));
  }

  @Test
  public void testLiteralFilterReturnsNull() {
    Expression literalExpr = RequestUtils.getLiteralExpression(true);
    assertNull(LeafStageToPinotQuery.ensureFilterIsFunctionExpression(literalExpr),
        "Literal filter should return null (drop filter)");
  }

  @Test
  public void testIdentifierWrappedAsEqualsTrue() {
    Expression identifierExpr = RequestUtils.getIdentifierExpression("boolCol");

    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(identifierExpr);

    assertNotNull(result);
    assertNotNull(result.getFunctionCall());
    assertEquals(result.getFunctionCall().getOperator(), "EQUALS");
    List<Expression> operands = result.getFunctionCall().getOperands();
    assertEquals(operands.size(), 2);
    assertNotNull(operands.get(0).getIdentifier());
    assertEquals(operands.get(0).getIdentifier().getName(), "boolCol");
    assertNotNull(operands.get(1).getLiteral(), "Second operand should be literal true");
  }

  @Test
  public void testIdentifierWrappedWithMutableOperandList() {
    // Verify operand list is mutable so downstream rewriters (e.g., PredicateComparisonRewriter) can modify it
    Expression identifierExpr = RequestUtils.getIdentifierExpression("boolCol");
    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(identifierExpr);

    // Should not throw UnsupportedOperationException
    result.getFunctionCall().getOperands().set(1, RequestUtils.getLiteralExpression(false));
  }

  @Test
  public void testFunctionExpressionPassedThroughUnchanged() {
    Expression funcExpr = makeEquals("col", "val");

    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(funcExpr);

    assertSame(result, funcExpr);
    assertEquals(result.getFunctionCall().getOperator(), "EQUALS");
  }

  // --- AND/OR handling (shared logic, tested symmetrically) ---

  @Test
  public void testAndWithLiteralDropsLiteralAndUnwraps() {
    assertCompoundWithLiteralDropsAndUnwraps("AND");
  }

  @Test
  public void testOrWithLiteralDropsLiteralAndUnwraps() {
    assertCompoundWithLiteralDropsAndUnwraps("OR");
  }

  private void assertCompoundWithLiteralDropsAndUnwraps(String op) {
    // OP(LITERAL(true), EQUALS(col, 'val')) → EQUALS(col, 'val')
    Expression equalsExpr = makeEquals("col", "val");
    Expression compoundExpr = makeCompound(op, RequestUtils.getLiteralExpression(true), equalsExpr);

    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(compoundExpr);

    assertNotNull(result);
    assertEquals(result.getFunctionCall().getOperator(), "EQUALS");
    assertEquals(result.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col");
  }

  @Test
  public void testAndWithIdentifierWrapsIdentifier() {
    assertCompoundWithIdentifierWraps("AND");
  }

  @Test
  public void testOrWithIdentifierWrapsIdentifier() {
    assertCompoundWithIdentifierWraps("OR");
  }

  private void assertCompoundWithIdentifierWraps(String op) {
    // OP(boolCol, EQUALS(col, 'val')) → OP(EQUALS(boolCol, true), EQUALS(col, 'val'))
    Expression identifierExpr = RequestUtils.getIdentifierExpression("boolCol");
    Expression equalsExpr = makeEquals("col", "val");
    Expression compoundExpr = makeCompound(op, identifierExpr, equalsExpr);

    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(compoundExpr);

    assertNotNull(result);
    assertEquals(result.getFunctionCall().getOperator(), op);
    List<Expression> operands = result.getFunctionCall().getOperands();
    assertEquals(operands.size(), 2);
    assertEquals(operands.get(0).getFunctionCall().getOperator(), "EQUALS");
    assertEquals(operands.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "boolCol");
    assertEquals(operands.get(1).getFunctionCall().getOperator(), "EQUALS");
  }

  @Test
  public void testAndWithAllLiteralsReturnsNull() {
    assertCompoundWithAllLiteralsReturnsNull("AND");
  }

  @Test
  public void testOrWithAllLiteralsReturnsNull() {
    assertCompoundWithAllLiteralsReturnsNull("OR");
  }

  private void assertCompoundWithAllLiteralsReturnsNull(String op) {
    // OP(LITERAL, LITERAL) → null
    Expression compoundExpr = makeCompound(op,
        RequestUtils.getLiteralExpression(true), RequestUtils.getLiteralExpression(false));

    assertNull(LeafStageToPinotQuery.ensureFilterIsFunctionExpression(compoundExpr));
  }

  @Test
  public void testAndWithMultipleIdentifiers() {
    assertCompoundWithMultipleIdentifiers("AND");
  }

  @Test
  public void testOrWithMultipleIdentifiers() {
    assertCompoundWithMultipleIdentifiers("OR");
  }

  private void assertCompoundWithMultipleIdentifiers(String op) {
    // OP(boolCol, active) → OP(EQUALS(boolCol, true), EQUALS(active, true))
    Expression compoundExpr = makeCompound(op,
        RequestUtils.getIdentifierExpression("boolCol"),
        RequestUtils.getIdentifierExpression("active"));

    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(compoundExpr);

    assertNotNull(result);
    assertEquals(result.getFunctionCall().getOperator(), op);
    List<Expression> operands = result.getFunctionCall().getOperands();
    assertEquals(operands.size(), 2);
    assertEquals(operands.get(0).getFunctionCall().getOperator(), "EQUALS");
    assertEquals(operands.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "boolCol");
    assertEquals(operands.get(1).getFunctionCall().getOperator(), "EQUALS");
    assertEquals(operands.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "active");
  }

  // --- NOT handling ---

  @Test
  public void testNotWithIdentifierWrapsChild() {
    // NOT(boolCol) → NOT(EQUALS(boolCol, true))
    Expression identifierExpr = RequestUtils.getIdentifierExpression("boolCol");
    Expression notExpr = makeCompound("NOT", identifierExpr);

    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(notExpr);

    assertNotNull(result);
    assertEquals(result.getFunctionCall().getOperator(), "NOT");
    List<Expression> operands = result.getFunctionCall().getOperands();
    assertEquals(operands.size(), 1);
    assertEquals(operands.get(0).getFunctionCall().getOperator(), "EQUALS");
    assertEquals(operands.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "boolCol");
  }

  @Test
  public void testNotWithLiteralReturnsNull() {
    // NOT(LITERAL(true)) → null
    Expression notExpr = makeCompound("NOT", RequestUtils.getLiteralExpression(true));

    assertNull(LeafStageToPinotQuery.ensureFilterIsFunctionExpression(notExpr));
  }

  @Test
  public void testNotWithFunctionPassesThrough() {
    // NOT(EQUALS(col, 'val')) → NOT(EQUALS(col, 'val')) unchanged
    Expression equalsExpr = makeEquals("col", "val");
    Expression notExpr = makeCompound("NOT", equalsExpr);

    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(notExpr);

    assertSame(result, notExpr);
    assertEquals(result.getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "EQUALS");
  }

  // --- Nested structures ---

  @Test
  public void testNestedOrInsideAndWithMixedTypes() {
    // OR(AND(LITERAL, EQUALS(a, 'x')), IDENTIFIER(boolCol))
    // Inner AND: LITERAL dropped → unwrap to EQUALS(a, 'x')
    // Outer OR: [EQUALS(a, 'x'), EQUALS(boolCol, true)]
    Expression innerAnd = makeCompound("AND",
        RequestUtils.getLiteralExpression(true), makeEquals("a", "x"));
    Expression outerOr = makeCompound("OR",
        innerAnd, RequestUtils.getIdentifierExpression("boolCol"));

    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(outerOr);

    assertNotNull(result);
    assertEquals(result.getFunctionCall().getOperator(), "OR");
    List<Expression> operands = result.getFunctionCall().getOperands();
    assertEquals(operands.size(), 2);
    assertEquals(operands.get(0).getFunctionCall().getOperator(), "EQUALS");
    assertEquals(operands.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "a");
    assertEquals(operands.get(1).getFunctionCall().getOperator(), "EQUALS");
    assertEquals(operands.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "boolCol");
  }

  @Test
  public void testNotInsideAndWithIdentifier() {
    // AND(NOT(boolCol), EQUALS(col, 'val'))
    // NOT child wrapped: NOT(EQUALS(boolCol, true))
    // AND has two FUNCTION children: NOT and EQUALS
    Expression notExpr = makeCompound("NOT", RequestUtils.getIdentifierExpression("boolCol"));
    Expression andExpr = makeCompound("AND", notExpr, makeEquals("col", "val"));

    Expression result = LeafStageToPinotQuery.ensureFilterIsFunctionExpression(andExpr);

    assertNotNull(result);
    assertEquals(result.getFunctionCall().getOperator(), "AND");
    List<Expression> operands = result.getFunctionCall().getOperands();
    assertEquals(operands.size(), 2);
    assertEquals(operands.get(0).getFunctionCall().getOperator(), "NOT");
    assertEquals(operands.get(0).getFunctionCall().getOperands().get(0).getFunctionCall().getOperator(), "EQUALS");
    assertEquals(operands.get(1).getFunctionCall().getOperator(), "EQUALS");
  }

  // --- Helpers ---

  private static Expression makeEquals(String column, String value) {
    Function equalsFunc = new Function("EQUALS");
    equalsFunc.setOperands(new ArrayList<>(List.of(
        RequestUtils.getIdentifierExpression(column),
        RequestUtils.getLiteralExpression(value))));
    Expression expr = new Expression(ExpressionType.FUNCTION);
    expr.setFunctionCall(equalsFunc);
    return expr;
  }

  private static Expression makeCompound(String operator, Expression... children) {
    Function func = new Function(operator);
    func.setOperands(new ArrayList<>(List.of(children)));
    Expression expr = new Expression(ExpressionType.FUNCTION);
    expr.setFunctionCall(func);
    return expr;
  }
}
