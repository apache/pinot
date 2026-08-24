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
package org.apache.pinot.core.query.optimizer.filter;

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DistributeConjunctsIntoOrFilterOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName("testTable").addSingleValueDimension("intColumn", FieldSpec.DataType.INT)
          .addSingleValueDimension("longColumn", FieldSpec.DataType.LONG)
          .addSingleValueDimension("stringColumn", FieldSpec.DataType.STRING).build();

  @Test
  public void testDistributeConjunctsIntoOr() {
    // P AND (A OR B) should be rewritten to P AND ((P AND A) OR (P AND B))
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT * FROM testTable WHERE intColumn = 5 AND (stringColumn = 'a' OR longColumn = 10)");
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Expression filter = pinotQuery.getFilterExpression();
    Function filterFunction = filter.getFunctionCall();
    Assert.assertNotNull(filterFunction);
    Assert.assertEquals(filterFunction.getOperator(), FilterKind.AND.name());

    List<Expression> operands = filterFunction.getOperands();
    Assert.assertEquals(operands.size(), 2, "Expected AND(intColumn=5, OR(...))");

    // The second operand should be an OR whose branches are ANDs that each start with intColumn = 5
    Expression orExpression = operands.get(1);
    Function orFunction = orExpression.getFunctionCall();
    Assert.assertNotNull(orFunction);
    Assert.assertEquals(orFunction.getOperator(), FilterKind.OR.name());
    Assert.assertEquals(orFunction.getOperands().size(), 2);

    for (Expression branch : orFunction.getOperands()) {
      Function branchFunction = branch.getFunctionCall();
      Assert.assertNotNull(branchFunction, "Each OR branch should be rewritten to an AND");
      Assert.assertEquals(branchFunction.getOperator(), FilterKind.AND.name());
      List<Expression> branchOperands = branchFunction.getOperands();
      Assert.assertEquals(branchOperands.size(), 2);
      // Pushed-down conjunct is the first operand of each branch
      assertPredicate(branchOperands.get(0), "intColumn", 5);
    }

    // The outer AND keeps the original conjunct
    assertPredicate(operands.get(0), "intColumn", 5);
  }

  @Test
  public void testNoDistributionWithoutOr() {
    // No OR child -> no rewrite
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT * FROM testTable WHERE intColumn = 5 AND stringColumn = 'a'");
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Expression filter = pinotQuery.getFilterExpression();
    Function filterFunction = filter.getFunctionCall();
    Assert.assertNotNull(filterFunction);
    Assert.assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> operands = filterFunction.getOperands();
    // intColumn = 5 may be merged with stringColumn = 'a'? No: different columns, so AND has 2 operands
    Assert.assertEquals(operands.size(), 2);
  }

  @Test
  public void testNoDistributionWithoutDistributableConjunct() {
    // Conjunct is an IN_SUBQUERY-like expression (LHS is not an identifier) -> no rewrite.
    // Use a function on the LHS so isDistributable returns false.
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT * FROM testTable WHERE add(intColumn, longColumn) = 5 AND (stringColumn = 'a' OR longColumn = 10)");
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Expression filter = pinotQuery.getFilterExpression();
    Function filterFunction = filter.getFunctionCall();
    Assert.assertNotNull(filterFunction);
    Assert.assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> operands = filterFunction.getOperands();
    Assert.assertEquals(operands.size(), 2);

    // The OR branch should NOT have been rewritten (branch stays a single predicate, not an AND)
    Expression orExpression = operands.get(1);
    Function orFunction = orExpression.getFunctionCall();
    Assert.assertNotNull(orFunction);
    Assert.assertEquals(orFunction.getOperator(), FilterKind.OR.name());
    for (Expression branch : orFunction.getOperands()) {
      Function branchFunction = branch.getFunctionCall();
      if (branchFunction != null) {
        Assert.assertNotEquals(branchFunction.getOperator(), FilterKind.AND.name() + " of intColumn = 5",
            "No conjunction should be pushed into the OR branch when the conjunct is not distributable");
      }
    }
  }

  @Test
  public void testBranchAlreadyContainsConjunct() {
    // P AND (P OR B): the first branch already contains the conjunct, no AND(P, P) should be created
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT * FROM testTable WHERE intColumn = 5 AND (intColumn = 5 OR longColumn = 10)");
    OPTIMIZER.optimize(pinotQuery, SCHEMA);

    Expression filter = pinotQuery.getFilterExpression();
    Function filterFunction = filter.getFunctionCall();
    Assert.assertNotNull(filterFunction);
    Assert.assertEquals(filterFunction.getOperator(), FilterKind.AND.name());
    List<Expression> operands = filterFunction.getOperands();
    Assert.assertEquals(operands.size(), 2);

    Expression orExpression = operands.get(1);
    Function orFunction = orExpression.getFunctionCall();
    Assert.assertNotNull(orFunction);
    Assert.assertEquals(orFunction.getOperator(), FilterKind.OR.name());
    List<Expression> branches = orFunction.getOperands();
    Assert.assertEquals(branches.size(), 2);
    // First branch stays intColumn = 5 (conjunct already present, not duplicated)
    assertPredicate(branches.get(0), "intColumn", 5);
    // Second branch becomes AND(intColumn = 5, longColumn = 10)
    Function branchFunction = branches.get(1).getFunctionCall();
    Assert.assertNotNull(branchFunction);
    Assert.assertEquals(branchFunction.getOperator(), FilterKind.AND.name());
  }

  @Test
  public void testNoRewritesOnEmptyAnd() {
    // Only OR with no conjuncts -> no rewrite
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT * FROM testTable WHERE (stringColumn = 'a' OR longColumn = 10)");
    OPTIMIZER.optimize(pinotQuery, SCHEMA);
    // The whole filter stays an OR (flattened)
    Function filterFunction = pinotQuery.getFilterExpression().getFunctionCall();
    Assert.assertNotNull(filterFunction);
    Assert.assertEquals(filterFunction.getOperator(), FilterKind.OR.name());
  }

  private static void assertPredicate(Expression expression, String columnName, int value) {
    Function function = expression.getFunctionCall();
    Assert.assertNotNull(function, "Expected a function predicate, got: " + expression);
    Assert.assertEquals(function.getOperator(), FilterKind.EQUALS.name());
    List<Expression> operands = function.getOperands();
    Assert.assertNotNull(operands);
    Assert.assertEquals(operands.size(), 2);
    Expression lhs = operands.get(0);
    Assert.assertEquals(lhs.getType(), ExpressionType.IDENTIFIER);
    Assert.assertEquals(lhs.getIdentifier().getName(), columnName);
    Expression rhs = operands.get(1);
    Assert.assertEquals(rhs.getType(), ExpressionType.LITERAL);
    Assert.assertEquals(rhs.getLiteral().getLongValue(), (long) value);
  }
}