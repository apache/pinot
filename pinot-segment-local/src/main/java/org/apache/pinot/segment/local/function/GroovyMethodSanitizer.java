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
package org.apache.pinot.segment.local.function;

import java.util.List;
import org.codehaus.groovy.ast.GroovyCodeVisitor;
import org.codehaus.groovy.ast.Parameter;
import org.codehaus.groovy.ast.expr.ArgumentListExpression;
import org.codehaus.groovy.ast.expr.ArrayExpression;
import org.codehaus.groovy.ast.expr.AttributeExpression;
import org.codehaus.groovy.ast.expr.BinaryExpression;
import org.codehaus.groovy.ast.expr.BitwiseNegationExpression;
import org.codehaus.groovy.ast.expr.BooleanExpression;
import org.codehaus.groovy.ast.expr.CastExpression;
import org.codehaus.groovy.ast.expr.ClassExpression;
import org.codehaus.groovy.ast.expr.ClosureExpression;
import org.codehaus.groovy.ast.expr.ClosureListExpression;
import org.codehaus.groovy.ast.expr.ConstantExpression;
import org.codehaus.groovy.ast.expr.ConstructorCallExpression;
import org.codehaus.groovy.ast.expr.DeclarationExpression;
import org.codehaus.groovy.ast.expr.ElvisOperatorExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.ast.expr.FieldExpression;
import org.codehaus.groovy.ast.expr.GStringExpression;
import org.codehaus.groovy.ast.expr.ListExpression;
import org.codehaus.groovy.ast.expr.MapEntryExpression;
import org.codehaus.groovy.ast.expr.MapExpression;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.ast.expr.MethodPointerExpression;
import org.codehaus.groovy.ast.expr.NotExpression;
import org.codehaus.groovy.ast.expr.PostfixExpression;
import org.codehaus.groovy.ast.expr.PrefixExpression;
import org.codehaus.groovy.ast.expr.PropertyExpression;
import org.codehaus.groovy.ast.expr.RangeExpression;
import org.codehaus.groovy.ast.expr.SpreadExpression;
import org.codehaus.groovy.ast.expr.SpreadMapExpression;
import org.codehaus.groovy.ast.expr.StaticMethodCallExpression;
import org.codehaus.groovy.ast.expr.TernaryExpression;
import org.codehaus.groovy.ast.expr.TupleExpression;
import org.codehaus.groovy.ast.expr.UnaryMinusExpression;
import org.codehaus.groovy.ast.expr.UnaryPlusExpression;
import org.codehaus.groovy.ast.expr.VariableExpression;
import org.codehaus.groovy.ast.stmt.AssertStatement;
import org.codehaus.groovy.ast.stmt.BlockStatement;
import org.codehaus.groovy.ast.stmt.BreakStatement;
import org.codehaus.groovy.ast.stmt.CaseStatement;
import org.codehaus.groovy.ast.stmt.CatchStatement;
import org.codehaus.groovy.ast.stmt.ContinueStatement;
import org.codehaus.groovy.ast.stmt.DoWhileStatement;
import org.codehaus.groovy.ast.stmt.ExpressionStatement;
import org.codehaus.groovy.ast.stmt.ForStatement;
import org.codehaus.groovy.ast.stmt.IfStatement;
import org.codehaus.groovy.ast.stmt.ReturnStatement;
import org.codehaus.groovy.ast.stmt.SwitchStatement;
import org.codehaus.groovy.ast.stmt.SynchronizedStatement;
import org.codehaus.groovy.ast.stmt.ThrowStatement;
import org.codehaus.groovy.ast.stmt.TryCatchStatement;
import org.codehaus.groovy.ast.stmt.WhileStatement;
import org.codehaus.groovy.classgen.BytecodeExpression;


public class GroovyMethodSanitizer implements GroovyCodeVisitor {
  List<String> _blockedMethods;
  boolean _isInvalid;

  public GroovyMethodSanitizer(List<String> blockedMethods) {
    _blockedMethods = blockedMethods;
    _isInvalid = false;
  }

  public boolean isInvalid() {
    return _isInvalid;
  }

  private boolean isMethodBlocked(Expression method) {
    return _blockedMethods.contains(method.getText());
  }

  @Override
  public void visitBlockStatement(BlockStatement blockStatement) {
    blockStatement.getStatements().forEach(stm -> stm.visit(this));
  }

  @Override
  public void visitForLoop(ForStatement forStatement) {
    forStatement.getVariable().visit(this);
    forStatement.getCollectionExpression().visit(this);
    forStatement.getLoopBlock().visit(this);
  }

  @Override
  public void visitWhileLoop(WhileStatement whileStatement) {
    whileStatement.getBooleanExpression().visit(this);
    whileStatement.getLoopBlock().visit(this);
  }

  @Override
  public void visitDoWhileLoop(DoWhileStatement doWhileStatement) {
    doWhileStatement.getBooleanExpression().visit(this);
    doWhileStatement.getLoopBlock().visit(this);
  }

  @Override
  public void visitIfElse(IfStatement ifStatement) {
    ifStatement.getBooleanExpression().visit(this);
    ifStatement.getIfBlock().visit(this);
    ifStatement.getElseBlock().visit(this);
  }

  @Override
  public void visitExpressionStatement(ExpressionStatement expressionStatement) {
    expressionStatement.getExpression().visit(this);
  }

  @Override
  public void visitReturnStatement(ReturnStatement returnStatement) {
    returnStatement.getExpression().visit(this);
  }

  @Override
  public void visitAssertStatement(AssertStatement assertStatement) {
    assertStatement.getBooleanExpression().visit(this);
    assertStatement.getMessageExpression().visit(this);
  }

  @Override
  public void visitTryCatchFinally(TryCatchStatement tryCatchStatement) {
    tryCatchStatement.getCatchStatements().forEach(stm -> stm.visit(this));
    tryCatchStatement.getTryStatement().visit(this);
    tryCatchStatement.getFinallyStatement().visit(this);
  }

  @Override
  public void visitSwitch(SwitchStatement switchStatement) {
    switchStatement.getExpression().visit(this);
    switchStatement.getCaseStatements().forEach(stm -> stm.visit(this));
    switchStatement.getDefaultStatement().visit(this);
  }

  @Override
  public void visitCaseStatement(CaseStatement caseStatement) {
    caseStatement.getExpression().visit(this);
    caseStatement.getCode().visit(this);
  }

  @Override
  public void visitBreakStatement(BreakStatement breakStatement) {
  }

  @Override
  public void visitContinueStatement(ContinueStatement continueStatement) {
  }

  @Override
  public void visitThrowStatement(ThrowStatement throwStatement) {
    throwStatement.getExpression().visit(this);
  }

  @Override
  public void visitSynchronizedStatement(SynchronizedStatement synchronizedStatement) {
    synchronizedStatement.getExpression().visit(this);
    synchronizedStatement.getCode().visit(this);
  }

  @Override
  public void visitCatchStatement(CatchStatement catchStatement) {
    catchStatement.getVariable().visit(this);
    catchStatement.getCode().visit(this);
  }

  @Override
  public void visitMethodCallExpression(MethodCallExpression methodCallExpression) {
    if (isMethodBlocked(methodCallExpression.getMethod())) {
      _isInvalid = true;
      return;
    }
    methodCallExpression.getMethod().visit(this);

    methodCallExpression.getArguments().visit(this);
    methodCallExpression.getObjectExpression().visit(this);
  }

  @Override
  public void visitStaticMethodCallExpression(StaticMethodCallExpression staticMethodCallExpression) {
    staticMethodCallExpression.getArguments().visit(this);
  }

  @Override
  public void visitConstructorCallExpression(ConstructorCallExpression constructorCallExpression) {
    constructorCallExpression.getArguments().visit(this);
  }

  @Override
  public void visitTernaryExpression(TernaryExpression ternaryExpression) {
    ternaryExpression.getBooleanExpression().visit(this);
    ternaryExpression.getTrueExpression().visit(this);
    ternaryExpression.getFalseExpression().visit(this);
  }

  @Override
  public void visitShortTernaryExpression(ElvisOperatorExpression elvisOperatorExpression) {
    elvisOperatorExpression.getBooleanExpression().visit(this);
    elvisOperatorExpression.getTrueExpression().visit(this);
    elvisOperatorExpression.getFalseExpression().visit(this);
  }

  @Override
  public void visitBinaryExpression(BinaryExpression binaryExpression) {
    binaryExpression.getLeftExpression().visit(this);
    binaryExpression.getRightExpression().visit(this);
  }

  @Override
  public void visitPrefixExpression(PrefixExpression prefixExpression) {
    prefixExpression.getExpression().visit(this);
  }

  @Override
  public void visitPostfixExpression(PostfixExpression postfixExpression) {
    postfixExpression.getExpression().visit(this);
  }

  @Override
  public void visitBooleanExpression(BooleanExpression booleanExpression) {
    booleanExpression.getExpression().visit(this);
  }

  @Override
  public void visitClosureExpression(ClosureExpression closureExpression) {
    closureExpression.getCode().visit(this);
    for (Parameter param : closureExpression.getParameters()) {
      param.visit(this);
    }
  }

  @Override
  public void visitTupleExpression(TupleExpression tupleExpression) {
    tupleExpression.getExpressions().forEach(exp -> exp.visit(this));
  }

  @Override
  public void visitMapExpression(MapExpression mapExpression) {
    mapExpression.getMapEntryExpressions().forEach(exp -> exp.visit(this));
  }

  @Override
  public void visitMapEntryExpression(MapEntryExpression mapEntryExpression) {
    mapEntryExpression.getKeyExpression().visit(this);
    mapEntryExpression.getValueExpression().visit(this);
  }

  @Override
  public void visitListExpression(ListExpression listExpression) {
    listExpression.getExpressions().forEach(exp -> exp.visit(this));
  }

  @Override
  public void visitRangeExpression(RangeExpression rangeExpression) {
    rangeExpression.getFrom().visit(this);
    rangeExpression.getTo().visit(this);
  }

  @Override
  public void visitPropertyExpression(PropertyExpression propertyExpression) {
    propertyExpression.getObjectExpression().visit(this);
    propertyExpression.getProperty().visit(this);
  }

  @Override
  public void visitAttributeExpression(AttributeExpression attributeExpression) {
    attributeExpression.getObjectExpression().visit(this);
  }

  @Override
  public void visitFieldExpression(FieldExpression fieldExpression) {
    fieldExpression.getField().visit(this);
  }

  @Override
  public void visitMethodPointerExpression(MethodPointerExpression methodPointerExpression) {
    methodPointerExpression.getExpression().visit(this);
    methodPointerExpression.getMethodName().visit(this);
  }

  @Override
  public void visitConstantExpression(ConstantExpression constantExpression) {
  }

  @Override
  public void visitClassExpression(ClassExpression classExpression) {
    if (classExpression.getDeclaringClass() != null) {
      classExpression.getDeclaringClass().visit(this);
    }
    if (classExpression.getAnnotations() != null) {
      classExpression.getAnnotations().forEach(ann -> ann.visit(this));
    }
  }

  @Override
  public void visitVariableExpression(VariableExpression variableExpression) {
    if (variableExpression.hasInitialExpression()) {
      variableExpression.getInitialExpression().visit(this);
    }
  }

  @Override
  public void visitDeclarationExpression(DeclarationExpression declarationExpression) {
    declarationExpression.getTupleExpression().visit(this);
    declarationExpression.getDeclaringClass().visit(this);
    declarationExpression.getVariableExpression().visit(this);
  }

  @Override
  public void visitGStringExpression(GStringExpression gStringExpression) {
    gStringExpression.getStrings().forEach(str -> str.visit(this));
    gStringExpression.getAnnotations().forEach(ann -> ann.visit(this));
    gStringExpression.getValues().forEach(val -> val.visit(this));
  }

  @Override
  public void visitArrayExpression(ArrayExpression arrayExpression) {
    arrayExpression.getExpressions().forEach(expr -> expr.visit(this));
    arrayExpression.getSizeExpression().forEach(expr -> expr.visit(this));
  }

  @Override
  public void visitSpreadExpression(SpreadExpression spreadExpression) {
    spreadExpression.getExpression().visit(this);
  }

  @Override
  public void visitSpreadMapExpression(SpreadMapExpression spreadMapExpression) {
    spreadMapExpression.getExpression().visit(this);
  }

  @Override
  public void visitNotExpression(NotExpression notExpression) {
    notExpression.getExpression().visit(this);
  }

  @Override
  public void visitUnaryMinusExpression(UnaryMinusExpression unaryMinusExpression) {
    unaryMinusExpression.getExpression().visit(this);
  }

  @Override
  public void visitUnaryPlusExpression(UnaryPlusExpression unaryPlusExpression) {
    unaryPlusExpression.getExpression().visit(this);
  }

  @Override
  public void visitBitwiseNegationExpression(BitwiseNegationExpression bitwiseNegationExpression) {
    bitwiseNegationExpression.getExpression().visit(this);
  }

  @Override
  public void visitCastExpression(CastExpression castExpression) {
    castExpression.getExpression().visit(this);
  }

  @Override
  public void visitArgumentlistExpression(ArgumentListExpression argumentListExpression) {
    argumentListExpression.getExpressions().forEach(expr -> expr.visit(this));
  }

  @Override
  public void visitClosureListExpression(ClosureListExpression closureListExpression) {
    closureListExpression.getExpressions().forEach(expr -> expr.visit(this));
  }

  @Override
  public void visitBytecodeExpression(BytecodeExpression bytecodeExpression) {
  }
}
