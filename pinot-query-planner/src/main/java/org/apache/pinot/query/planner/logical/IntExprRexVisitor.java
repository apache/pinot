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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;


/**
 * {@code IntExprRexVisitor} will visit a {@link RexNode} that
 * contains only integer literals and uses the {@link SqlStdOperatorTable#PLUS}
 * operator and return the Integer that the expression represents.
 */
public class IntExprRexVisitor implements RexVisitor<Integer> {

  public static final IntExprRexVisitor INSTANCE = new IntExprRexVisitor();

  private IntExprRexVisitor() {
  }

  public Integer visit(RexNode in) {
    if (in == null) {
      return null;
    }

    return in.accept(this);
  }

  @Override
  public Integer visitInputRef(RexInputRef inputRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer visitLocalRef(RexLocalRef localRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer visitLiteral(RexLiteral literal) {
    return literal.getValueAs(Integer.class);
  }

  @Override
  public Integer visitCall(RexCall call) {
    SqlOperator operator = call.getOperator();
    if (!(operator == SqlStdOperatorTable.PLUS)) {
      throw new UnsupportedOperationException("Expected PLUS operator but got " + operator);
    }

    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);

    return left.accept(this) + right.accept(this);
  }

  @Override
  public Integer visitOver(RexOver over) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer visitRangeRef(RexRangeRef rangeRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer visitFieldAccess(RexFieldAccess fieldAccess) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer visitSubQuery(RexSubQuery subQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer visitTableInputRef(RexTableInputRef fieldRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new UnsupportedOperationException();
  }
}
