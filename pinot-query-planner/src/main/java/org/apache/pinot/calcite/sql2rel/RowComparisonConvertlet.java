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
package org.apache.pinot.calcite.sql2rel;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;


/**
 * Convertlet that rewrites ROW comparisons into scalar comparisons.
 *
 * Transforms:
 *   (A, B) = (X, Y)   → (A = X) AND (B = Y)
 *   (A, B) > (X, Y)   → (A > X) OR ((A = X) AND (B > Y))
 *   (A, B) >= (X, Y)  → (A > X) OR ((A = X) AND (B > Y)) OR ((A = X) AND (B = Y))
 *   (A, B) < (X, Y)   → (A < X) OR ((A = X) AND (B < Y))
 *   (A, B) <= (X, Y)  → (A < X) OR ((A = X) AND (B < Y)) OR ((A = X) AND (B = Y))
 *   (A, B) <> (X, Y)  → NOT((A = X) AND (B = Y))
 */
public class RowComparisonConvertlet implements SqlRexConvertlet {

  public static final RowComparisonConvertlet INSTANCE =
      new RowComparisonConvertlet();

  @Override
  public RexNode convertCall(SqlRexContext cx, SqlCall call) {
    if (call.getOperandList().size() != 2) {
      throw new IllegalArgumentException(
          "ROW comparisons must have exactly 2 operands, got: " + call.getOperandList().size());
    }

    SqlNode leftNode = call.operand(0);
    SqlNode rightNode = call.operand(1);

    // Validate both operands are ROW expressions
    if (!isRowExpression(leftNode)) {
      throw new IllegalArgumentException(
          "Left operand of ROW comparison must be a ROW expression. Got: " + getNodeKind(leftNode)
              + ". Both operands must be ROW expressions for comparison.");
    }
    if (!isRowExpression(rightNode)) {
      throw new IllegalArgumentException(
          "Right operand of ROW comparison must be a ROW expression. Got: " + getNodeKind(rightNode)
              + ". Both operands must be ROW expressions for comparison.");
    }

    // Unwrap CAST to get the actual ROW expressions
    SqlCall leftRow = unwrapCastToRow(leftNode);
    SqlCall rightRow = unwrapCastToRow(rightNode);

    // Validate both ROW expressions have the same number of fields
    int leftSize = leftRow.getOperandList().size();
    int rightSize = rightRow.getOperandList().size();
    if (leftSize != rightSize) {
      throw new IllegalArgumentException(
          String.format("ROW comparison operands must have the same number of fields. Left has %d fields, "
                  + "right has %d fields.",
              leftSize, rightSize));
    }
    if (leftSize == 0) {
      throw new IllegalArgumentException("ROW expressions cannot be empty");
    }

    RexBuilder rexBuilder = cx.getRexBuilder();
    // Convert ROW fields to RexNodes
    List<RexNode> leftFields = new ArrayList<>(leftSize);
    for (SqlNode field : leftRow.getOperandList()) {
      leftFields.add(cx.convertExpression(field));
    }
    List<RexNode> rightFields = new ArrayList<>(rightSize);
    for (SqlNode field : rightRow.getOperandList()) {
      rightFields.add(cx.convertExpression(field));
    }

    SqlKind kind = call.getKind();
    switch (kind) {
      case EQUALS:
        return rewriteEquals(rexBuilder, leftFields, rightFields);
      case NOT_EQUALS:
        return rewriteNotEquals(rexBuilder, leftFields, rightFields);
      case GREATER_THAN:
        return rewriteGreaterThan(rexBuilder, leftFields, rightFields);
      case GREATER_THAN_OR_EQUAL:
        return rewriteGreaterThanOrEqual(rexBuilder, leftFields, rightFields);
      case LESS_THAN:
        return rewriteLessThan(rexBuilder, leftFields, rightFields);
      case LESS_THAN_OR_EQUAL:
        return rewriteLessThanOrEqual(rexBuilder, leftFields, rightFields);
      default:
        throw new IllegalArgumentException("Unsupported ROW comparison operator: " + kind);
    }
  }

  private static boolean isRowExpression(SqlNode node) {
    if (!(node instanceof SqlCall)) {
      return false;
    }

    SqlCall call = (SqlCall) node;
    if (call.getKind() == SqlKind.ROW) {
      return true;
    }
    // ROW wrapped in CAST
    if (call.getKind() == SqlKind.CAST && call.getOperandList().size() > 0) {
      SqlNode castOperand = call.getOperandList().get(0);
      if (castOperand instanceof SqlCall && ((SqlCall) castOperand).getKind() == SqlKind.ROW) {
        return true;
      }
    }
    return false;
  }

  private static SqlCall unwrapCastToRow(SqlNode node) {
    if (!(node instanceof SqlCall)) {
      throw new IllegalArgumentException("Expected SqlCall, got: " + node.getClass());
    }

    SqlCall call = (SqlCall) node;
    if (call.getKind() == SqlKind.ROW) {
      return call;
    }
    // CAST(ROW(...) AS ...) - extract the ROW
    if (call.getKind() == SqlKind.CAST && call.getOperandList().size() > 0) {
      SqlNode operand = call.getOperandList().get(0);
      if (operand instanceof SqlCall && ((SqlCall) operand).getKind() == SqlKind.ROW) {
        return (SqlCall) operand;
      }
    }
    throw new IllegalArgumentException("Expected ROW or CAST(ROW), got: " + call.getKind());
  }

  private static SqlKind getNodeKind(SqlNode node) {
    if (!(node instanceof SqlCall)) {
      return SqlKind.OTHER;
    }

    SqlCall call = (SqlCall) node;
    if (call.getKind() == SqlKind.CAST && call.getOperandList().size() > 0) {
      SqlNode operand = call.getOperandList().get(0);
      if (operand instanceof SqlCall) {
        return ((SqlCall) operand).getKind();
      }
    }
    return call.getKind();
  }

  private RexNode rewriteEquals(RexBuilder rexBuilder, List<RexNode> left, List<RexNode> right) {
    List<RexNode> conditions = new ArrayList<>(left.size());
    for (int i = 0; i < left.size(); i++) {
      conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left.get(i), right.get(i)));
    }
    return RexUtil.composeConjunction(rexBuilder, conditions);
  }

  private RexNode rewriteNotEquals(RexBuilder rexBuilder, List<RexNode> left, List<RexNode> right) {
    RexNode equals = rewriteEquals(rexBuilder, left, right);
    return rexBuilder.makeCall(SqlStdOperatorTable.NOT, equals);
  }

  private RexNode rewriteGreaterThan(RexBuilder rexBuilder, List<RexNode> left, List<RexNode> right) {
    List<RexNode> orClauses = new ArrayList<>();
    for (int i = 0; i < left.size(); i++) {
      List<RexNode> andConditions = new ArrayList<>();
      for (int j = 0; j < i; j++) {
        andConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left.get(j), right.get(j)));
      }
      andConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, left.get(i), right.get(i)));
      orClauses.add(RexUtil.composeConjunction(rexBuilder, andConditions));
    }
    return RexUtil.composeDisjunction(rexBuilder, orClauses);
  }

  private RexNode rewriteGreaterThanOrEqual(RexBuilder rexBuilder, List<RexNode> left, List<RexNode> right) {
    List<RexNode> orClauses = new ArrayList<>();

    for (int i = 0; i < left.size(); i++) {
      List<RexNode> andConditions = new ArrayList<>();
      for (int j = 0; j < i; j++) {
        andConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left.get(j), right.get(j)));
      }
      andConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, left.get(i), right.get(i)));
      orClauses.add(RexUtil.composeConjunction(rexBuilder, andConditions));
    }

    orClauses.add(rewriteEquals(rexBuilder, left, right));
    return RexUtil.composeDisjunction(rexBuilder, orClauses);
  }

  private RexNode rewriteLessThan(RexBuilder rexBuilder, List<RexNode> left, List<RexNode> right) {
    List<RexNode> orClauses = new ArrayList<>();

    for (int i = 0; i < left.size(); i++) {
      List<RexNode> andConditions = new ArrayList<>();
      for (int j = 0; j < i; j++) {
        andConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left.get(j), right.get(j)));
      }
      andConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, left.get(i), right.get(i)));
      orClauses.add(RexUtil.composeConjunction(rexBuilder, andConditions));
    }

    return RexUtil.composeDisjunction(rexBuilder, orClauses);
  }

  private RexNode rewriteLessThanOrEqual(RexBuilder rexBuilder, List<RexNode> left, List<RexNode> right) {
    List<RexNode> orClauses = new ArrayList<>();

    for (int i = 0; i < left.size(); i++) {
      List<RexNode> andConditions = new ArrayList<>();
      for (int j = 0; j < i; j++) {
        andConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left.get(j), right.get(j)));
      }
      andConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, left.get(i), right.get(i)));
      orClauses.add(RexUtil.composeConjunction(rexBuilder, andConditions));
    }

    orClauses.add(rewriteEquals(rexBuilder, left, right));
    return RexUtil.composeDisjunction(rexBuilder, orClauses);
  }
}
