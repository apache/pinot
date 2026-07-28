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
package org.apache.pinot.query.planner.validation;

import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;


/**
 * Rejects operations that would otherwise assign physical byte ordering, equality, or hashing semantics to a raw
 * VARIANT value. The visitor has no mutable state and is thread-safe, so callers may share {@link #INSTANCE}.
 */
public final class VariantTypeValidationVisitor extends PlanNodeVisitor.DepthFirstVisitor<Void, Void> {
  public static final VariantTypeValidationVisitor INSTANCE = new VariantTypeValidationVisitor();

  private VariantTypeValidationVisitor() {
  }

  @Override
  protected boolean traverseStageBoundary() {
    return false;
  }

  @Override
  public Void visitAggregate(AggregateNode node, Void context) {
    List<PlanNode> inputs = node.getInputs();
    if (inputs.size() == 1) {
      validateAggregateInputs(node, inputs.get(0).getDataSchema());
    }
    return super.visitAggregate(node, context);
  }

  /**
   * Validates aggregate operands against their logical input schema.
   *
   * <p>This method is also invoked by the runtime as a defensive check for plans that did not pass through the
   * current broker planner.
   */
  public static void validateAggregateInputs(AggregateNode node, DataSchema inputSchema) {
    validateAggregateInputs(node.getAggCalls(), inputSchema);
  }

  /**
   * Validates aggregate or window-function operands against their logical input schema.
   */
  public static void validateAggregateInputs(List<RexExpression.FunctionCall> aggCalls, DataSchema inputSchema) {
    for (RexExpression.FunctionCall aggCall : aggCalls) {
      if (isRawVariantIndependent(aggCall)) {
        continue;
      }
      for (RexExpression operand : aggCall.getFunctionOperands()) {
        if (!getLogicalType(operand, inputSchema).supportsDirectAggregation()) {
          throw unsupported("Aggregate function " + aggCall.getFunctionName());
        }
      }
    }
  }

  @Override
  public Void visitSort(SortNode node, Void context) {
    DataSchema dataSchema = node.getDataSchema();
    for (RelFieldCollation collation : node.getCollations()) {
      int fieldIndex = collation.getFieldIndex();
      if (!dataSchema.getColumnDataType(fieldIndex).supportsOrdering()) {
        throw unsupported("ORDER BY");
      }
    }
    return super.visitSort(node, context);
  }

  @Override
  public Void visitSetOp(SetOpNode node, Void context) {
    if (!(node.getSetOpType() == SetOpNode.SetOpType.UNION && node.isAll())) {
      for (DataSchema.ColumnDataType dataType : node.getDataSchema().getColumnDataTypes()) {
        if (!dataType.supportsEquality() || !dataType.supportsHashing()) {
          throw unsupported(node.explain().replace('_', ' '));
        }
      }
    }
    return super.visitSetOp(node, context);
  }

  @Override
  public Void visitJoin(JoinNode node, Void context) {
    List<PlanNode> inputs = node.getInputs();
    if (inputs.size() == 2) {
      validateJoinInputs(node, inputs.get(0).getDataSchema(), inputs.get(1).getDataSchema());
    }
    return super.visitJoin(node, context);
  }

  /**
   * Validates equality/hash join keys against both logical input schemas.
   *
   * <p>The runtime also invokes this for mixed-version plans, including LOOKUP joins that do not construct a
   * {@code HashJoinOperator}.
   */
  public static void validateJoinInputs(JoinNode node, DataSchema leftSchema, DataSchema rightSchema) {
    try {
      JoinKeyTypeValidator.validate(node, leftSchema, rightSchema);
    } catch (IllegalArgumentException e) {
      throw new QueryException(QueryErrorCode.QUERY_PLANNING, e.getMessage(), e);
    }
  }

  @Override
  public Void visitWindow(WindowNode node, Void context) {
    List<PlanNode> inputs = node.getInputs();
    if (inputs.size() == 1) {
      validateWindowInputs(node, inputs.get(0).getDataSchema());
    }
    return super.visitWindow(node, context);
  }

  /**
   * Validates window partition keys, ordering keys, and function operands against their logical input schema.
   *
   * <p>This method is also invoked by the runtime as a defensive check for plans that did not pass through the
   * current broker planner.
   */
  public static void validateWindowInputs(WindowNode node, DataSchema inputSchema) {
    for (int key : node.getKeys()) {
      DataSchema.ColumnDataType dataType = inputSchema.getColumnDataType(key);
      if (!dataType.supportsEquality() || !dataType.supportsHashing()) {
        throw unsupported("Window PARTITION BY");
      }
    }
    for (RelFieldCollation collation : node.getCollations()) {
      if (!inputSchema.getColumnDataType(collation.getFieldIndex()).supportsOrdering()) {
        throw unsupported("Window ORDER BY");
      }
    }
    validateAggregateInputs(node.getAggCalls(), inputSchema);
  }

  private static boolean isRawVariantIndependent(RexExpression.FunctionCall aggCall) {
    return !aggCall.isDistinct() && aggCall.getFunctionName().equalsIgnoreCase("COUNT");
  }

  private static DataSchema.ColumnDataType getLogicalType(RexExpression expression, DataSchema inputSchema) {
    if (expression instanceof RexExpression.InputRef) {
      return inputSchema.getColumnDataType(((RexExpression.InputRef) expression).getIndex());
    }
    if (expression instanceof RexExpression.Literal) {
      return ((RexExpression.Literal) expression).getDataType();
    }
    if (expression instanceof RexExpression.FunctionCall) {
      return ((RexExpression.FunctionCall) expression).getDataType();
    }
    throw new IllegalStateException("Unsupported aggregate operand: " + expression.getClass().getName());
  }

  private static QueryException unsupported(String operation) {
    return new QueryException(QueryErrorCode.QUERY_PLANNING,
        operation + " does not support raw VARIANT values; extract a typed path with variantGet first");
  }
}
