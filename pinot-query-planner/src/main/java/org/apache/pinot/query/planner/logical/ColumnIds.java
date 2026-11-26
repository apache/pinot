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
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.PlanNode;


/// Identities of the columns of a stage, so that the spool finder can compare stages whose columns differ.
///
/// The identity of a column says how the stage computes it, not where the column sits. When two stages compute the
/// same rows, two columns with equal identities hold the same value in each row. So a stage can read a column of
/// another stage by identity, even when the two stages project their columns in a different order or project
/// different columns.
///
/// An identity is one of these values, compared with `equals`:
/// - [ScanColumn]: a column that a table scan reads.
/// - [SpoolColumn]: a column that a stage receives from a group of equivalent stages.
/// - [NodeColumn]: an output column of a node that the finder does not map by name, such as an aggregate or a join.
/// - [Call]: a function call over identities.
/// - [RexExpression.Literal]: a literal.
///
/// Expressions are deterministic here: the finder never groups a stage that calls a volatile function. So a call
/// over equal identities computes equal values.
final class ColumnIds {
  private ColumnIds() {
  }

  /// A column that a table scan reads. The scan comparison checks that the two scans read the same table.
  record ScanColumn(String name, ColumnDataType dataType) {
  }

  /// A column that a receive reads from a group of equivalent stages: the ordinal of the column in the union columns
  /// of the group. The group is identified by the stage id of its leader.
  record SpoolColumn(int leaderStageId, int ordinal) {
  }

  /// Output column `index` of a node that two stages compute in the same way, and whose output the finder does not map
  /// by name. `node` is always the node of the stage that the finder compares against (the leader of the group), so
  /// the identities stay comparable across comparisons with the same leader. Nodes are compared by reference.
  static final class NodeColumn {
    private final PlanNode _node;
    private final int _index;

    NodeColumn(PlanNode node, int index) {
      _node = node;
      _index = index;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof NodeColumn)) {
        return false;
      }
      NodeColumn that = (NodeColumn) o;
      return that._node == _node && that._index == _index;
    }

    @Override
    public int hashCode() {
      return 31 * System.identityHashCode(_node) + _index;
    }

    @Override
    public String toString() {
      return "NodeColumn{" + _node.getClass().getSimpleName() + "#" + _index + "}";
    }
  }

  /// A function call over identities. It has the same fields as [RexExpression.FunctionCall].
  record Call(ColumnDataType dataType, String functionName, List<Object> operands, boolean distinct,
              boolean ignoreNulls) {
  }

  /// Returns the identity of an expression, given the identities of the columns of its input.
  static Object of(RexExpression expression, List<?> inputIds) {
    if (expression instanceof RexExpression.InputRef) {
      return inputIds.get(((RexExpression.InputRef) expression).getIndex());
    }
    if (expression instanceof RexExpression.Literal) {
      return expression;
    }
    if (expression instanceof RexExpression.FunctionCall) {
      RexExpression.FunctionCall call = (RexExpression.FunctionCall) expression;
      return new Call(call.getDataType(), call.getFunctionName(), of(call.getFunctionOperands(), inputIds),
          call.isDistinct(), call.isIgnoreNulls());
    }
    throw new IllegalStateException("Unsupported expression: " + expression);
  }

  /// Returns the identities of a list of expressions, given the identities of the columns of their input.
  static List<Object> of(List<RexExpression> expressions, List<?> inputIds) {
    List<Object> result = new ArrayList<>(expressions.size());
    for (RexExpression expression : expressions) {
      result.add(of(expression, inputIds));
    }
    return result;
  }

  /// Returns an expression that computes the identity from an input with the given column identities, or null when
  /// the identity needs a column that the input does not have.
  @Nullable
  static RexExpression toExpression(Object id, List<?> inputIds) {
    int index = inputIds.indexOf(id);
    if (index >= 0) {
      return new RexExpression.InputRef(index);
    }
    if (id instanceof RexExpression.Literal) {
      return (RexExpression.Literal) id;
    }
    if (id instanceof Call) {
      Call call = (Call) id;
      List<RexExpression> operands = new ArrayList<>(call.operands().size());
      for (Object operand : call.operands()) {
        RexExpression expression = toExpression(operand, inputIds);
        if (expression == null) {
          return null;
        }
        operands.add(expression);
      }
      return new RexExpression.FunctionCall(call.dataType(), call.functionName(), operands, call.distinct(),
          call.ignoreNulls());
    }
    return null;
  }

  /// Adds to `result` the scan columns that the identity reads, in first-seen order and without duplicates.
  static void collectScanColumns(Object id, List<ScanColumn> result) {
    if (id instanceof ScanColumn) {
      if (!result.contains(id)) {
        result.add((ScanColumn) id);
      }
    } else if (id instanceof Call) {
      for (Object operand : ((Call) id).operands()) {
        collectScanColumns(operand, result);
      }
    }
  }

  /// Returns true if the identity only reads scan columns and literals, so that a stage that scans the table can
  /// compute it.
  static boolean onlyReadsScanColumns(Object id) {
    if (id instanceof ScanColumn || id instanceof RexExpression.Literal) {
      return true;
    }
    if (id instanceof Call) {
      for (Object operand : ((Call) id).operands()) {
        if (!onlyReadsScanColumns(operand)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
