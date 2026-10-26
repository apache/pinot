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
package org.apache.pinot.calcite.rel.rules;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.sql.fun.ArrayToMvFunctionName;


/// Detects and rewrites the `GROUP BY ARRAY_TO_MV(expr)` keys of an aggregate.
///
/// Calcite puts every group expression in a `Project` under the aggregate. A group key is an MV key when its
/// `Project` expression is exactly `ARRAY_TO_MV(expr)` and `expr` has the SQL type `ARRAY`. Only the top-level call
/// is detected: a key like `CAST(ARRAY_TO_MV(expr) AS VARCHAR)` or `LOWER(ARRAY_TO_MV(expr))` is not an MV key.
///
/// The class is stateless and thread-safe.
public final class ArrayToMvGroupKeys {
  private ArrayToMvGroupKeys() {
  }

  /// Returns the positions in the group set of the keys whose input expression is `ARRAY_TO_MV(expr)` with an
  /// array-typed `expr`, in ascending order. Returns an empty list when the input of the aggregate is not a
  /// `Project` or when no key matches.
  public static List<Integer> detect(Aggregate aggRel) {
    RelNode input = PinotRuleUtils.unboxRel(aggRel.getInput());
    if (!(input instanceof Project)) {
      return List.of();
    }
    List<RexNode> projects = ((Project) input).getProjects();
    List<Integer> mvKeys = new ArrayList<>();
    ImmutableBitSet groupSet = aggRel.getGroupSet();
    int position = 0;
    for (int column : groupSet) {
      if (isArrayToMvCall(projects.get(column))) {
        mvKeys.add(position);
      }
      position++;
    }
    return mvKeys;
  }

  /// Returns a copy of `project` where the expression of each column in `columns` is replaced by the operand of its
  /// `ARRAY_TO_MV` call. The row type of the copy gives these columns the array type of the operand.
  ///
  /// @param columns indexes of `Project` columns whose expression is `ARRAY_TO_MV(expr)`
  public static Project rewriteProject(Project project, List<Integer> columns) {
    List<RexNode> projects = new ArrayList<>(project.getProjects());
    for (int column : columns) {
      RexNode expression = projects.get(column);
      if (!isArrayToMvCall(expression)) {
        throw new IllegalArgumentException(
            "Project column " + column + " is not an ARRAY_TO_MV call on an array: " + expression);
      }
      projects.set(column, ((RexCall) expression).getOperands().get(0));
    }
    RelDataType rowType = RexUtil.createStructType(project.getCluster().getTypeFactory(), projects,
        project.getRowType().getFieldNames(), null);
    return project.copy(project.getTraitSet(), project.getInput(), projects, rowType);
  }

  /// Returns the indexes of the input columns of the MV keys at `mvKeyIndexes` (positions in the group set).
  public static List<Integer> toInputColumns(Aggregate aggRel, List<Integer> mvKeyIndexes) {
    List<Integer> groupColumns = aggRel.getGroupSet().asList();
    List<Integer> columns = new ArrayList<>(mvKeyIndexes.size());
    for (int mvKeyIndex : mvKeyIndexes) {
      columns.add(groupColumns.get(mvKeyIndex));
    }
    return columns;
  }

  /// Returns whether `expression` is a call to `ARRAY_TO_MV` whose operand has the SQL type `ARRAY`.
  public static boolean isArrayToMvCall(RexNode expression) {
    if (!(expression instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) expression;
    if (call.getOperands().size() != 1 || !ArrayToMvFunctionName.matches(call.getOperator().getName())) {
      return false;
    }
    return call.getOperands().get(0).getType().getSqlTypeName() == SqlTypeName.ARRAY;
  }
}
