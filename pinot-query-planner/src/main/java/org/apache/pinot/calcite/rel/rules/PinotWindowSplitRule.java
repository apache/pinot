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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;

/**
 * A RelOptRule to split a single LogicalWindow with multiple window groups
 * into a chain of LogicalWindows, where each has exactly one window group.
 *
 * This version correctly handles window expressions that refer to constants
 * by shifting RexInputRef pointers as the input field count changes down the chain.
 */
public class PinotWindowSplitRule extends RelOptRule {

  public static final PinotWindowSplitRule INSTANCE = new PinotWindowSplitRule();

  private PinotWindowSplitRule() {
    super(operand(LogicalWindow.class, any()), "PinotWindowSplitterRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalWindow window = call.rel(0);
    return window.groups.size() > 1;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalWindow originalWindow = call.rel(0);
    final List<Window.Group> groups = originalWindow.groups;

    RelNode currentInput = originalWindow.getInput();
    final List<RelDataTypeField> originalOutputFields = originalWindow.getRowType().getFieldList();
    final int originalInputFieldCount = currentInput.getRowType().getFieldCount();
    final RelDataTypeFactory typeFactory = originalWindow.getCluster().getTypeFactory();

    int cumulativeAggFieldCount = 0;

    for (int i = 0; i < groups.size(); i++) {
      Window.Group group = groups.get(i);
      final RelDataType currentInputType = currentInput.getRowType();
      final int currentInputFieldCount = currentInputType.getFieldCount();

      // Only shift if this is not the first window in the chain.
      if (i > 0) {
        int shift = currentInputFieldCount - originalInputFieldCount;
        if (shift > 0) {
          RexConstantRefShifter shifter = new RexConstantRefShifter(originalInputFieldCount, shift);
          group = shifter.apply(group);
        }
      }

      // 1. Determine the RowType for the new single-group window.
      List<RelDataTypeField> newWindowFields = new ArrayList<>(currentInputType.getFieldList());
      for (int j = 0; j < group.aggCalls.size(); j++) {
        int fieldIndexInOriginal = originalInputFieldCount + cumulativeAggFieldCount + j;
        newWindowFields.add(originalOutputFields.get(fieldIndexInOriginal));
      }
      final RelDataType newWindowRowType = typeFactory.createStructType(newWindowFields);
      cumulativeAggFieldCount += group.aggCalls.size();

      // 2. Create the new LogicalWindow with the (potentially shifted) group.
      // The newly created window becomes the input for the next iteration.
      currentInput = new LogicalWindow(
          originalWindow.getCluster(),
          originalWindow.getTraitSet(),
          originalWindow.getHints(),
          currentInput,
          originalWindow.getConstants(),
          newWindowRowType,
          ImmutableList.of(group));
    }
    call.transformTo(currentInput);
  }

  /**
   * A RexShuttle that shifts indices of RexInputRefs that point to constants.
   *
   * A RexInputRef can point to an input field or a constant. If its index is >= originalInputFieldCount,
   * it's a constant. When we chain windows, the input field count for subsequent windows increases,
   * so we must shift the indices for these constant references to avoid them being misinterpreted
   * as input field references.
   */
  static class RexConstantRefShifter extends RexShuttle {
    private final int _originalInputFieldCount;
    private final int _shift;

    RexConstantRefShifter(int originalInputFieldCount, int shift) {
      _originalInputFieldCount = originalInputFieldCount;
      _shift = shift;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      // If the index is greater than or equal to the original number of input fields,
      // it refers to a constant, so we must shift it.
      if (index >= _originalInputFieldCount) {
        return new RexInputRef(index + _shift, inputRef.getType());
      }
      // Otherwise, it's a reference to a field from the original input relation,
      // which does not need shifting.
      return inputRef;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (call instanceof Window.RexWinAggCall) {
        Window.RexWinAggCall winCall = (Window.RexWinAggCall) call;
        List<RexNode> newOperands = winCall.getOperands().stream()
            .map(operand -> operand.accept(this))
            .collect(Collectors.toList());
        return new Window.RexWinAggCall(
            (SqlAggFunction) winCall.getOperator(),
            winCall.getType(),
            newOperands,
            winCall.ordinal,
            winCall.distinct,
            winCall.ignoreNulls
        );
      }
      return super.visitCall(call);
    }

    /**
     * Applies the shuttle to all expressions within a Window.Group.
     */
    public Window.Group apply(Window.Group group) {
      List<Window.RexWinAggCall> newAggCalls = group.aggCalls.stream()
          .map(agg -> (Window.RexWinAggCall) agg.accept(this))
          .collect(Collectors.toList());

      RexWindowBound newLowerBound = group.lowerBound.accept(this);
      RexWindowBound newUpperBound = group.upperBound.accept(this);

      return new Window.Group(
          group.keys,
          group.isRows,
          newLowerBound,
          newUpperBound,
          group.exclude,
          group.orderKeys,
          newAggCalls
      );
    }
  }
}
