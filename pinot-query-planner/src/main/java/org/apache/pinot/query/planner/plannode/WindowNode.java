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
package org.apache.pinot.query.planner.plannode;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexLiteral;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class WindowNode extends AbstractPlanNode {
  @ProtoProperties
  private List<RexExpression> _groupSet;
  @ProtoProperties
  private List<RexExpression> _orderSet;
  @ProtoProperties
  private List<RelFieldCollation.Direction> _orderSetDirection;
  @ProtoProperties
  private List<RelFieldCollation.NullDirection> _orderSetNullDirection;
  @ProtoProperties
  private List<RexExpression> _aggCalls;
  @ProtoProperties
  private int _lowerBound;
  @ProtoProperties
  private int _upperBound;
  @ProtoProperties
  private List<RexExpression> _constants;
  @ProtoProperties
  private WindowFrameType _windowFrameType;

  /**
   * Enum to denote the type of window frame
   * ROWS - ROWS type window frame
   * RANGE - RANGE type window frame
   */
  public enum WindowFrameType {
    ROWS, RANGE
  }

  public WindowNode(int planFragmentId) {
    super(planFragmentId);
  }

  public WindowNode(int planFragmentId, List<Window.Group> windowGroups, List<RexLiteral> constants,
      DataSchema dataSchema) {
    super(planFragmentId, dataSchema);
    // Only a single Window Group should exist per WindowNode.
    Preconditions.checkState(windowGroups.size() == 1,
        String.format("Only a single window group is allowed! Number of window groups: %d", windowGroups.size()));
    Window.Group windowGroup = windowGroups.get(0);

    _groupSet = windowGroup.keys == null ? Collections.emptyList() : RexExpressionUtils.fromInputRefs(windowGroup.keys);
    List<RelFieldCollation> relFieldCollations =
        windowGroup.orderKeys == null ? new ArrayList<>() : windowGroup.orderKeys.getFieldCollations();
    _orderSet = new ArrayList<>(relFieldCollations.size());
    _orderSetDirection = new ArrayList<>(relFieldCollations.size());
    _orderSetNullDirection = new ArrayList<>(relFieldCollations.size());
    for (RelFieldCollation relFieldCollation : relFieldCollations) {
      _orderSet.add(new RexExpression.InputRef(relFieldCollation.getFieldIndex()));
      _orderSetDirection.add(relFieldCollation.direction);
      _orderSetNullDirection.add(relFieldCollation.nullDirection);
    }
    _aggCalls = windowGroup.aggCalls.stream().map(RexExpressionUtils::fromRexCall).collect(Collectors.toList());

    // TODO: For now only the default frame is supported. Add support for custom frames including rows support.
    //       Frame literals come in the constants from the LogicalWindow and the bound.getOffset() stores the
    //       InputRef to the constants array offset by the input array length. These need to be extracted here and
    //       set to the bounds.

    // Lower bound can only be unbounded preceding for now, set to Integer.MIN_VALUE
    _lowerBound = Integer.MIN_VALUE;
    // Upper bound can only be unbounded following or current row for now
    _upperBound = windowGroup.upperBound.isUnbounded() ? Integer.MAX_VALUE : 0;
    _windowFrameType = windowGroup.isRows ? WindowFrameType.ROWS : WindowFrameType.RANGE;

    // TODO: Constants are used to store constants needed such as the frame literals. For now just save this, need to
    //       extract the constant values into bounds as a part of frame support.
    _constants = new ArrayList<>();
    for (RexLiteral constant : constants) {
      _constants.add(RexExpressionUtils.fromRexLiteral(constant));
    }
  }

  @Override
  public String explain() {
    return "WINDOW";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitWindow(this, context);
  }

  public List<RexExpression> getGroupSet() {
    return _groupSet;
  }

  public List<RexExpression> getOrderSet() {
    return _orderSet;
  }

  public List<RelFieldCollation.Direction> getOrderSetDirection() {
    return _orderSetDirection;
  }

  public List<RelFieldCollation.NullDirection> getOrderSetNullDirection() {
    return _orderSetNullDirection;
  }

  public List<RexExpression> getAggCalls() {
    return _aggCalls;
  }

  public int getLowerBound() {
    return _lowerBound;
  }

  public int getUpperBound() {
    return _upperBound;
  }

  public WindowFrameType getWindowFrameType() {
    return _windowFrameType;
  }

  public List<RexExpression> getConstants() {
    return _constants;
  }
}
