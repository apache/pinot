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
package org.apache.pinot.query.planner.stage;

import com.clearspring.analytics.util.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class WindowNode extends AbstractStageNode {
  @ProtoProperties
  public List<RexExpression> _groupSet;
  @ProtoProperties
  public List<RexExpression> _orderSet;
  @ProtoProperties
  public List<RelFieldCollation.Direction> _orderSetDirection;
  @ProtoProperties
  public List<RelFieldCollation.NullDirection> _orderSetNullDirection;
  @ProtoProperties
  public List<RexExpression> _aggCalls;
  @ProtoProperties
  public int _lowerBound;
  @ProtoProperties
  public int _upperBound;
  @ProtoProperties
  public boolean _isRows;
  @ProtoProperties
  private List<RexExpression> _constants;

  public WindowNode(int stageId) {
    super(stageId);
  }

  public WindowNode(int stageId, List<Window.Group> windowGroups, List<RexLiteral> constants, DataSchema dataSchema) {
    super(stageId, dataSchema);
    // Only a single Window Group should exist per WindowNode.
    Preconditions.checkState(windowGroups.size() == 1,
        String.format("Only a single window group is allowed! Number of window groups: %d", windowGroups.size()));
    Window.Group windowGroup = windowGroups.get(0);

    _groupSet = windowGroup.keys == null ? new ArrayList<>() : RexExpression.toRexInputRefs(windowGroup.keys);
    List<RelFieldCollation> relFieldCollations = windowGroup.orderKeys == null ? new ArrayList<>()
        : windowGroup.orderKeys.getFieldCollations();
    _orderSet = new ArrayList<>(relFieldCollations.size());
    _orderSetDirection = new ArrayList<>(relFieldCollations.size());
    _orderSetNullDirection = new ArrayList<>(relFieldCollations.size());
    for (RelFieldCollation relFieldCollation : relFieldCollations) {
      _orderSet.add(new RexExpression.InputRef(relFieldCollation.getFieldIndex()));
      _orderSetDirection.add(relFieldCollation.direction);
      _orderSetNullDirection.add(relFieldCollation.nullDirection);
    }
    _aggCalls = windowGroup.aggCalls.stream().map(RexExpression::toRexExpression).collect(Collectors.toList());

    // TODO: For now only the default frame is supported. Add support for custom frames including rows support.
    //       Frame literals come in the constants from the LogicalWindow and the bound.getOffset() stores the
    //       InputRef to the constants array offset by the input array length. These need to be extracted here and
    //       set to the bounds.
    validateFrameBounds(windowGroup.lowerBound, windowGroup.upperBound, windowGroup.isRows);
    // Lower bound can only be unbounded preceding for now, set to Integer.MIN_VALUE
    _lowerBound = Integer.MIN_VALUE;
    // Upper bound can only be unbounded following or current row for now
    _upperBound = windowGroup.upperBound.isUnbounded() ? Integer.MAX_VALUE : 0;
    _isRows = windowGroup.isRows;

    // TODO: Constants are used to store constants needed such as the frame literals. For now just save this, need to
    //       extract the constant values into bounds as a part of frame support.
    _constants = new ArrayList<>();
    for (RexLiteral constant : constants) {
      _constants.add(RexExpression.toRexExpression(constant));
    }
  }

  @Override
  public String explain() {
    return "WINDOW";
  }

  @Override
  public <T, C> T visit(StageNodeVisitor<T, C> visitor, C context) {
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

  public boolean isRows() {
    return _isRows;
  }

  public List<RexExpression> getConstants() {
    return _constants;
  }

  private void validateFrameBounds(RexWindowBound lowerBound, RexWindowBound upperBound, boolean isRows) {
    Preconditions.checkState(!isRows, "Only default frame is supported which must be RANGE and not ROWS");
    Preconditions.checkState(lowerBound.isPreceding() && lowerBound.isUnbounded()
            && lowerBound.getOffset() == null,
        String.format("Only default frame is supported, actual lower bound frame provided: %s", lowerBound));
    if (_orderSet.isEmpty()) {
      Preconditions.checkState(upperBound.isFollowing() && upperBound.isUnbounded()
              && upperBound.getOffset() == null,
          String.format("Only default frame is supported, actual upper bound frame provided: %s", upperBound));
    } else {
      Preconditions.checkState(upperBound.isCurrentRow() && upperBound.getOffset() == null,
          String.format("Only default frame is supported, actual upper bound frame provided: %s", upperBound));
    }
  }
}
