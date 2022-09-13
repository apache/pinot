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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class WindowNode extends AbstractStageNode {
  @ProtoProperties
  private List<PinotWindow> _windows;

  public WindowNode(int stageId) {
    super(stageId);
  }

  public WindowNode(int stageId, List<Window.Group> windowGroups, List<RexLiteral> constants,
      DataSchema dataSchema) {
    super(stageId, dataSchema);
    _windows = new ArrayList<>(windowGroups.size());
    for (Window.Group group : windowGroups) {
      _windows.add(new PinotWindow(group.keys, group.orderKeys, group.aggCalls, group.lowerBound, group.upperBound));
    }
  }

  public static class PinotWindow {
    public List<RexExpression> _groupSet;
    public List<RexExpression> _orderSet;
    public List<RexExpression> _aggCalls;
    public int _lowerBound;
    public int _upperBound;

    public PinotWindow(ImmutableBitSet keys, RelCollation orderKeys, ImmutableList<Window.RexWinAggCall> winAggCalls,
        RexWindowBound lowerBound, RexWindowBound upperBound) {
      _groupSet = RexExpression.toRexInputRefs(keys);
      _orderSet = RexExpression.toRexInputRefs(orderKeys.getKeys());
      _aggCalls = winAggCalls.stream().map(RexExpression::toRexExpression).collect(Collectors.toList());
      // TODO: support isPreceding, isFollowing, isUnbounded.
      Preconditions.checkState(lowerBound.getOffset() == null || lowerBound.getOffset().isA(SqlKind.LITERAL),
          "only literal bounds are supported");
      Preconditions.checkState(upperBound.getOffset() == null || upperBound.getOffset().isA(SqlKind.LITERAL),
          "only literal bounds are supported");
      _lowerBound = lowerBound.isUnbounded() ? Integer.MIN_VALUE : lowerBound.isCurrentRow() ? 0 :
          ((RexLiteral) lowerBound.getOffset()).getValueAs(Integer.class) * (lowerBound.isPreceding() ? -1 : 1);
      _upperBound = upperBound.isUnbounded() ? Integer.MIN_VALUE : upperBound.isCurrentRow() ? 0 :
          ((RexLiteral) upperBound.getOffset()).getValueAs(Integer.class) * (upperBound.isPreceding() ? -1 : 1);
    }
  }
}
