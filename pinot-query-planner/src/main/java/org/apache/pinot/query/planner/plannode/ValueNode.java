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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexLiteral;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.planner.serde.ProtoProperties;


public class ValueNode extends AbstractPlanNode {
  @ProtoProperties
  private List<List<RexExpression>> _literalRows;

  public ValueNode(int planFragmentId) {
    super(planFragmentId);
  }

  public ValueNode(int currentStageId, DataSchema dataSchema, ImmutableList<ImmutableList<RexLiteral>> literalTuples) {
    super(currentStageId, dataSchema);
    _literalRows = new ArrayList<>();
    for (List<RexLiteral> literalTuple : literalTuples) {
      List<RexExpression> literalRow = new ArrayList<>();
      for (RexLiteral literal : literalTuple) {
        if (literal == null) {
          literalRow.add(null);
          continue;
        }
        literalRow.add(RexExpressionUtils.fromRexLiteral(literal));
      }
      _literalRows.add(literalRow);
    }
  }

  public List<List<RexExpression>> getLiteralRows() {
    return _literalRows;
  }

  @Override
  public String explain() {
    return "LITERAL";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitValue(this, context);
  }
}
