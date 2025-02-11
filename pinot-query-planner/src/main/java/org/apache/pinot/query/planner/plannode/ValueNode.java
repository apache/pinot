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

import java.util.List;
import java.util.Objects;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class ValueNode extends BasePlanNode {
  private final List<List<RexExpression.Literal>> _literalRows;

  public ValueNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<List<RexExpression.Literal>> literalRows) {
    super(stageId, dataSchema, nodeHint, inputs);
    _literalRows = literalRows;
  }

  public List<List<RexExpression.Literal>> getLiteralRows() {
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

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new ValueNode(_stageId, _dataSchema, _nodeHint, inputs, _literalRows);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ValueNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ValueNode valueNode = (ValueNode) o;
    return Objects.equals(_literalRows, valueNode._literalRows);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _literalRows);
  }
}
