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
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


/**
 * UnnestNode models UNNEST/UNNEST(CROSS JOIN) semantics: expand an array/collection expression into multiple rows.
 *
 * DataSchema on this node reflects the output schema post-expansion, typically input columns plus the element column.
 */
public class UnnestNode extends BasePlanNode {
  private final RexExpression _arrayExpr;
  @Nullable
  private final String _columnAlias;
  private final boolean _withOrdinality;
  @Nullable
  private final String _ordinalityAlias;
  // Absolute output indexes in the stage schema; -1 means unspecified
  private final int _elementIndex;
  private final int _ordinalityIndex;

  public UnnestNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      RexExpression arrayExpr, @Nullable String columnAlias, boolean withOrdinality, @Nullable String ordinalityAlias) {
    super(stageId, dataSchema, nodeHint, inputs);
    _arrayExpr = arrayExpr;
    _columnAlias = columnAlias;
    _withOrdinality = withOrdinality;
    _ordinalityAlias = ordinalityAlias;
    _elementIndex = -1;
    _ordinalityIndex = -1;
  }

  public UnnestNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      RexExpression arrayExpr, @Nullable String columnAlias, boolean withOrdinality, @Nullable String ordinalityAlias,
      int elementIndex, int ordinalityIndex) {
    super(stageId, dataSchema, nodeHint, inputs);
    _arrayExpr = arrayExpr;
    _columnAlias = columnAlias;
    _withOrdinality = withOrdinality;
    _ordinalityAlias = ordinalityAlias;
    _elementIndex = elementIndex;
    _ordinalityIndex = ordinalityIndex;
  }

  public RexExpression getArrayExpr() {
    return _arrayExpr;
  }

  @Nullable
  public String getColumnAlias() {
    return _columnAlias;
  }

  public boolean isWithOrdinality() {
    return _withOrdinality;
  }

  @Nullable
  public String getOrdinalityAlias() {
    return _ordinalityAlias;
  }

  public int getElementIndex() {
    return _elementIndex;
  }

  public int getOrdinalityIndex() {
    return _ordinalityIndex;
  }

  @Override
  public String explain() {
    return "UNNEST";
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitUnnest(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new UnnestNode(_stageId, _dataSchema, _nodeHint, inputs, _arrayExpr, _columnAlias, _withOrdinality,
        _ordinalityAlias, _elementIndex, _ordinalityIndex);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UnnestNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    UnnestNode that = (UnnestNode) o;
    return Objects.equals(_arrayExpr, that._arrayExpr) && Objects.equals(_columnAlias, that._columnAlias)
        && _withOrdinality == that._withOrdinality && Objects.equals(_ordinalityAlias, that._ordinalityAlias)
        && _elementIndex == that._elementIndex && _ordinalityIndex == that._ordinalityIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _arrayExpr, _columnAlias, _withOrdinality, _ordinalityAlias, _elementIndex,
        _ordinalityIndex);
  }
}
