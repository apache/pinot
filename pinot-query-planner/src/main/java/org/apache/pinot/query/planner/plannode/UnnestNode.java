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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


/**
 * UnnestNode models UNNEST/UNNEST(CROSS JOIN) semantics: expand array/collection expressions into multiple rows.
 * Supports multiple arrays, aligning them by index (like a zip operation).
 * If arrays have different lengths, shorter arrays are padded with null values.
 *
 * DataSchema on this node reflects the output schema post-expansion, typically input columns plus the element columns.
 */
public class UnnestNode extends BasePlanNode {
  public static final int UNSPECIFIED_INDEX = -1;

  private final List<RexExpression> _arrayExprs;
  private final List<String> _columnAliases;
  private final boolean _withOrdinality;
  @Nullable
  private final String _ordinalityAlias;
  // Absolute output indexes in the stage schema; -1 means unspecified
  private final List<Integer> _elementIndexes;
  private final int _ordinalityIndex;

  public UnnestNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RexExpression> arrayExprs, List<String> columnAliases, boolean withOrdinality,
      @Nullable String ordinalityAlias) {
    super(stageId, dataSchema, nodeHint, inputs);
    _arrayExprs = arrayExprs;
    _columnAliases = columnAliases;
    _withOrdinality = withOrdinality;
    _ordinalityAlias = ordinalityAlias;
    _elementIndexes = new ArrayList<>();
    for (int i = 0; i < arrayExprs.size(); i++) {
      _elementIndexes.add(UNSPECIFIED_INDEX);
    }
    _ordinalityIndex = UNSPECIFIED_INDEX;
  }

  public UnnestNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RexExpression> arrayExprs, List<String> columnAliases, boolean withOrdinality,
      @Nullable String ordinalityAlias, List<Integer> elementIndexes, int ordinalityIndex) {
    super(stageId, dataSchema, nodeHint, inputs);
    _arrayExprs = arrayExprs;
    _columnAliases = columnAliases;
    _withOrdinality = withOrdinality;
    _ordinalityAlias = ordinalityAlias;
    _elementIndexes = elementIndexes;
    _ordinalityIndex = ordinalityIndex;
  }

  // Backward compatibility constructor for single array
  public UnnestNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      RexExpression arrayExpr, @Nullable String columnAlias, boolean withOrdinality, @Nullable String ordinalityAlias) {
    this(stageId, dataSchema, nodeHint, inputs, List.of(arrayExpr),
        columnAlias != null ? List.of(columnAlias) : List.of(), withOrdinality, ordinalityAlias);
  }

  // Backward compatibility constructor for single array with indexes
  public UnnestNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      RexExpression arrayExpr, @Nullable String columnAlias, boolean withOrdinality, @Nullable String ordinalityAlias,
      int elementIndex, int ordinalityIndex) {
    this(stageId, dataSchema, nodeHint, inputs, List.of(arrayExpr),
        columnAlias != null ? List.of(columnAlias) : List.of(), withOrdinality, ordinalityAlias,
        List.of(elementIndex), ordinalityIndex);
  }

  public List<RexExpression> getArrayExprs() {
    return _arrayExprs;
  }

  // Backward compatibility method
  public RexExpression getArrayExpr() {
    return _arrayExprs.isEmpty() ? null : _arrayExprs.get(0);
  }

  public List<String> getColumnAliases() {
    return _columnAliases;
  }

  // Backward compatibility method
  @Nullable
  public String getColumnAlias() {
    return _columnAliases.isEmpty() ? null : _columnAliases.get(0);
  }

  public boolean isWithOrdinality() {
    return _withOrdinality;
  }

  @Nullable
  public String getOrdinalityAlias() {
    return _ordinalityAlias;
  }

  public List<Integer> getElementIndexes() {
    return _elementIndexes;
  }

  // Backward compatibility method
  public int getElementIndex() {
    return _elementIndexes.isEmpty() ? UNSPECIFIED_INDEX : _elementIndexes.get(0);
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
    return new UnnestNode(_stageId, _dataSchema, _nodeHint, inputs, _arrayExprs, _columnAliases, _withOrdinality,
        _ordinalityAlias, _elementIndexes, _ordinalityIndex);
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
    return Objects.equals(_arrayExprs, that._arrayExprs) && Objects.equals(_columnAliases, that._columnAliases)
        && _withOrdinality == that._withOrdinality && Objects.equals(_ordinalityAlias, that._ordinalityAlias)
        && Objects.equals(_elementIndexes, that._elementIndexes) && _ordinalityIndex == that._ordinalityIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _arrayExprs, _columnAliases, _withOrdinality, _ordinalityAlias,
        _elementIndexes, _ordinalityIndex);
  }
}
