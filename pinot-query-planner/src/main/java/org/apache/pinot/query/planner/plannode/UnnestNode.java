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
  private final TableFunctionContext _tableFunctionContext;

  public UnnestNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RexExpression> arrayExprs, List<String> columnAliases, boolean withOrdinality,
      @Nullable String ordinalityAlias) {
    this(stageId, dataSchema, nodeHint, inputs, arrayExprs,
        new TableFunctionContext(columnAliases, withOrdinality, ordinalityAlias,
            defaultElementIndexes(arrayExprs.size()), UNSPECIFIED_INDEX));
  }

  public UnnestNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RexExpression> arrayExprs, List<String> columnAliases, boolean withOrdinality,
      @Nullable String ordinalityAlias, List<Integer> elementIndexes, int ordinalityIndex) {
    this(stageId, dataSchema, nodeHint, inputs, arrayExprs,
        new TableFunctionContext(columnAliases, withOrdinality, ordinalityAlias, elementIndexes, ordinalityIndex));
  }

  public UnnestNode(int stageId, DataSchema dataSchema, NodeHint nodeHint, List<PlanNode> inputs,
      List<RexExpression> arrayExprs, TableFunctionContext tableFunctionContext) {
    super(stageId, dataSchema, nodeHint, inputs);
    _arrayExprs = arrayExprs;
    _tableFunctionContext = tableFunctionContext;
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

  public TableFunctionContext getTableFunctionContext() {
    return _tableFunctionContext;
  }

  public List<String> getColumnAliases() {
    return _tableFunctionContext.getColumnAliases();
  }

  // Backward compatibility method
  @Nullable
  public String getColumnAlias() {
    return getColumnAliases().isEmpty() ? null : getColumnAliases().get(0);
  }

  public boolean isWithOrdinality() {
    return _tableFunctionContext.isWithOrdinality();
  }

  @Nullable
  public String getOrdinalityAlias() {
    return _tableFunctionContext.getOrdinalityAlias();
  }

  public List<Integer> getElementIndexes() {
    return _tableFunctionContext.getElementIndexes();
  }

  // Backward compatibility method
  public int getElementIndex() {
    return getElementIndexes().isEmpty() ? UNSPECIFIED_INDEX : getElementIndexes().get(0);
  }

  public int getOrdinalityIndex() {
    return _tableFunctionContext.getOrdinalityIndex();
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
    return new UnnestNode(_stageId, _dataSchema, _nodeHint, inputs, _arrayExprs, _tableFunctionContext.copy());
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
    return Objects.equals(_arrayExprs, that._arrayExprs)
        && Objects.equals(_tableFunctionContext, that._tableFunctionContext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _arrayExprs, _tableFunctionContext);
  }

  private static List<Integer> defaultElementIndexes(int count) {
    List<Integer> indexes = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      indexes.add(UNSPECIFIED_INDEX);
    }
    return indexes;
  }

  /**
   * Encapsulates standard SQL table function metadata (column aliases, ordinality) for UNNEST.
   */
  public static final class TableFunctionContext {
    private final List<String> _columnAliases;
    private final boolean _withOrdinality;
    @Nullable
    private final String _ordinalityAlias;
    private final List<Integer> _elementIndexes;
    private final int _ordinalityIndex;

    public TableFunctionContext(List<String> columnAliases, boolean withOrdinality, @Nullable String ordinalityAlias,
        List<Integer> elementIndexes, int ordinalityIndex) {
      _columnAliases = List.copyOf(columnAliases);
      _withOrdinality = withOrdinality;
      _ordinalityAlias = ordinalityAlias;
      _elementIndexes = List.copyOf(elementIndexes);
      _ordinalityIndex = ordinalityIndex;
    }

    public List<String> getColumnAliases() {
      return _columnAliases;
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

    public int getOrdinalityIndex() {
      return _ordinalityIndex;
    }

    public TableFunctionContext copy() {
      return new TableFunctionContext(_columnAliases, _withOrdinality, _ordinalityAlias, _elementIndexes,
          _ordinalityIndex);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TableFunctionContext)) {
        return false;
      }
      TableFunctionContext that = (TableFunctionContext) o;
      return _withOrdinality == that._withOrdinality && _ordinalityIndex == that._ordinalityIndex
          && Objects.equals(_columnAliases, that._columnAliases)
          && Objects.equals(_ordinalityAlias, that._ordinalityAlias)
          && Objects.equals(_elementIndexes, that._elementIndexes);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_columnAliases, _withOrdinality, _ordinalityAlias, _elementIndexes, _ordinalityIndex);
    }
  }
}
