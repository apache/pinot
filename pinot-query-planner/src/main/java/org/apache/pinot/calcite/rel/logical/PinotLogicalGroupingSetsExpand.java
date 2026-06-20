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
package org.apache.pinot.calcite.rel.logical;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.request.context.GroupingSets;


/**
 * Logical rel for the native multi-stage {@code GROUP BY GROUPING SETS / ROLLUP / CUBE} expansion. It produces, per
 * input row, one output row per grouping set: the union group-by columns are nulled where rolled up, and a synthetic
 * INT {@link GroupingSets#GROUPING_ID_COLUMN} ({@code $groupingId}) column is appended carrying the per-set rolled-up
 * bitmask. {@code GroupingSetsExpander} produces this rel beneath an ordinary aggregate keyed on
 * {@code [unionGroupKeys..., $groupingId]}, which the standard {@code PinotAggregateExchangeNodeInsertRule} then splits
 * into LEAF / hash-exchange / FINAL like any other aggregate.
 *
 * <p>Output row type: the input row type with every union group-by column widened to nullable (since it may be nulled)
 * plus a trailing {@code $groupingId} INT column.
 */
public class PinotLogicalGroupingSetsExpand extends SingleRel {
  private final List<Integer> _groupingColumns;
  private final List<Integer> _groupingIds;
  private final RelDataType _rowType;

  private PinotLogicalGroupingSetsExpand(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      RelDataType rowType, List<Integer> groupingColumns, List<Integer> groupingIds) {
    super(cluster, traitSet, input);
    _rowType = rowType;
    _groupingColumns = List.copyOf(groupingColumns);
    _groupingIds = List.copyOf(groupingIds);
  }

  /**
   * Creates an expand rel over {@code input}. {@code groupingColumns} are the input field indexes of the union
   * group-by columns (in union order); {@code groupingIds} are the per-set rolled-up bitmasks (bit {@code p} set iff
   * {@code groupingColumns.get(p)} is rolled up in that set).
   */
  public static PinotLogicalGroupingSetsExpand create(RelNode input, List<Integer> groupingColumns,
      List<Integer> groupingIds) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traitSet = input.getTraitSet().replace(Convention.NONE);
    RelDataType rowType = buildRowType(cluster.getTypeFactory(), input.getRowType(), groupingColumns);
    return new PinotLogicalGroupingSetsExpand(cluster, traitSet, input, rowType, groupingColumns, groupingIds);
  }

  private static RelDataType buildRowType(RelDataTypeFactory typeFactory, RelDataType inputRowType,
      List<Integer> groupingColumns) {
    Set<Integer> unionColumns = new HashSet<>(groupingColumns);
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    List<RelDataTypeField> fields = inputRowType.getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      RelDataType type =
          unionColumns.contains(i) ? typeFactory.createTypeWithNullability(field.getType(), true) : field.getType();
      builder.add(field.getName(), type);
    }
    builder.add(GroupingSets.GROUPING_ID_COLUMN, typeFactory.createSqlType(SqlTypeName.INTEGER));
    return builder.build();
  }

  public List<Integer> getGroupingColumns() {
    return _groupingColumns;
  }

  public List<Integer> getGroupingIds() {
    return _groupingIds;
  }

  @Override
  protected RelDataType deriveRowType() {
    return _rowType;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    RelNode input = sole(inputs);
    RelDataType rowType = input == getInput() ? _rowType
        : buildRowType(getCluster().getTypeFactory(), input.getRowType(), _groupingColumns);
    return new PinotLogicalGroupingSetsExpand(getCluster(), traitSet, input, rowType, _groupingColumns, _groupingIds);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("groupingColumns", _groupingColumns).item("groupingIds", _groupingIds);
  }
}
