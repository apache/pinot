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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;


/// An aggregate that groups each input row by every element of its multi-value (MV) group keys, with the
/// single-stage engine semantics: a row contributes once to the group of each element, a repeated element counts
/// once per copy, an empty array contributes to no group, and several MV keys give the cartesian product.
///
/// [#getMvKeyIndexes()] holds positions in the group set (ascending). The input column of an MV key is an array;
/// the output column is its component type, with the nullability of the input column. Every other field of the row
/// type is the one of a plain [Aggregate], so a parent that referenced a plain aggregate over `ARRAY_TO_MV(e)`
/// keeps its references.
///
/// This class does not extend [PinotLogicalAggregate] on purpose: a rule or converter that only knows the plain
/// aggregate must not treat an MV aggregate as one. Grouping sets are not supported.
///
/// Instances are immutable and thread-safe, like every Calcite [RelNode].
public class PinotLogicalMvAggregate extends Aggregate {
  private final AggType _aggType;
  private final boolean _leafReturnFinalResult;
  // The following fields are set when group trim is enabled, and are extracted from the Sort on top of this Aggregate.
  private final List<RelFieldCollation> _collations;
  private final int _limit;
  private final List<Integer> _mvKeyIndexes;

  public PinotLogicalMvAggregate(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelNode input,
      ImmutableBitSet groupSet, @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls,
      AggType aggType, boolean leafReturnFinalResult, @Nullable List<RelFieldCollation> collations, int limit,
      List<Integer> mvKeyIndexes) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
    if (getGroupType() != Group.SIMPLE) {
      throw new IllegalArgumentException("Grouping sets are not supported by the MV aggregate, got: " + this.groupSets);
    }
    int groupCount = groupSet.cardinality();
    int previous = -1;
    for (int mvKeyIndex : mvKeyIndexes) {
      if (mvKeyIndex <= previous || mvKeyIndex >= groupCount) {
        throw new IllegalArgumentException(
            "MV key indexes must be ascending positions in the group set of size " + groupCount + ", got: "
                + mvKeyIndexes);
      }
      previous = mvKeyIndex;
    }
    _aggType = aggType;
    _leafReturnFinalResult = leafReturnFinalResult;
    _collations = collations;
    _limit = limit;
    _mvKeyIndexes = List.copyOf(mvKeyIndexes);
  }

  public AggType getAggType() {
    return _aggType;
  }

  public boolean isLeafReturnFinalResult() {
    return _leafReturnFinalResult;
  }

  @Nullable
  public List<RelFieldCollation> getCollations() {
    return _collations;
  }

  public int getLimit() {
    return _limit;
  }

  /// The positions in the group set of the MV keys, in ascending order.
  public List<Integer> getMvKeyIndexes() {
    return _mvKeyIndexes;
  }

  @Override
  public PinotLogicalMvAggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    // The MV key indexes are positions in the group set. A rule that changes the group set (for example by merging a
    // Project that reorders the input) would make them point at the wrong keys, so fail instead of returning a wrong
    // plan. The MV aggregate is created after the rules that rewrite aggregates.
    if (!groupSet.equals(this.groupSet)) {
      throw new UnsupportedOperationException(
          "Cannot change the group set of an MV aggregate from " + this.groupSet + " to " + groupSet);
    }
    return new PinotLogicalMvAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls, _aggType,
        _leafReturnFinalResult, _collations, _limit, _mvKeyIndexes);
  }

  @Override
  protected RelDataType deriveRowType() {
    RelDataType rowType = super.deriveRowType();
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    List<RelDataTypeField> fields = rowType.getFieldList();
    List<RelDataType> types = new ArrayList<>(fields.size());
    List<String> names = new ArrayList<>(fields.size());
    for (RelDataTypeField field : fields) {
      types.add(field.getType());
      names.add(field.getName());
    }
    for (int mvKeyIndex : _mvKeyIndexes) {
      RelDataType arrayType = types.get(mvKeyIndex);
      RelDataType componentType = arrayType.getComponentType();
      if (componentType == null) {
        throw new IllegalStateException(
            "MV key at group position " + mvKeyIndex + " must be an array, got: " + arrayType.getFullTypeString());
      }
      types.set(mvKeyIndex, typeFactory.createTypeWithNullability(componentType, arrayType.isNullable()));
    }
    return typeFactory.createStructType(types, names);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter relWriter = super.explainTerms(pw);
    relWriter.item("mvKeys", _mvKeyIndexes);
    relWriter.item("aggType", _aggType);
    relWriter.itemIf("leafReturnFinalResult", true, _leafReturnFinalResult);
    relWriter.itemIf("collations", _collations, _collations != null);
    relWriter.itemIf("limit", _limit, _limit > 0);
    return relWriter;
  }

  @Override
  public RelNode withHints(List<RelHint> hintList) {
    return new PinotLogicalMvAggregate(getCluster(), traitSet, hintList, input, groupSet, groupSets, aggCalls,
        _aggType, _leafReturnFinalResult, _collations, _limit, _mvKeyIndexes);
  }
}
