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

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class PinotLogicalMvAggregateTest {
  private final RelDataTypeFactory _typeFactory = new JavaTypeFactoryImpl();
  private final RelOptCluster _cluster =
      RelOptCluster.create(new HepPlanner(HepProgram.builder().build()), new RexBuilder(_typeFactory));

  /// Builds `GROUP BY sv, arr` with `COUNT(*)` over an input `(sv INT, arr VARCHAR ARRAY, nullableArr BIGINT ARRAY)`
  /// and returns the plain aggregate, used as the template for the MV aggregate.
  private Aggregate plainAggregate(int... groupKeys) {
    RelDataType varcharArray = _typeFactory.createArrayType(_typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
    RelDataType nullableBigintArray = _typeFactory.createTypeWithNullability(
        _typeFactory.createArrayType(_typeFactory.createSqlType(SqlTypeName.BIGINT), -1), true);
    RelDataType inputType = _typeFactory.builder()
        .add("sv", SqlTypeName.INTEGER)
        .add("arr", varcharArray)
        .add("nullableArr", nullableBigintArray)
        .build();
    RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(_cluster, null);
    relBuilder.push(LogicalValues.createEmpty(_cluster, inputType));
    RelNode relNode = relBuilder.aggregate(relBuilder.groupKey(groupKeys), relBuilder.countStar("cnt")).build();
    return (Aggregate) relNode;
  }

  private PinotLogicalMvAggregate mvAggregate(Aggregate aggregate, List<Integer> mvKeyIndexes) {
    return new PinotLogicalMvAggregate(_cluster, aggregate.getTraitSet(), List.of(), aggregate.getInput(),
        aggregate.getGroupSet(), null, aggregate.getAggCallList(), AggType.LEAF, false, null, 0, mvKeyIndexes);
  }

  @Test
  public void testDeriveRowTypeUsesComponentTypeForMvKeys() {
    PinotLogicalMvAggregate mvAggregate = mvAggregate(plainAggregate(0, 1, 2), List.of(1, 2));
    RelDataType rowType = mvAggregate.getRowType();
    assertEquals(rowType.getFieldNames(), List.of("sv", "arr", "nullableArr", "cnt"));
    // SV key unchanged
    assertEquals(rowType.getFieldList().get(0).getType().getSqlTypeName(), SqlTypeName.INTEGER);
    // VARCHAR ARRAY becomes VARCHAR, not nullable as the input
    RelDataType arrKeyType = rowType.getFieldList().get(1).getType();
    assertEquals(arrKeyType.getSqlTypeName(), SqlTypeName.VARCHAR);
    assertFalse(arrKeyType.isNullable());
    // Nullable BIGINT ARRAY becomes BIGINT, with the nullability of the component type, as ARRAY_TO_MV
    RelDataType nullableArrKeyType = rowType.getFieldList().get(2).getType();
    assertEquals(nullableArrKeyType.getSqlTypeName(), SqlTypeName.BIGINT);
    assertFalse(nullableArrKeyType.isNullable());
    // Aggregate call unchanged
    assertEquals(rowType.getFieldList().get(3).getType().getSqlTypeName(), SqlTypeName.BIGINT);
  }

  /// The MV key type must equal the type of `ARRAY_TO_MV(array)` for every nullability combination, or the rule that
  /// replaces a plain aggregate over `ARRAY_TO_MV(array)` with the MV aggregate fails Calcite's type check.
  @Test
  public void testMvKeyTypeEqualsArrayToMvType() {
    RelDataType varchar = _typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType nullableVarchar = _typeFactory.createTypeWithNullability(varchar, true);
    List<RelDataType> arrayTypes = List.of(
        _typeFactory.createArrayType(varchar, -1),
        _typeFactory.createTypeWithNullability(_typeFactory.createArrayType(varchar, -1), true),
        _typeFactory.createArrayType(nullableVarchar, -1),
        _typeFactory.createTypeWithNullability(_typeFactory.createArrayType(nullableVarchar, -1), true));
    // The component type decides, as for ARRAY_TO_MV before the MV aggregate.
    List<Boolean> expectedNullable = List.of(false, false, true, true);
    RexBuilder rexBuilder = _cluster.getRexBuilder();
    for (int i = 0; i < arrayTypes.size(); i++) {
      RelDataType inputType = _typeFactory.builder().add("arr", arrayTypes.get(i)).build();
      RelNode values = LogicalValues.createEmpty(_cluster, inputType);
      RelDataType arrayToMvType =
          rexBuilder.makeCall(PinotOperatorTable.ARRAY_TO_MV, rexBuilder.makeInputRef(values, 0)).getType();
      PinotLogicalMvAggregate mvAggregate =
          new PinotLogicalMvAggregate(_cluster, values.getTraitSet(), List.of(), values, ImmutableBitSet.of(0), null,
              List.of(), AggType.LEAF, false, null, 0, List.of(0));
      RelDataType keyType = mvAggregate.getRowType().getFieldList().get(0).getType();
      assertEquals(keyType, arrayToMvType, "array type: " + arrayTypes.get(i).getFullTypeString());
      assertEquals(keyType.isNullable(), (boolean) expectedNullable.get(i),
          "array type: " + arrayTypes.get(i).getFullTypeString());
    }
  }

  @Test
  public void testCopyAndWithHintsKeepFields() {
    Aggregate aggregate = plainAggregate(0, 1);
    PinotLogicalMvAggregate mvAggregate = mvAggregate(aggregate, List.of(1));
    PinotLogicalMvAggregate copy =
        (PinotLogicalMvAggregate) mvAggregate.copy(mvAggregate.getTraitSet(), mvAggregate.getInputs());
    assertEquals(copy.getMvKeyIndexes(), List.of(1));
    assertEquals(copy.getAggType(), AggType.LEAF);
    assertEquals(copy.getRowType(), mvAggregate.getRowType());
    PinotLogicalMvAggregate withHints = (PinotLogicalMvAggregate) mvAggregate.withHints(List.of());
    assertEquals(withHints.getMvKeyIndexes(), List.of(1));
    assertEquals(withHints.getRowType(), mvAggregate.getRowType());
  }

  @Test
  public void testCopyRejectsChangedGroupSet() {
    PinotLogicalMvAggregate mvAggregate = mvAggregate(plainAggregate(0, 1), List.of(1));
    assertThrows(UnsupportedOperationException.class,
        () -> mvAggregate.copy(mvAggregate.getTraitSet(), mvAggregate.getInput(), ImmutableBitSet.of(1, 2), null,
            mvAggregate.getAggCallList()));
  }

  @Test
  public void testExplainShowsMvKeys() {
    String explain = mvAggregate(plainAggregate(0, 1), List.of(1)).explain();
    assertTrue(explain.contains("mvKeys=[[1]]"), explain);
    assertTrue(explain.contains("aggType=[LEAF]"), explain);
  }

  @Test
  public void testRejectsGroupingSets() {
    Aggregate aggregate = plainAggregate(0, 1);
    List<ImmutableBitSet> groupSets = List.of(ImmutableBitSet.of(0, 1), ImmutableBitSet.of(1));
    assertThrows(IllegalArgumentException.class,
        () -> new PinotLogicalMvAggregate(_cluster, aggregate.getTraitSet(), List.of(), aggregate.getInput(),
            aggregate.getGroupSet(), groupSets, aggregate.getAggCallList(), AggType.LEAF, false, null, 0,
            List.of(1)));
  }

  @Test
  public void testRejectsInvalidMvKeyIndexes() {
    Aggregate aggregate = plainAggregate(0, 1);
    // Out of the group set
    assertThrows(IllegalArgumentException.class, () -> mvAggregate(aggregate, List.of(2)));
    // Not ascending
    assertThrows(IllegalArgumentException.class, () -> mvAggregate(plainAggregate(0, 1, 2), List.of(2, 1)));
  }

  @Test
  public void testNonArrayMvKeyFails() {
    PinotLogicalMvAggregate mvAggregate = mvAggregate(plainAggregate(0, 1), List.of(0));
    assertThrows(IllegalStateException.class, mvAggregate::getRowType);
  }
}
