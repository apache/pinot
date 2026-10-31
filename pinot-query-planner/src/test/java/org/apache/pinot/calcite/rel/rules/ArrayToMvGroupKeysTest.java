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
package org.apache.pinot.calcite.rel.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.sql.fun.PinotOperatorTable;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class ArrayToMvGroupKeysTest {
  private final RelDataTypeFactory _typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder _rexBuilder = new RexBuilder(_typeFactory);
  private final RelOptCluster _cluster =
      RelOptCluster.create(new HepPlanner(HepProgram.builder().build()), _rexBuilder);
  private final RelDataType _varcharType = _typeFactory.createSqlType(SqlTypeName.VARCHAR);
  private final RelDataType _varcharArrayType = _typeFactory.createArrayType(_varcharType, -1);
  /// Input `(sv VARCHAR, arr VARCHAR ARRAY)`.
  private final RelNode _values = LogicalValues.createEmpty(_cluster,
      _typeFactory.builder().add("sv", _varcharType).add("arr", _varcharArrayType).build());

  private RexNode sv() {
    return _rexBuilder.makeInputRef(_values, 0);
  }

  private RexNode arr() {
    return _rexBuilder.makeInputRef(_values, 1);
  }

  private RexNode arrayToMv(RexNode operand) {
    return _rexBuilder.makeCall(PinotOperatorTable.ARRAY_TO_MV, operand);
  }

  private Project project(RexNode... projects) {
    List<String> names = new ArrayList<>();
    for (int i = 0; i < projects.length; i++) {
      names.add("c" + i);
    }
    return LogicalProject.create(_values, List.of(), List.of(projects), names, Set.of());
  }

  /// `GROUP BY` every column of `input`, no aggregate calls.
  private Aggregate groupByAll(RelNode input) {
    return LogicalAggregate.create(input, List.of(), ImmutableBitSet.range(input.getRowType().getFieldCount()), null,
        List.of());
  }

  @Test
  public void testDetectTopLevelCall() {
    // GROUP BY sv, ARRAY_TO_MV(arr), ARRAY_TO_MV(arr)
    Aggregate aggregate = groupByAll(project(sv(), arrayToMv(arr()), arrayToMv(arr())));
    assertEquals(ArrayToMvGroupKeys.detect(aggregate), List.of(1, 2));
  }

  @Test
  public void testDetectReturnsGroupSetPositions() {
    // The group set skips column 0, so input column 2 is at group position 1.
    Project project = project(arrayToMv(arr()), sv(), arrayToMv(arr()));
    Aggregate aggregate = LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(1, 2), null, List.of());
    assertEquals(ArrayToMvGroupKeys.detect(aggregate), List.of(1));
    assertEquals(ArrayToMvGroupKeys.toInputColumns(aggregate, List.of(1)), List.of(2));
  }

  @Test
  public void testNestedCallIsNotDetected() {
    RexNode lower = _rexBuilder.makeCall(SqlStdOperatorTable.LOWER, arrayToMv(arr()));
    RexNode cast = _rexBuilder.makeCast(_varcharType, arrayToMv(arr()));
    Aggregate aggregate = groupByAll(project(lower, cast));
    assertTrue(ArrayToMvGroupKeys.detect(aggregate).isEmpty());
  }

  @Test
  public void testNonArrayOperandIsNotDetected() {
    RexNode call = _rexBuilder.makeCall(_varcharType, PinotOperatorTable.ARRAY_TO_MV, List.of(sv()));
    Aggregate aggregate = groupByAll(project(call));
    assertTrue(ArrayToMvGroupKeys.detect(aggregate).isEmpty());
  }

  @Test
  public void testInputThatIsNotAProjectIsNotDetected() {
    assertTrue(ArrayToMvGroupKeys.detect(groupByAll(_values)).isEmpty());
  }

  @Test
  public void testRewriteProjectUsesTheArrayOperand() {
    Project project = project(sv(), arrayToMv(arr()));
    assertEquals(project.getRowType().getFieldList().get(1).getType().getSqlTypeName(), SqlTypeName.VARCHAR);

    Project rewritten = ArrayToMvGroupKeys.rewriteProject(project, List.of(1));
    assertEquals(rewritten.getProjects(), List.of(sv(), arr()));
    assertEquals(rewritten.getRowType().getFieldNames(), List.of("c0", "c1"));
    RelDataType rewrittenType = rewritten.getRowType().getFieldList().get(1).getType();
    assertEquals(rewrittenType.getSqlTypeName(), SqlTypeName.ARRAY);
    assertEquals(rewrittenType.getComponentType().getSqlTypeName(), SqlTypeName.VARCHAR);
  }

  @Test
  public void testRewriteProjectRejectsAColumnWithoutArrayToMv() {
    assertThrows(IllegalArgumentException.class,
        () -> ArrayToMvGroupKeys.rewriteProject(project(sv(), arrayToMv(arr())), List.of(0)));
  }
}
