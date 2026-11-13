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
package org.apache.pinot.query.planner.validation;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.MvAggregateNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class ArrayToMvValidationVisitorTest {
  private static final List<RexExpression.FunctionCall> COUNT =
      List.of(new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of()));

  private static PlanNode input(ColumnDataType... types) {
    String[] names = new String[types.length];
    for (int i = 0; i < types.length; i++) {
      names[i] = "c" + i;
    }
    return new ValueNode(1, new DataSchema(names, types), PlanNode.NodeHint.EMPTY, List.of(), List.of());
  }

  private static DataSchema output(ColumnDataType... keyTypes) {
    String[] names = new String[keyTypes.length + 1];
    ColumnDataType[] types = new ColumnDataType[keyTypes.length + 1];
    for (int i = 0; i < keyTypes.length; i++) {
      names[i] = "k" + i;
      types[i] = keyTypes[i];
    }
    names[keyTypes.length] = "cnt";
    types[keyTypes.length] = ColumnDataType.LONG;
    return new DataSchema(names, types);
  }

  private static MvAggregateNode mvAggregate(PlanNode input, DataSchema output, List<Integer> mvKeyIndexes,
      List<RexExpression.FunctionCall> aggCalls) {
    List<Integer> groupKeys = new ArrayList<>();
    for (int i = 0; i < output.size() - 1; i++) {
      groupKeys.add(i);
    }
    return new MvAggregateNode(1, output, PlanNode.NodeHint.EMPTY, List.of(input), aggCalls, List.of(-1), groupKeys,
        AggregateNode.AggType.LEAF, false, null, 0, mvKeyIndexes);
  }

  private static void validate(PlanNode node, boolean isIntermediateStage) {
    node.visit(ArrayToMvValidationVisitor.INSTANCE, isIntermediateStage);
  }

  @Test
  public void testValidMvAggregate() {
    PlanNode input = input(ColumnDataType.STRING_ARRAY, ColumnDataType.INT);
    MvAggregateNode node =
        mvAggregate(input, output(ColumnDataType.STRING, ColumnDataType.INT), List.of(0), COUNT);
    validate(node, false);
    validate(node, true);
  }

  @Test
  public void testMvKeyThatIsNotAnArrayInTheInputIsAPlannerBug() {
    MvAggregateNode node =
        mvAggregate(input(ColumnDataType.STRING), output(ColumnDataType.STRING), List.of(0), COUNT);
    assertThrows(IllegalStateException.class, () -> validate(node, false));
    assertThrows(IllegalStateException.class, () -> validate(node, true));
  }

  @Test
  public void testMvKeyThatIsAnArrayInTheOutputIsAPlannerBug() {
    MvAggregateNode node =
        mvAggregate(input(ColumnDataType.STRING_ARRAY), output(ColumnDataType.STRING_ARRAY), List.of(0), COUNT);
    assertThrows(IllegalStateException.class, () -> validate(node, true));
  }

  @Test
  public void testArraySvKeyOfAnMvAggregateFails() {
    PlanNode input = input(ColumnDataType.STRING_ARRAY, ColumnDataType.LONG_ARRAY);
    MvAggregateNode node =
        mvAggregate(input, output(ColumnDataType.STRING, ColumnDataType.LONG_ARRAY), List.of(0), COUNT);
    QueryException e = expectThrows(QueryException.class, () -> validate(node, false));
    assertTrue(e.getMessage().contains("Use ARRAY_TO_MV() to group by multi-value column"), e.getMessage());
    assertTrue(e.getMessage().endsWith(ArrayToMvValidationVisitor.SUPPORTED_USES), e.getMessage());
  }

  @Test
  public void testArrayToMvInAggCallOfAnIntermediateMvAggregateFails() {
    RexExpression.FunctionCall sumOfArrayToMv = new RexExpression.FunctionCall(ColumnDataType.LONG, "SUM",
        List.of(new RexExpression.FunctionCall(ColumnDataType.LONG, "ARRAY_TO_MV",
            List.of(new RexExpression.InputRef(1)))));
    PlanNode input = input(ColumnDataType.STRING_ARRAY, ColumnDataType.LONG_ARRAY);
    MvAggregateNode node = mvAggregate(input, output(ColumnDataType.STRING), List.of(0), List.of(sumOfArrayToMv));
    validate(node, false);
    QueryException e = expectThrows(QueryException.class, () -> validate(node, true));
    assertTrue(e.getMessage().endsWith(ArrayToMvValidationVisitor.SUPPORTED_USES), e.getMessage());
  }

  @Test
  public void testArrayGroupKeyOfALeafAggregateFails() {
    AggregateNode node = new AggregateNode(1, output(ColumnDataType.STRING_ARRAY), PlanNode.NodeHint.EMPTY,
        List.of(input(ColumnDataType.STRING_ARRAY)), COUNT, List.of(-1), List.of(0), AggregateNode.AggType.LEAF,
        false, null, 0);
    QueryException e = expectThrows(QueryException.class, () -> validate(node, false));
    assertTrue(e.getMessage().contains("in the leaf stage"), e.getMessage());
    assertTrue(e.getMessage().contains("Use ARRAY_TO_MV() to group by multi-value column"), e.getMessage());
  }

  @Test
  public void testLeafAggregateDoesNotVisitItsProject() {
    // GROUP BY LOWER(ARRAY_TO_MV(c0)) in the leaf stage: the single-stage engine evaluates LOWER(mv).
    RexExpression lowerOfArrayToMv = new RexExpression.FunctionCall(ColumnDataType.STRING, "LOWER",
        List.of(new RexExpression.FunctionCall(ColumnDataType.STRING, "ARRAY_TO_MV",
            List.of(new RexExpression.InputRef(0)))));
    ProjectNode project = new ProjectNode(1, new DataSchema(new String[]{"k"}, new ColumnDataType[]{
        ColumnDataType.STRING}), PlanNode.NodeHint.EMPTY, List.of(input(ColumnDataType.STRING_ARRAY)),
        List.of(lowerOfArrayToMv));
    AggregateNode node = new AggregateNode(1, output(ColumnDataType.STRING), PlanNode.NodeHint.EMPTY,
        List.of(project), COUNT, List.of(-1), List.of(0), AggregateNode.AggType.LEAF, false, null, 0);
    validate(node, false);
    QueryException e = expectThrows(QueryException.class, () -> validate(node, true));
    assertTrue(e.getMessage().startsWith("ARRAY_TO_MV found in PROJECT in the intermediate stage"), e.getMessage());
  }
}
