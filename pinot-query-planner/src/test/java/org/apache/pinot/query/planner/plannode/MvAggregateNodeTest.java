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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;


public class MvAggregateNodeTest {
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"mv", "sv", "cnt"},
      new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG});
  private static final List<RexExpression.FunctionCall> AGG_CALLS =
      List.of(new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of()));

  private static MvAggregateNode node(List<PlanNode> inputs, List<Integer> mvKeyIndexes) {
    return new MvAggregateNode(1, SCHEMA, PlanNode.NodeHint.EMPTY, inputs, AGG_CALLS, List.of(-1), List.of(0, 1),
        AggregateNode.AggType.LEAF, false, null, 0, mvKeyIndexes);
  }

  @Test
  public void testEquals() {
    assertEquals(node(List.of(), List.of(0)), node(List.of(), List.of(0)));
    assertEquals(node(List.of(), List.of(0)).hashCode(), node(List.of(), List.of(0)).hashCode());
    assertNotEquals(node(List.of(), List.of(0)), node(List.of(), List.of(1)));
  }

  @Test
  public void testNotEqualToPlainAggregate() {
    AggregateNode aggregateNode = new AggregateNode(1, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), AGG_CALLS,
        List.of(-1), List.of(0, 1), AggregateNode.AggType.LEAF, false, null, 0);
    assertNotEquals(node(List.of(), List.of(0)), aggregateNode);
    assertNotEquals(aggregateNode, node(List.of(), List.of(0)));
  }

  @Test
  public void testWithInputsKeepsFields() {
    MvAggregateNode node = node(List.of(), List.of(0));
    PlanNode input = new ValueNode(1, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    MvAggregateNode copy = (MvAggregateNode) node.withInputs(List.of(input));
    assertSame(copy.getInputs().get(0), input);
    assertEquals(copy.withInputs(List.of()), node);
    assertEquals(copy.getMvKeyIndexes(), List.of(0));
    assertEquals(copy.getGroupKeys(), List.of(0, 1));
    assertEquals(copy.getAggType(), AggregateNode.AggType.LEAF);
    assertEquals(copy.getCollations(), List.of());
    assertEquals(copy.explain(), "MV_AGGREGATE_LEAF");
  }

  @Test
  public void testVisitCallsVisitMvAggregate() {
    String visited = node(List.of(), List.of(0)).visit(new PlanNodeVisitor.DepthFirstVisitor<String, Void>() {
      @Override
      public String visitMvAggregate(MvAggregateNode node, Void context) {
        return "mvAggregate";
      }
    }, null);
    assertEquals(visited, "mvAggregate");
  }
}
