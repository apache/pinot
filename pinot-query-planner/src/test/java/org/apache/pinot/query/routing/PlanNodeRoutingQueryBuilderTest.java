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
package org.apache.pinot.query.routing;

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class PlanNodeRoutingQueryBuilderTest {
  private static final DataSchema TEST_SCHEMA = new DataSchema(new String[]{"col1", "col2"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});

  @Test
  public void testCreatePinotQueryForRoutingPlanNodeTree() {
    TableScanNode tableScanNode =
        new TableScanNode(1, TEST_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "testTable", List.of("col1", "col2"));
    FilterNode filterNode = new FilterNode(1, TEST_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScanNode),
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, "EQUALS",
            List.of(new RexExpression.InputRef(0),
                new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "foo"))));
    ProjectNode projectNode = new ProjectNode(1, new DataSchema(new String[]{"col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}), PlanNode.NodeHint.EMPTY,
        List.of(filterNode), List.of(new RexExpression.InputRef(1)));

    PinotQuery pinotQuery = PlanNodeRoutingQueryBuilder.createPinotQueryForRouting("testTable", projectNode, false);

    assertEquals(pinotQuery.getDataSource().getTableName(), "testTable");
    assertEquals(pinotQuery.getSelectList().size(), 1);
    assertEquals(pinotQuery.getSelectList().get(0).getIdentifier().getName(), "col2");
    assertNotNull(pinotQuery.getFilterExpression());
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperator(), "EQUALS");
    assertEquals(pinotQuery.getFilterExpression().getFunctionCall().getOperands().get(0).getIdentifier().getName(),
        "col1");
  }

  @Test
  public void testCreatePinotQueryForRoutingCombinesStackedLeafFilters() {
    // Two filter nodes below the leaf boundary (both operate on the scan row stream) must be AND-combined into the
    // routing filter, not overwritten -- otherwise a genuine row-level condition is silently dropped and pruning
    // becomes incorrect. Reverting the builder to overwrite-the-filter would make this fail.
    TableScanNode tableScanNode =
        new TableScanNode(1, TEST_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "testTable", List.of("col1", "col2"));
    FilterNode lowerFilter = new FilterNode(1, TEST_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScanNode),
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, "EQUALS",
            List.of(new RexExpression.InputRef(0),
                new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "foo"))));
    FilterNode upperFilter = new FilterNode(1, TEST_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(lowerFilter),
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, "EQUALS",
            List.of(new RexExpression.InputRef(1),
                new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "bar"))));

    PinotQuery pinotQuery = PlanNodeRoutingQueryBuilder.createPinotQueryForRouting("testTable", upperFilter, false);

    Expression filter = pinotQuery.getFilterExpression();
    assertNotNull(filter);
    assertEquals(filter.getFunctionCall().getOperator(), "AND");
    List<Expression> operands = filter.getFunctionCall().getOperands();
    assertEquals(operands.size(), 2);
    // Combined as AND(lower, upper): EQUALS(col1,'foo') AND EQUALS(col2,'bar').
    assertEquals(operands.get(0).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
    assertEquals(operands.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col2");
  }

  @Test
  public void testCreatePinotQueryForRoutingStopsAtLeafBoundary() {
    // A node that is neither Filter nor Project (here an AggregateNode) is a leaf boundary: anything above it operates
    // on a different (post-aggregate) row space, so its filters/projects must NOT be folded into the routing query.
    // The routing query should carry only the WHERE filter below the aggregate.
    TableScanNode tableScanNode =
        new TableScanNode(1, TEST_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "testTable", List.of("col1", "col2"));
    FilterNode whereFilter = new FilterNode(1, TEST_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(tableScanNode),
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, "EQUALS",
            List.of(new RexExpression.InputRef(0),
                new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "foo"))));
    DataSchema aggSchema = new DataSchema(new String[]{"col1", "EXPR$1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    AggregateNode aggregateNode = new AggregateNode(1, aggSchema, PlanNode.NodeHint.EMPTY, List.of(whereFilter),
        List.of(new RexExpression.FunctionCall(DataSchema.ColumnDataType.LONG, "COUNT", List.of())),
        List.of(), List.of(0), AggregateNode.AggType.DIRECT, false, List.of(), -1);
    // A HAVING filter above the aggregate; its InputRef(1) indexes the aggregate output, not the scan columns.
    FilterNode havingFilter = new FilterNode(1, aggSchema, PlanNode.NodeHint.EMPTY, List.of(aggregateNode),
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, "GREATER_THAN",
            List.of(new RexExpression.InputRef(1),
                new RexExpression.Literal(DataSchema.ColumnDataType.LONG, 10))));

    PinotQuery pinotQuery = PlanNodeRoutingQueryBuilder.createPinotQueryForRouting("testTable", havingFilter, false);

    Expression filter = pinotQuery.getFilterExpression();
    assertNotNull(filter);
    assertEquals(filter.getFunctionCall().getOperator(), "EQUALS");
    assertEquals(filter.getFunctionCall().getOperands().get(0).getIdentifier().getName(), "col1");
  }
}
