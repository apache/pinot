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
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
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
}
