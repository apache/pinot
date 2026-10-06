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
package org.apache.pinot.query.planner.logical;

import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests for [PlanNodeToRelConverter].
public class PlanNodeToRelConverterTest {

  @Test
  public void testAggregateFilter() {
    // COUNT(*), COUNT(*) FILTER (WHERE c) ... GROUP BY a
    DataSchema leafSchema = new DataSchema(new String[]{"a", "b", "c"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.BOOLEAN});
    PlanNode leaf = new ExplainedNode(0, leafSchema, null, List.of(), "Leaf", Map.of());
    DataSchema projectSchema =
        new DataSchema(new String[]{"a", "c"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.BOOLEAN});
    PlanNode project = new ProjectNode(0, projectSchema, null, List.of(leaf),
        List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2)));
    DataSchema aggregateSchema = new DataSchema(new String[]{"a", "cnt", "filteredCnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.LONG});
    RexExpression.FunctionCall count = new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of());
    PlanNode aggregate = new AggregateNode(0, aggregateSchema, null, List.of(project), List.of(count, count),
        List.of(-1, 1), List.of(0), AggregateNode.AggType.DIRECT, false, null, -1);

    RelBuilder relBuilder = RelBuilder.create(Frameworks.newConfigBuilder().build());
    RelNode relNode = PlanNodeToRelConverter.convert(relBuilder, aggregate);
    assertEquals(RelOptUtil.toString(relNode),
        "LogicalAggregate(group=[{0}], agg#0=[COUNT()], agg#1=[COUNT() FILTER $1])\n"
            + "  LogicalProject(a=[$0], c=[$2])\n"
            + "    Leaf\n");
  }
}
