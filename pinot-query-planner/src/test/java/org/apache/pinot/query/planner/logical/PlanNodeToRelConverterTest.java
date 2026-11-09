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
import org.apache.pinot.query.planner.plannode.UnnestNode;
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

  @Test
  public void testAggregateGroupingSets() {
    // MAX(d) ... GROUP BY ROLLUP(b, c), where the grouping sets hold indexes into the group keys [1, 2]
    DataSchema leafSchema = new DataSchema(new String[]{"a", "b", "c", "d"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT});
    PlanNode leaf = new ExplainedNode(0, leafSchema, null, List.of(), "Leaf", Map.of());
    DataSchema aggregateSchema = new DataSchema(new String[]{"b", "c", "$groupingId", "max"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT});
    RexExpression.FunctionCall max =
        new RexExpression.FunctionCall(ColumnDataType.INT, "MAX", List.of(new RexExpression.InputRef(3)));
    PlanNode aggregate = new AggregateNode(0, aggregateSchema, null, List.of(leaf), List.of(max), List.of(-1),
        List.of(1, 2), AggregateNode.AggType.LEAF, false, null, 0, List.of(List.of(0, 1), List.of(0), List.of()));

    RelBuilder relBuilder = RelBuilder.create(Frameworks.newConfigBuilder().build());
    RelNode relNode = PlanNodeToRelConverter.convert(relBuilder, aggregate);
    assertEquals(RelOptUtil.toString(relNode),
        "LogicalAggregate(group=[{1, 2}], groups=[[{1, 2}, {1}, {}]], agg#0=[MAX($3)])\n"
            + "  Leaf\n");
  }

  @Test
  public void testAggregateOverUnnest() {
    // COUNT(*) ... CROSS JOIN UNNEST(b) AS u(elem) GROUP BY a, elem, where the unnest output is [a, b, elem]
    PlanNode unnest = new UnnestNode(0, new DataSchema(new String[]{"a", "b", "elem"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING_ARRAY, ColumnDataType.STRING}), null,
        List.of(unnestLeaf()), List.of(new RexExpression.InputRef(1)),
        new UnnestNode.TableFunctionContext(false, List.of(2), UnnestNode.UNSPECIFIED_INDEX));
    DataSchema aggregateSchema = new DataSchema(new String[]{"a", "elem", "cnt"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.LONG});
    RexExpression.FunctionCall count = new RexExpression.FunctionCall(ColumnDataType.LONG, "COUNT", List.of());
    PlanNode aggregate = new AggregateNode(0, aggregateSchema, null, List.of(unnest), List.of(count), List.of(-1),
        List.of(0, 2), AggregateNode.AggType.LEAF, false, null, 0);

    RelBuilder relBuilder = RelBuilder.create(Frameworks.newConfigBuilder().build());
    RelNode relNode = PlanNodeToRelConverter.convert(relBuilder, aggregate);
    assertEquals(RelOptUtil.toString(relNode),
        "LogicalAggregate(group=[{0, 2}], agg#0=[COUNT()])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    Leaf\n"
            + "    Uncollect\n"
            + "      LogicalProject(b=[$cor0.b])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n");
  }

  @Test
  public void testProjectOverPrunedUnnest() {
    // SELECT a, elem, ord ... CROSS JOIN UNNEST(b) WITH ORDINALITY AS u(elem, ord), where the unnest output drops b
    PlanNode unnest = new UnnestNode(0, new DataSchema(new String[]{"a", "elem", "ord"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT}), null,
        List.of(unnestLeaf()), List.of(new RexExpression.InputRef(1)),
        new UnnestNode.TableFunctionContext(true, List.of(1), 2, List.of(0), true));
    PlanNode project = new ProjectNode(0, unnest.getDataSchema(), null, List.of(unnest),
        List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1), new RexExpression.InputRef(2)));

    RelBuilder relBuilder = RelBuilder.create(Frameworks.newConfigBuilder().build());
    RelNode relNode = PlanNodeToRelConverter.convert(relBuilder, project);
    assertEquals(RelOptUtil.toString(relNode),
        "LogicalProject(a=[$0], b=[$2], ORDINALITY=[$3])\n"
            + "  LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{1}])\n"
            + "    Leaf\n"
            + "    Uncollect(withOrdinality=[true])\n"
            + "      LogicalProject(b=[$cor0.b])\n"
            + "        LogicalValues(tuples=[[{ 0 }]])\n");
  }

  private static PlanNode unnestLeaf() {
    DataSchema leafSchema = new DataSchema(new String[]{"a", "b"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING_ARRAY});
    return new ExplainedNode(0, leafSchema, null, List.of(), "Leaf", Map.of());
  }
}
