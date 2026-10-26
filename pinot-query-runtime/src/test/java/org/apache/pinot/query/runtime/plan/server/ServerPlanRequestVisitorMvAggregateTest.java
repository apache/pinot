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
package org.apache.pinot.query.runtime.plan.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MvAggregateNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.MvAggregateOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.PlanNodeToOpChain;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests how [ServerPlanRequestVisitor] places an [MvAggregateNode] in the leaf stage: pushed down to the single-stage
/// engine as a plain `GROUP BY` on the array expression when it sits on the scan, or kept above the leaf boundary
/// (run by [MvAggregateOperator]) when its input already set the boundary, as a lookup join does.
public class ServerPlanRequestVisitorMvAggregateTest {
  private static final String TABLE = "e";
  private static final DataSchema SCAN_SCHEMA = new DataSchema(new String[]{"sv", "mvStr", "mvLong"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING_ARRAY, ColumnDataType.LONG_ARRAY});

  /// `SELECT ARRAY_TO_MV(mvStr), sv, COUNT(*) FROM e GROUP BY 1, 2` after the planner rewrite: the Project holds the
  /// bare MV column and the MV aggregate groups by it.
  @Test
  public void testMvAggregateOnScanIsLeafBoundary() {
    MvAggregateNode mvAggregate = mvAggregateOnScan(new RexExpression.InputRef(1), ColumnDataType.STRING_ARRAY,
        ColumnDataType.STRING);

    ServerPlanRequestContext context = walk(mvAggregate);

    assertSame(context.getLeafStageBoundaryNode(), mvAggregate);
    PinotQuery pinotQuery = context.getPinotQuery();
    assertEquals(pinotQuery.getGroupByList(),
        List.of(RequestUtils.getIdentifierExpression("mvStr"), RequestUtils.getIdentifierExpression("sv")));
    assertFalse(pinotQuery.toString().toLowerCase().contains("arraytomv"), "Unexpected query: " + pinotQuery);
    List<Expression> selectList = pinotQuery.getSelectList();
    assertEquals(selectList.size(), 3);
    assertEquals(selectList.get(0), RequestUtils.getIdentifierExpression("mvStr"));
    assertEquals(selectList.get(1), RequestUtils.getIdentifierExpression("sv"));
    assertEquals(selectList.get(2).getFunctionCall().getOperator(), "count");
  }

  /// The group-by key of the single-stage query is the array transform when the planner kept one under the
  /// `ARRAY_TO_MV`, as in `GROUP BY ARRAY_TO_MV(VALUE_IN(mvStr, 'a', 'b'))`.
  @Test
  public void testMvAggregateOnArrayTransform() {
    RexExpression valueIn = new RexExpression.FunctionCall(ColumnDataType.STRING_ARRAY, "VALUE_IN",
        List.of(new RexExpression.InputRef(1), new RexExpression.Literal(ColumnDataType.STRING, "a"),
            new RexExpression.Literal(ColumnDataType.STRING, "b")));
    MvAggregateNode mvAggregate = mvAggregateOnScan(valueIn, ColumnDataType.STRING_ARRAY, ColumnDataType.STRING);

    ServerPlanRequestContext context = walk(mvAggregate);

    assertSame(context.getLeafStageBoundaryNode(), mvAggregate);
    Expression groupKey = context.getPinotQuery().getGroupByList().get(0);
    assertEquals(groupKey.getFunctionCall().getOperator(), "valuein");
    assertEquals(groupKey.getFunctionCall().getOperands().get(0), RequestUtils.getIdentifierExpression("mvStr"));
  }

  /// The single-stage engine types an MV group key with its single-value type, so the stored types of its result
  /// equal the stored types of the MV aggregate schema and `LeafOperator` does not convert the rows. This also holds
  /// for nullable MV keys: [ColumnDataType] carries no nullability.
  @Test
  public void testSingleStageResultSchemaMatchesNodeSchema()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE)
        .addSingleValueDimension("sv", FieldSpec.DataType.INT)
        .addMultiValueDimension("mvStr", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvLong", FieldSpec.DataType.LONG)
        .build();
    MockInstanceDataManagerFactory factory = new MockInstanceDataManagerFactory("server");
    factory.registerTable(schema, TABLE + "_OFFLINE");
    List<GenericRow> rows = new ArrayList<>();
    rows.add(row(1, new Object[]{"a", "b"}, new Object[]{1L, 2L}));
    rows.add(row(2, new Object[]{"b"}, new Object[]{3L}));
    rows.add(row(1, new Object[]{"a", "a"}, new Object[]{1L}));
    ImmutableSegment segment = factory.addSegment(TABLE + "_OFFLINE", rows);
    try {
      // Group by the STRING MV column and the SV column.
      MvAggregateNode strNode =
          mvAggregateOnScan(new RexExpression.InputRef(1), ColumnDataType.STRING_ARRAY, ColumnDataType.STRING);
      Map<List<Object>, Long> strCounts = runOnSegment(strNode, segment);
      Map<List<Object>, Long> expectedStrCounts = new HashMap<>();
      expectedStrCounts.put(List.of("a", 1), 3L);
      expectedStrCounts.put(List.of("b", 1), 1L);
      expectedStrCounts.put(List.of("b", 2), 1L);
      assertEquals(strCounts, expectedStrCounts);

      // Group by the LONG MV column and the SV column.
      MvAggregateNode longNode =
          mvAggregateOnScan(new RexExpression.InputRef(2), ColumnDataType.LONG_ARRAY, ColumnDataType.LONG);
      Map<List<Object>, Long> longCounts = runOnSegment(longNode, segment);
      Map<List<Object>, Long> expectedLongCounts = new HashMap<>();
      expectedLongCounts.put(List.of(1L, 1), 2L);
      expectedLongCounts.put(List.of(2L, 1), 1L);
      expectedLongCounts.put(List.of(3L, 2), 1L);
      assertEquals(longCounts, expectedLongCounts);
    } finally {
      segment.destroy();
    }
  }

  /// With a lookup join, the right input (the dimension table scan) is the leaf boundary and the MV aggregate above
  /// the join runs as an [MvAggregateOperator], not inside [LeafOperator].
  @Test
  public void testMvAggregateAboveLookupJoinIsNotLeafBoundary() {
    DataSchema leftSchema = new DataSchema(new String[]{"key", "mv"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING_ARRAY});
    MailboxReceiveNode left = new MailboxReceiveNode(1, leftSchema, 2, PinotRelExchangeType.STREAMING,
        RelDistribution.Type.SINGLETON, null, null, false, false, null);
    DataSchema dimSchema = new DataSchema(new String[]{"key", "name"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    TableScanNode dimScan = new TableScanNode(1, dimSchema, PlanNode.NodeHint.EMPTY, List.of(), "dim",
        List.of("key", "name"));
    ProjectNode right = new ProjectNode(1, dimSchema, PlanNode.NodeHint.EMPTY, List.of(dimScan),
        List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(1)));
    DataSchema joinSchema = new DataSchema(new String[]{"key", "mv", "key0", "name"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING_ARRAY, ColumnDataType.INT,
            ColumnDataType.STRING});
    JoinNode join = new JoinNode(1, joinSchema, PlanNode.NodeHint.EMPTY, List.of(left, right), JoinRelType.INNER,
        List.of(0), List.of(0), List.of(), JoinNode.JoinStrategy.LOOKUP);
    DataSchema projectSchema = new DataSchema(new String[]{"mv"}, new ColumnDataType[]{ColumnDataType.STRING_ARRAY});
    ProjectNode project = new ProjectNode(1, projectSchema, PlanNode.NodeHint.EMPTY, List.of(join),
        List.of(new RexExpression.InputRef(1)));
    DataSchema aggSchema =
        new DataSchema(new String[]{"mv", "cnt"}, new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.LONG});
    MvAggregateNode mvAggregate = new MvAggregateNode(1, aggSchema, PlanNode.NodeHint.EMPTY, List.of(project),
        List.of(countStar()), List.of(-1), List.of(0), AggType.LEAF, false, null, 0, List.of(0));

    ServerPlanRequestContext serverContext = walk(mvAggregate);

    assertSame(serverContext.getLeafStageBoundaryNode(), right);
    assertNull(serverContext.getPinotQuery().getGroupByList());

    // The op chain runs the MV aggregate as an MSE operator and the dimension table scan as the leaf operator. The
    // lookup join itself cannot be built here (no dimension table is registered); it does not matter for this test.
    serverContext.setServerQueryRequests(List.of(mock(ServerQueryRequest.class)));
    OpChainExecutionContext context = OperatorTestUtil.getContext(Map.of());
    context.setLeafStageContext(serverContext);
    Map<PlanNode, MultiStageOperator> operators = new HashMap<>();
    MultiStageOperator root = PlanNodeToOpChain.convert(mvAggregate, context, operators::put).getRoot();

    assertTrue(root instanceof MvAggregateOperator, "Unexpected root operator: " + root);
    assertTrue(operators.get(right) instanceof LeafOperator, "Unexpected boundary operator: " + operators.get(right));
  }

  /// Builds `MvAggregate(LEAF, group by [0, 1], mvKeys [0], COUNT(*)) <- Project(mvKey, sv) <- Scan(e)`.
  private static MvAggregateNode mvAggregateOnScan(RexExpression mvKey, ColumnDataType mvKeyArrayType,
      ColumnDataType mvKeyElementType) {
    TableScanNode scan = new TableScanNode(1, SCAN_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), TABLE,
        List.of("sv", "mvStr", "mvLong"));
    DataSchema projectSchema = new DataSchema(new String[]{"mvKey", "sv"},
        new ColumnDataType[]{mvKeyArrayType, ColumnDataType.INT});
    ProjectNode project = new ProjectNode(1, projectSchema, PlanNode.NodeHint.EMPTY, List.of(scan),
        List.of(mvKey, new RexExpression.InputRef(0)));
    DataSchema aggSchema = new DataSchema(new String[]{"mvKey", "sv", "cnt"},
        new ColumnDataType[]{mvKeyElementType, ColumnDataType.INT, ColumnDataType.LONG});
    return new MvAggregateNode(1, aggSchema, PlanNode.NodeHint.EMPTY, List.of(project), List.of(countStar()),
        List.of(-1), List.of(0, 1), AggType.LEAF, false, null, 0, List.of(0));
  }

  private static ServerPlanRequestContext walk(PlanNode root) {
    ServerPlanRequestContext context = new ServerPlanRequestContext(new StagePlan(root, null), null, null, null);
    context.getPinotQuery().setLimit(Integer.MAX_VALUE);
    ServerPlanRequestVisitor.walkPlanNode(root, context);
    return context;
  }

  /// Runs the single-stage query that the visitor builds for the node on one segment, checks that its stored column
  /// types equal the node's, and returns the counts by group key.
  private static Map<List<Object>, Long> runOnSegment(MvAggregateNode node, ImmutableSegment segment) {
    PinotQuery pinotQuery = walk(node).getPinotQuery();
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    Operator<?> operator =
        new InstancePlanMakerImplV2().makeSegmentPlanNode(new SegmentContext(segment), queryContext).run();
    GroupByResultsBlock block = (GroupByResultsBlock) operator.nextBlock();

    assertEquals(block.getDataSchema().getStoredColumnDataTypes(), node.getDataSchema().getStoredColumnDataTypes(),
        "Single-stage schema " + block.getDataSchema() + " does not match node schema " + node.getDataSchema());
    AggregationGroupByResult result = block.getAggregationGroupByResult();
    Map<List<Object>, Long> counts = new HashMap<>();
    Iterator<GroupKeyGenerator.GroupKey> groupKeys = result.getGroupKeyIterator();
    while (groupKeys.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeys.next();
      counts.put(Arrays.asList(groupKey._keys), (Long) result.getResultForGroupId(0, groupKey._groupId));
    }
    return counts;
  }

  private static GenericRow row(int sv, Object[] mvStr, Object[] mvLong) {
    GenericRow row = new GenericRow();
    row.putValue("sv", sv);
    row.putValue("mvStr", mvStr);
    row.putValue("mvLong", mvLong);
    return row;
  }

  private static RexExpression.FunctionCall countStar() {
    return new RexExpression.FunctionCall(ColumnDataType.LONG, SqlKind.COUNT.name(), List.of());
  }
}
