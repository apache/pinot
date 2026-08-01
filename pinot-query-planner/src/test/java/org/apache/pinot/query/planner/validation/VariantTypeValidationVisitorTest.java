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

import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VariantTypeValidationVisitorTest {
  private static final DataSchema VARIANT_SCHEMA =
      new DataSchema(new String[]{"payload"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.VARIANT});
  private static final DataSchema TYPED_EXTRACTION_SCHEMA =
      new DataSchema(new String[]{"typedPayload"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});

  @Test
  public void testRejectsVariantOrderBy() {
    SortNode sortNode = new SortNode(0, VARIANT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(),
        List.of(new RelFieldCollation(0)), 10, 0);

    QueryException exception =
        Assert.expectThrows(QueryException.class, () -> sortNode.visit(VariantTypeValidationVisitor.INSTANCE, null));
    Assert.assertTrue(exception.getMessage().contains("ORDER BY"));
  }

  @Test
  public void testRawVariantProjectionRequiresNullHandling() {
    QueryException exception = Assert.expectThrows(QueryException.class,
        () -> VariantTypeValidationVisitor.validateResultSchema(VARIANT_SCHEMA, false));
    Assert.assertTrue(exception.getMessage().contains("requires query null handling"));

    VariantTypeValidationVisitor.validateResultSchema(VARIANT_SCHEMA, true);
    VariantTypeValidationVisitor.validateResultSchema(TYPED_EXTRACTION_SCHEMA, false);
  }

  @Test
  public void testRejectsEqualityDependentSetOperations() {
    List<SetOpNode> unsupportedNodes = List.of(
        setOp(SetOpNode.SetOpType.UNION, false),
        setOp(SetOpNode.SetOpType.INTERSECT, false),
        setOp(SetOpNode.SetOpType.INTERSECT, true),
        setOp(SetOpNode.SetOpType.MINUS, false),
        setOp(SetOpNode.SetOpType.MINUS, true));

    for (SetOpNode node : unsupportedNodes) {
      QueryException exception =
          Assert.expectThrows(QueryException.class, () -> node.visit(VariantTypeValidationVisitor.INSTANCE, null));
      Assert.assertTrue(exception.getMessage().contains("raw VARIANT"));
    }
  }

  @Test
  public void testAllowsVariantUnionAll() {
    setOp(SetOpNode.SetOpType.UNION, true).visit(VariantTypeValidationVisitor.INSTANCE, null);
  }

  @Test
  public void testRejectsRawVariantJoinKeysForEveryStrategy() {
    for (JoinNode.JoinStrategy strategy : JoinNode.JoinStrategy.values()) {
      JoinNode leftVariant = join(strategy, VARIANT_SCHEMA, TYPED_EXTRACTION_SCHEMA);
      QueryException exception = Assert.expectThrows(QueryException.class,
          () -> leftVariant.visit(VariantTypeValidationVisitor.INSTANCE, null));
      Assert.assertTrue(exception.getMessage().contains("JOIN keys"));

      JoinNode rightVariant = join(strategy, TYPED_EXTRACTION_SCHEMA, VARIANT_SCHEMA);
      exception = Assert.expectThrows(QueryException.class,
          () -> rightVariant.visit(VariantTypeValidationVisitor.INSTANCE, null));
      Assert.assertTrue(exception.getMessage().contains("JOIN keys"));
    }
  }

  @Test
  public void testRejectsRawVariantAsofMatchKeys() {
    DataSchema leftVariant = new DataSchema(new String[]{"key", "match"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.VARIANT});
    DataSchema rightTyped = new DataSchema(new String[]{"key", "match"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});
    QueryException exception = Assert.expectThrows(QueryException.class,
        () -> asofJoin(leftVariant, rightTyped).visit(VariantTypeValidationVisitor.INSTANCE, null));
    Assert.assertTrue(exception.getMessage().contains("Raw VARIANT values do not support ASOF JOIN match keys"));

    DataSchema leftTyped = new DataSchema(new String[]{"key", "match"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG});
    DataSchema rightVariant = new DataSchema(new String[]{"key", "match"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.VARIANT});
    exception = Assert.expectThrows(QueryException.class,
        () -> asofJoin(leftTyped, rightVariant).visit(VariantTypeValidationVisitor.INSTANCE, null));
    Assert.assertTrue(exception.getMessage().contains("Raw VARIANT values do not support ASOF JOIN match keys"));
  }

  @Test
  public void testRejectsAggregatesThatConsumeRawVariant() {
    for (String functionName : List.of("SUM", "ANYVALUE", "DISTINCTCOUNTHLL")) {
      AggregateNode node = aggregate(functionName, false, VARIANT_SCHEMA);
      QueryException exception =
          Assert.expectThrows(QueryException.class, () -> node.visit(VariantTypeValidationVisitor.INSTANCE, null));
      Assert.assertTrue(exception.getMessage().contains("Aggregate function " + functionName));
      Assert.assertTrue(exception.getMessage().contains("variantGet"));
    }
  }

  @Test
  public void testRejectsDistinctCountOfRawVariant() {
    AggregateNode node = aggregate("COUNT", true, VARIANT_SCHEMA);

    QueryException exception =
        Assert.expectThrows(QueryException.class, () -> node.visit(VariantTypeValidationVisitor.INSTANCE, null));
    Assert.assertTrue(exception.getMessage().contains("Aggregate function COUNT"));
  }

  @Test
  public void testAllowsRawVariantCount() {
    aggregate("COUNT", false, VARIANT_SCHEMA).visit(VariantTypeValidationVisitor.INSTANCE, null);
  }

  @Test
  public void testUsesLogicalTypeInsteadOfVariantStorageType() {
    DataSchema bytesSchema =
        new DataSchema(new String[]{"bytes"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.BYTES});
    aggregate("ANYVALUE", false, bytesSchema).visit(VariantTypeValidationVisitor.INSTANCE, null);
  }

  @Test
  public void testAllowsAggregateOverTypedVariantExtraction() {
    RexExpression.FunctionCall typedExtraction =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.STRING, "variantGet",
            List.of(new RexExpression.InputRef(0)));
    AggregateNode node = aggregate("MINSTRING", false, VARIANT_SCHEMA, typedExtraction);

    node.visit(VariantTypeValidationVisitor.INSTANCE, null);
  }

  @Test
  public void testValidatesWindowAggregateInputs() {
    QueryException exception = Assert.expectThrows(QueryException.class,
        () -> window("SUM").visit(VariantTypeValidationVisitor.INSTANCE, null));
    Assert.assertTrue(exception.getMessage().contains("Aggregate function SUM"));

    window("COUNT").visit(VariantTypeValidationVisitor.INSTANCE, null);
  }

  @Test
  public void testRejectsRawVariantWindowKeys() {
    QueryException exception = Assert.expectThrows(QueryException.class,
        () -> window("COUNT", VARIANT_SCHEMA, List.of(0), List.of())
            .visit(VariantTypeValidationVisitor.INSTANCE, null));
    Assert.assertTrue(exception.getMessage().contains("Window PARTITION BY"));

    exception = Assert.expectThrows(QueryException.class,
        () -> window("COUNT", VARIANT_SCHEMA, List.of(), List.of(new RelFieldCollation(0)))
            .visit(VariantTypeValidationVisitor.INSTANCE, null));
    Assert.assertTrue(exception.getMessage().contains("Window ORDER BY"));
  }

  @Test
  public void testAllowsWindowKeysOverTypedVariantExtraction() {
    window("COUNT", TYPED_EXTRACTION_SCHEMA, List.of(0), List.of(new RelFieldCollation(0)))
        .visit(VariantTypeValidationVisitor.INSTANCE, null);
  }

  private static SetOpNode setOp(SetOpNode.SetOpType setOpType, boolean all) {
    return new SetOpNode(0, VARIANT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), setOpType, all);
  }

  private static JoinNode join(JoinNode.JoinStrategy strategy, DataSchema leftSchema, DataSchema rightSchema) {
    ValueNode left = new ValueNode(0, leftSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    ValueNode right = new ValueNode(0, rightSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    DataSchema resultSchema =
        new DataSchema(new String[]{"result"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    return new JoinNode(0, resultSchema, PlanNode.NodeHint.EMPTY, List.of(left, right), JoinRelType.INNER, List.of(0),
        List.of(0), List.of(), strategy);
  }

  private static JoinNode asofJoin(DataSchema leftSchema, DataSchema rightSchema) {
    ValueNode left = new ValueNode(0, leftSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    ValueNode right = new ValueNode(0, rightSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    DataSchema resultSchema = new DataSchema(new String[]{"result"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    RexExpression matchCondition =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, "GREATER_THAN",
            List.of(new RexExpression.InputRef(1), new RexExpression.InputRef(leftSchema.size() + 1)));
    return new JoinNode(0, resultSchema, PlanNode.NodeHint.EMPTY, List.of(left, right), JoinRelType.ASOF, List.of(0),
        List.of(0), List.of(), JoinNode.JoinStrategy.ASOF, matchCondition);
  }

  private static AggregateNode aggregate(String functionName, boolean distinct, DataSchema inputSchema) {
    return aggregate(functionName, distinct, inputSchema, new RexExpression.InputRef(0));
  }

  private static AggregateNode aggregate(String functionName, boolean distinct, DataSchema inputSchema,
      RexExpression operand) {
    ValueNode input = new ValueNode(0, inputSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    RexExpression.FunctionCall aggCall = functionCall(functionName, distinct, operand);
    DataSchema resultSchema =
        new DataSchema(new String[]{"result"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    return new AggregateNode(0, resultSchema, PlanNode.NodeHint.EMPTY, List.of(input), List.of(aggCall), List.of(-1),
        List.of(), AggregateNode.AggType.DIRECT, false, List.of(), 0);
  }

  private static WindowNode window(String functionName) {
    return window(functionName, VARIANT_SCHEMA, List.of(), List.of());
  }

  private static WindowNode window(String functionName, DataSchema inputSchema, List<Integer> keys,
      List<RelFieldCollation> collations) {
    ValueNode input = new ValueNode(0, inputSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of());
    return new WindowNode(0, inputSchema, PlanNode.NodeHint.EMPTY, List.of(input), keys, collations,
        List.of(functionCall(functionName, false, new RexExpression.InputRef(0))), WindowNode.WindowFrameType.ROWS,
        Integer.MIN_VALUE, Integer.MAX_VALUE, WindowNode.WindowExclusion.NO_OTHERS, List.of());
  }

  private static RexExpression.FunctionCall functionCall(String functionName, boolean distinct,
      RexExpression operand) {
    return new RexExpression.FunctionCall(DataSchema.ColumnDataType.LONG, functionName, List.of(operand), distinct,
        false);
  }
}
