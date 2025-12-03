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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.testng.Assert;
import org.testng.annotations.Test;


public class UnnestNodeTest {

  @Test
  public void testSingleArrayConstructor() {
    DataSchema dataSchema = new DataSchema(new String[]{"id", "elem"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
    RexExpression.InputRef arrayExpr = new RexExpression.InputRef(1);

    UnnestNode node = new UnnestNode(0, dataSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), arrayExpr, "elem", false, null);

    // Test backward compatibility methods
    Assert.assertEquals(node.getArrayExpr(), arrayExpr);
    Assert.assertEquals(node.getElementIndex(), UnnestNode.UNSPECIFIED_INDEX);

    // Test new methods
    Assert.assertEquals(node.getArrayExprs().size(), 1);
    Assert.assertEquals(node.getArrayExprs().get(0), arrayExpr);
    Assert.assertEquals(node.getElementIndexes().size(), 1);
    Assert.assertEquals(node.getElementIndexes().get(0).intValue(), UnnestNode.UNSPECIFIED_INDEX);
    Assert.assertFalse(node.isWithOrdinality());
  }

  @Test
  public void testSingleArrayWithOrdinalityConstructor() {
    DataSchema dataSchema = new DataSchema(new String[]{"id", "elem", "ord"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT
        });
    RexExpression.InputRef arrayExpr = new RexExpression.InputRef(1);

    UnnestNode node = new UnnestNode(0, dataSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), arrayExpr, "elem", true, "ord", 1, 2);

    // Test backward compatibility methods
    Assert.assertEquals(node.getArrayExpr(), arrayExpr);
    Assert.assertEquals(node.getElementIndex(), 1);
    Assert.assertEquals(node.getOrdinalityIndex(), 2);

    // Test new methods
    Assert.assertEquals(node.getArrayExprs().size(), 1);
    Assert.assertEquals(node.getElementIndexes().size(), 1);
    Assert.assertEquals(node.getElementIndexes().get(0).intValue(), 1);
    Assert.assertTrue(node.isWithOrdinality());
  }

  @Test
  public void testMultipleArraysConstructor() {
    DataSchema dataSchema = new DataSchema(new String[]{"id", "longVal", "strVal"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.LONG,
            DataSchema.ColumnDataType.STRING
        });
    RexExpression.InputRef longArrayExpr = new RexExpression.InputRef(1);
    RexExpression.InputRef stringArrayExpr = new RexExpression.InputRef(2);
    List<RexExpression> arrayExprs = List.of(longArrayExpr, stringArrayExpr);
    List<String> columnAliases = List.of("longVal", "strVal");

    UnnestNode node = new UnnestNode(0, dataSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), arrayExprs, columnAliases, false, null);

    // Test new methods
    Assert.assertEquals(node.getArrayExprs().size(), 2);
    Assert.assertEquals(node.getArrayExprs().get(0), longArrayExpr);
    Assert.assertEquals(node.getArrayExprs().get(1), stringArrayExpr);
    Assert.assertEquals(node.getElementIndexes().size(), 2);
    Assert.assertEquals(node.getElementIndexes().get(0).intValue(), UnnestNode.UNSPECIFIED_INDEX);
    Assert.assertEquals(node.getElementIndexes().get(1).intValue(), UnnestNode.UNSPECIFIED_INDEX);
    Assert.assertFalse(node.isWithOrdinality());

    // Test backward compatibility methods (should return first element)
    Assert.assertEquals(node.getArrayExpr(), longArrayExpr);
    Assert.assertEquals(node.getElementIndex(), UnnestNode.UNSPECIFIED_INDEX);
  }

  @Test
  public void testMultipleArraysWithOrdinalityConstructor() {
    DataSchema dataSchema = new DataSchema(new String[]{"id", "longVal", "strVal", "ord"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.LONG,
            DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT
        });
    RexExpression.InputRef longArrayExpr = new RexExpression.InputRef(1);
    RexExpression.InputRef stringArrayExpr = new RexExpression.InputRef(2);
    List<RexExpression> arrayExprs = List.of(longArrayExpr, stringArrayExpr);
    List<String> columnAliases = List.of("longVal", "strVal");
    List<Integer> elementIndexes = List.of(1, 2);

    UnnestNode node = new UnnestNode(0, dataSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), arrayExprs, columnAliases, true, "ord", elementIndexes, 3);

    // Test new methods
    Assert.assertEquals(node.getArrayExprs().size(), 2);
    Assert.assertEquals(node.getArrayExprs().get(0), longArrayExpr);
    Assert.assertEquals(node.getArrayExprs().get(1), stringArrayExpr);
    Assert.assertEquals(node.getElementIndexes().size(), 2);
    Assert.assertEquals(node.getElementIndexes().get(0).intValue(), 1);
    Assert.assertEquals(node.getElementIndexes().get(1).intValue(), 2);
    Assert.assertTrue(node.isWithOrdinality());
    Assert.assertEquals(node.getOrdinalityIndex(), 3);

    // Test backward compatibility methods (should return first element)
    Assert.assertEquals(node.getArrayExpr(), longArrayExpr);
    Assert.assertEquals(node.getElementIndex(), 1);
  }

  @Test
  public void testWithInputs() {
    DataSchema dataSchema = new DataSchema(new String[]{"id", "elem"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
    RexExpression.InputRef arrayExpr = new RexExpression.InputRef(1);
    List<RexExpression> arrayExprs = List.of(arrayExpr);
    List<String> columnAliases = List.of("elem");

    UnnestNode node1 = new UnnestNode(0, dataSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), arrayExprs, columnAliases, false, null);

    PlanNode input = new TableScanNode(0, dataSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), "testTable", List.of("id", "arr"));
    UnnestNode node2 = (UnnestNode) node1.withInputs(List.of(input));

    Assert.assertEquals(node2.getInputs().size(), 1);
    Assert.assertEquals(node2.getInputs().get(0), input);
    Assert.assertEquals(node2.getArrayExprs(), node1.getArrayExprs());
    Assert.assertEquals(node2.getElementIndexes(), node1.getElementIndexes());
    Assert.assertNotSame(node2.getTableFunctionContext(), node1.getTableFunctionContext());
    Assert.assertEquals(node2.getTableFunctionContext(), node1.getTableFunctionContext());
  }

  @Test
  public void testEqualsAndHashCode() {
    DataSchema dataSchema = new DataSchema(new String[]{"id", "elem1", "elem2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.LONG,
            DataSchema.ColumnDataType.STRING
        });
    RexExpression.InputRef arrayExpr1 = new RexExpression.InputRef(1);
    RexExpression.InputRef arrayExpr2 = new RexExpression.InputRef(2);
    List<RexExpression> arrayExprs = List.of(arrayExpr1, arrayExpr2);
    List<String> columnAliases = List.of("elem1", "elem2");
    List<Integer> elementIndexes = List.of(1, 2);

    UnnestNode node1 = new UnnestNode(0, dataSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), arrayExprs, columnAliases, false, null, elementIndexes, -1);
    UnnestNode node2 = new UnnestNode(0, dataSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), arrayExprs, columnAliases, false, null, elementIndexes, -1);
    UnnestNode node3 = new UnnestNode(0, dataSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), List.of(arrayExpr1), List.of("elem1"), false, null, List.of(1), -1);

    Assert.assertEquals(node1, node2);
    Assert.assertEquals(node1.hashCode(), node2.hashCode());
    Assert.assertNotEquals(node1, node3);
  }
}
