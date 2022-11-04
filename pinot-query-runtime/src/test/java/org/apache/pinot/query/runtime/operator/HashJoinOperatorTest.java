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
package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.hamcrest.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;


public class HashJoinOperatorTest {
  private static JoinNode.JoinKeys getJoinKeys(List<Integer> leftIdx, List<Integer> rightIdx) {
    FieldSelectionKeySelector leftSelect = new FieldSelectionKeySelector(leftIdx);
    FieldSelectionKeySelector rightSelect = new FieldSelectionKeySelector(rightIdx);
    return new JoinNode.JoinKeys(leftSelect, rightSelect);
  }
  @Mock
  Operator<TransferableBlock> _leftOperator;

  @Mock
  Operator<TransferableBlock> _rightOperator;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testHashJoinKeyCollisionInnerJoin() {
    // "Aa" and "BB" have same hash code in java.
    List<Object[]> rows = Arrays.asList(new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"});
    when(_leftOperator.nextBlock()).thenReturn(OperatorTestUtil.getRowDataBlock(rows))
        .thenReturn(OperatorTestUtil.getEndOfStreamRowBlock());
    when(_rightOperator.nextBlock()).thenReturn(OperatorTestUtil.getRowDataBlock(rows))
        .thenReturn(OperatorTestUtil.getEndOfStreamRowBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.STRING
    });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, JoinRelType.INNER);

    TransferableBlock result = join.getNextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows =
        Arrays.asList(new Object[]{1, "Aa", 1, "Aa"}, new Object[]{2, "BB", 2, "BB"}, new Object[]{2, "BB", 3, "BB"},
            new Object[]{3, "BB", 2, "BB"}, new Object[]{3, "BB", 3, "BB"});
    assertThat(expectedRows, Matchers.containsInAnyOrder(resultRows.toArray()));
  }

  @Test
  public void testInnerJoin() {
    List<Object[]> leftRows = Arrays.asList(new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"});
    when(_leftOperator.nextBlock()).thenReturn(OperatorTestUtil.getRowDataBlock(leftRows))
        .thenReturn(OperatorTestUtil.getEndOfStreamRowBlock());
    List<Object[]> rightRows = Arrays.asList(new Object[]{1, "AA"}, new Object[]{2, "Aa"});
    when(_rightOperator.nextBlock()).thenReturn(OperatorTestUtil.getRowDataBlock(rightRows))
        .thenReturn(OperatorTestUtil.getEndOfStreamRowBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.STRING
    });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, JoinRelType.INNER);

    TransferableBlock result = join.getNextBlock();
    List<Object[]> resultRows = result.getContainer();
    Object[] expRow = new Object[]{1, "Aa", 2, "Aa"};
    List<Object[]> expectedRows = new ArrayList<>();
    expectedRows.add(expRow);
    assertThat(expectedRows, Matchers.containsInAnyOrder(resultRows.toArray()));
  }

  @Test
  public void testLeftJoin() {
    List<Object[]> leftRows = Arrays.asList(new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"});
    when(_leftOperator.nextBlock()).thenReturn(OperatorTestUtil.getRowDataBlock(leftRows))
        .thenReturn(OperatorTestUtil.getEndOfStreamRowBlock());
    List<Object[]> rightRows = Arrays.asList(new Object[]{1, "AA"}, new Object[]{2, "Aa"});
    when(_rightOperator.nextBlock()).thenReturn(OperatorTestUtil.getRowDataBlock(rightRows))
        .thenReturn(OperatorTestUtil.getEndOfStreamRowBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.STRING
    });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, JoinRelType.LEFT);

    TransferableBlock result = join.getNextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "Aa", 2, "Aa"}, new Object[]{2, "BB", null, null},
        new Object[]{3, "BB", null, null});
    assertThat(expectedRows, Matchers.containsInAnyOrder(resultRows.toArray()));
  }
}
