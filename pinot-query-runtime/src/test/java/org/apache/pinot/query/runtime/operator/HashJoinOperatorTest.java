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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HashJoinOperatorTest {
  private AutoCloseable _mocks;

  @Mock
  private Operator<TransferableBlock> _leftOperator;

  @Mock
  private Operator<TransferableBlock> _rightOperator;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  private static JoinNode.JoinKeys getJoinKeys(List<Integer> leftIdx, List<Integer> rightIdx) {
    FieldSelectionKeySelector leftSelect = new FieldSelectionKeySelector(leftIdx);
    FieldSelectionKeySelector rightSelect = new FieldSelectionKeySelector(rightIdx);
    return new JoinNode.JoinKeys(leftSelect, rightSelect);
  }

  @Test
  public void shouldHandleHashJoinKeyCollisionInnerJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    List<RexExpression> joinClauses = new ArrayList<>();
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator joinOnString = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, JoinRelType.INNER);

    TransferableBlock result = joinOnString.nextBlock();
    while (result.isNoOpBlock()) {
      result = joinOnString.nextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows =
        Arrays.asList(new Object[]{1, "Aa", 2, "Aa"}, new Object[]{2, "BB", 2, "BB"}, new Object[]{2, "BB", 3, "BB"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
    Assert.assertEquals(resultRows.get(2), expectedRows.get(2));
  }

  @Test
  public void shouldHandleInnerJoinOnInt() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_co2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator joinOnInt = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, JoinRelType.INNER);
    TransferableBlock result = joinOnInt.nextBlock();
    while (result.isNoOpBlock()) {
      result = joinOnInt.nextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{2, "BB", 2, "Aa"}, new Object[]{2, "BB", 2, "BB"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void shouldHandleJoinOnEmptySelector() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_co2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator joinOnInt = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(new ArrayList<>(), new ArrayList<>()), joinClauses, JoinRelType.INNER);
    TransferableBlock result = joinOnInt.nextBlock();
    while (result.isNoOpBlock()) {
      result = joinOnInt.nextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows =
        Arrays.asList(new Object[]{1, "Aa", 2, "Aa"}, new Object[]{1, "Aa", 2, "BB"}, new Object[]{1, "Aa", 3, "BB"},
            new Object[]{2, "BB", 2, "Aa"}, new Object[]{2, "BB", 2, "BB"}, new Object[]{2, "BB", 3, "BB"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
    Assert.assertEquals(resultRows.get(2), expectedRows.get(2));
    Assert.assertEquals(resultRows.get(3), expectedRows.get(3));
    Assert.assertEquals(resultRows.get(4), expectedRows.get(4));
    Assert.assertEquals(resultRows.get(5), expectedRows.get(5));
  }

  @Test
  public void shouldHandleLeftJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "CC"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, JoinRelType.LEFT);

    TransferableBlock result = join.nextBlock();
    while (result.isNoOpBlock()) {
      result = join.nextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "Aa", 2, "Aa"}, new Object[]{2, "CC", null, null});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void shouldPassLeftTableEOS() {
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, JoinRelType.INNER);

    TransferableBlock result = join.nextBlock();
    while (result.isNoOpBlock()) {
      result = join.nextBlock();
    }
    Assert.assertTrue(result.isEndOfStreamBlock());
  }

  @Test
  public void shouldHandleLeftJoinOneToN() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, JoinRelType.LEFT);

    TransferableBlock result = join.nextBlock();
    while (result.isNoOpBlock()) {
      result = join.nextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "Aa", 1, "BB"}, new Object[]{1, "Aa", 1, "CC"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void shouldPassRightTableEOS() {
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, JoinRelType.INNER);

    TransferableBlock result = join.nextBlock();
    while (result.isNoOpBlock()) {
      result = join.nextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    Assert.assertTrue(resultRows.isEmpty());
  }

  @Test
  public void shouldHandleInequiJoinOnString() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    List<RexExpression> functionOperands = new ArrayList<>();
    functionOperands.add(new RexExpression.InputRef(1));
    functionOperands.add(new RexExpression.InputRef(3));
    joinClauses.add(
        new RexExpression.FunctionCall(SqlKind.NOT_EQUALS, FieldSpec.DataType.STRING, "NOT_EQUALS", functionOperands));
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(new ArrayList<>(), new ArrayList<>()), joinClauses, JoinRelType.INNER);

    TransferableBlock result = join.nextBlock();
    while (result.isNoOpBlock()) {
      result = join.nextBlock();
    }
    Assert.assertTrue(result.isErrorBlock());
    MetadataBlock errorBlock = (MetadataBlock) result.getDataBlock();
    Assert.assertTrue(errorBlock.getExceptions().get(1000).matches(".*notEquals.*"));
  }

  @Test
  public void shouldHandleInequiJoinOnInt() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{1, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    List<RexExpression> functionOperands = new ArrayList<>();
    functionOperands.add(new RexExpression.InputRef(0));
    functionOperands.add(new RexExpression.InputRef(2));
    joinClauses.add(
        new RexExpression.FunctionCall(SqlKind.NOT_EQUALS, FieldSpec.DataType.STRING, "NOT_EQUALS", functionOperands));
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(new ArrayList<>(), new ArrayList<>()), joinClauses, JoinRelType.INNER);

    TransferableBlock result = join.nextBlock();
    while (result.isNoOpBlock()) {
      result = join.nextBlock();
    }
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "Aa", 2, "Aa"}, new Object[]{2, "BB", 1, "BB"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*RIGHT is not supported"
      + ".*")
  public void shouldThrowOnRightJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.STRING
    });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, JoinRelType.RIGHT);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*SEMI is not "
      + "supported.*")
  public void shouldThrowOnSemiJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.STRING
    });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, JoinRelType.SEMI);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*FULL is not supported.*")
  public void shouldThrowOnFullJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.STRING
    });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, JoinRelType.FULL);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*ANTI is not supported.*")
  public void shouldThrowOnAntiJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.STRING
    });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, JoinRelType.ANTI);
  }

  @Test
  public void shouldPropagateRightTableError() {
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("testInnerJoinRightError")));

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, JoinRelType.INNER);

    TransferableBlock result = join.nextBlock();
    while (result.isNoOpBlock()) {
      result = join.nextBlock();
    }
    Assert.assertTrue(result.isErrorBlock());
    Assert.assertTrue(result.getDataBlock().getExceptions().get(QueryException.UNKNOWN_ERROR_CODE)
        .matches("testInnerJoinRightError"));
  }

  @Test
  public void shouldPropagateLeftTableError() {
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("testInnerJoinLeftError")));

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, JoinRelType.INNER);

    TransferableBlock result = join.nextBlock();
    while (result.isNoOpBlock()) {
      result = join.nextBlock();
    }
    Assert.assertTrue(result.isErrorBlock());
    Assert.assertTrue(
        result.getDataBlock().getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).matches("testInnerJoinLeftError"));
  }

  @Test
  public void shouldHandleNoOpBlock() {
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(OperatorTestUtil.block(rightSchema, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock())
        .thenReturn(OperatorTestUtil.block(rightSchema, new Object[]{2, "CC"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}))
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock())
        .thenReturn(OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}))
        .thenReturn(TransferableBlockUtils.getNoOpTransferableBlock())
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.STRING
        });
    HashJoinOperator join = new HashJoinOperator(_leftOperator, _rightOperator, resultSchema,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, JoinRelType.INNER);

    TransferableBlock result = join.nextBlock(); // first no-op consumes first right data block.
    Assert.assertTrue(result.isNoOpBlock());
    result = join.nextBlock(); // second no-op consumes no-op right block.
    Assert.assertTrue(result.isNoOpBlock());
    result = join.nextBlock(); // third no-op consumes another right data block.
    Assert.assertTrue(result.isNoOpBlock());
    result = join.nextBlock(); // forth no-op consumes another right data block.
    Assert.assertTrue(result.isNoOpBlock());
    result = join.nextBlock();   // build result using the first left block
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = ImmutableList.of(new Object[]{2, "BB", 2, "Aa"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    result = join.nextBlock();     // second left block is no-op
    Assert.assertTrue(result.isNoOpBlock());
    result = join.nextBlock(); // third left block consumes some extra data
    expectedRows = ImmutableList.of(new Object[]{2, "CC", 2, "Aa"});
    resultRows = result.getContainer();
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    result = join.nextBlock(); // last one is EOS.
    Assert.assertTrue(result.isEndOfStreamBlock());
  }
}
