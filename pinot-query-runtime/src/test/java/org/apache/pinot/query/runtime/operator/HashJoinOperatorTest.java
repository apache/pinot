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
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
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
  private MultiStageOperator _leftOperator;

  @Mock
  private MultiStageOperator _rightOperator;

  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  private static JoinNode.JoinKeys getJoinKeys(List<Integer> leftKeys, List<Integer> rightKeys) {
    return new JoinNode.JoinKeys(leftKeys, rightKeys);
  }

  private static List<RelHint> getJoinHints(Map<String, String> hintsMap) {
    RelHint.Builder relHintBuilder = RelHint.builder(PinotHintOptions.JOIN_HINT_OPTIONS);
    hintsMap.forEach(relHintBuilder::hintOption);
    return ImmutableList.of(relHintBuilder.build());
  }

  @Test
  public void shouldHandleHashJoinKeyCollisionInnerJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    List<RexExpression> joinClauses = new ArrayList<>();
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, Collections.emptyList());
    HashJoinOperator joinOnString =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);

    TransferableBlock result = joinOnString.nextBlock();
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
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_co2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, Collections.emptyList());
    HashJoinOperator joinOnInt =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);
    TransferableBlock result = joinOnInt.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{2, "BB", 2, "Aa"}, new Object[]{2, "BB", 2, "BB"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void shouldHandleJoinOnEmptySelector() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_co2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(new ArrayList<>(), new ArrayList<>()), joinClauses, Collections.emptyList());
    HashJoinOperator joinOnInt =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);
    TransferableBlock result = joinOnInt.nextBlock();
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
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "CC"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.LEFT,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);

    TransferableBlock result = join.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "Aa", 2, "Aa"}, new Object[]{2, "CC", null, null});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void shouldPassLeftTableEOS() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    List<RexExpression> joinClauses = new ArrayList<>();
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);

    TransferableBlock result = join.nextBlock();
    Assert.assertTrue(result.isEndOfStreamBlock());
  }

  @Test
  public void shouldHandleLeftJoinOneToN() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.LEFT,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);

    TransferableBlock result = join.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "Aa", 1, "BB"}, new Object[]{1, "Aa", 1, "CC"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
  }

  @Test
  public void shouldPassRightTableEOS() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });

    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);

    TransferableBlock result = join.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    Assert.assertTrue(resultRows.isEmpty());
  }

  @Test
  public void shouldHandleInequiJoinOnString() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
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
    joinClauses.add(new RexExpression.FunctionCall(SqlKind.NOT_EQUALS, ColumnDataType.BOOLEAN, "<>", functionOperands));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(new ArrayList<>(), new ArrayList<>()), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);
    TransferableBlock result = join.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows =
        Arrays.asList(new Object[]{1, "Aa", 2, "BB"}, new Object[]{1, "Aa", 3, "BB"}, new Object[]{2, "BB", 2, "Aa"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    for (int i = 0; i < expectedRows.size(); i++) {
      Assert.assertEquals(resultRows.get(i), expectedRows.get(i));
    }
  }

  @Test
  public void shouldHandleInequiJoinOnInt() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
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
    joinClauses.add(new RexExpression.FunctionCall(SqlKind.NOT_EQUALS, ColumnDataType.BOOLEAN, "<>", functionOperands));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(new ArrayList<>(), new ArrayList<>()), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);
    TransferableBlock result = join.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{1, "Aa", 2, "Aa"}, new Object[]{2, "BB", 1, "BB"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    for (int i = 0; i < expectedRows.size(); i++) {
      Assert.assertEquals(resultRows.get(i), expectedRows.get(i));
    }
  }

  @Test
  public void shouldHandleRightJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
    });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.RIGHT,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, Collections.emptyList());
    HashJoinOperator joinOnNum =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);
    TransferableBlock result = joinOnNum.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = Arrays.asList(new Object[]{2, "BB", 2, "Aa"}, new Object[]{2, "BB", 2, "BB"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
    // Second block should be non-matched broadcast rows
    result = joinOnNum.nextBlock();
    resultRows = result.getContainer();
    expectedRows = ImmutableList.of(new Object[]{null, null, 3, "BB"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    // Third block is EOS block.
    result = joinOnNum.nextBlock();
    Assert.assertTrue(result.isSuccessfulEndOfStreamBlock());
  }

  @Test
  public void shouldHandleSemiJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{4, "CC"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
    });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.SEMI,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);
    TransferableBlock result = join.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows =
        ImmutableList.of(new Object[]{1, "Aa", null, null}, new Object[]{2, "BB", null, null});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
    result = join.nextBlock();
    Assert.assertTrue(result.isSuccessfulEndOfStreamBlock());
  }

  @Test
  public void shouldHandleFullJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{4, "CC"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
    });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.FULL,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);
    TransferableBlock result = join.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = ImmutableList.of(new Object[]{1, "Aa", null, null}, new Object[]{2, "BB", 2, "Aa"},
        new Object[]{2, "BB", 2, "BB"}, new Object[]{4, "CC", null, null});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    Assert.assertEquals(resultRows.get(1), expectedRows.get(1));
    Assert.assertEquals(resultRows.get(2), expectedRows.get(2));
    Assert.assertEquals(resultRows.get(3), expectedRows.get(3));
    // Second block should be non-matched broadcast rows
    result = join.nextBlock();
    resultRows = result.getContainer();
    expectedRows = ImmutableList.of(new Object[]{null, null, 3, "BB"});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    // Third block is EOS block.
    result = join.nextBlock();
    Assert.assertTrue(result.isSuccessfulEndOfStreamBlock());
  }

  @Test
  public void shouldHandleAntiJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{4, "CC"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
    });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.ANTI,
        getJoinKeys(Arrays.asList(1), Arrays.asList(1)), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);
    TransferableBlock result = join.nextBlock();
    List<Object[]> resultRows = result.getContainer();
    List<Object[]> expectedRows = ImmutableList.of(new Object[]{4, "CC", null, null});
    Assert.assertEquals(resultRows.size(), expectedRows.size());
    Assert.assertEquals(resultRows.get(0), expectedRows.get(0));
    result = join.nextBlock();
    Assert.assertTrue(result.isSuccessfulEndOfStreamBlock());
  }

  @Test
  public void shouldPropagateRightTableError() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("testInnerJoinRightError")));

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);

    TransferableBlock result = join.nextBlock();
    Assert.assertTrue(result.isErrorBlock());
    Assert.assertTrue(
        result.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("testInnerJoinRightError"));
  }

  @Test
  public void shouldPropagateLeftTableError() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("testInnerJoinLeftError")));

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, Collections.emptyList());
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);

    TransferableBlock result = join.nextBlock();
    Assert.assertTrue(result.isErrorBlock());
    Assert.assertTrue(result.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("testInnerJoinLeftError"));
  }

  @Test
  public void shouldPropagateJoinLimitError() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    Map<String, String> hintsMap = ImmutableMap.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "THROW",
        PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "1");
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, getJoinHints(hintsMap));
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);

    TransferableBlock result = join.nextBlock();
    Assert.assertTrue(result.isErrorBlock());
    Assert.assertTrue(result.getExceptions().get(QueryException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE)
        .contains("reach number of rows limit"));
  }

  @Test
  public void shouldHandleJoinWithPartialResultsWhenHitDataRowsLimit() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    Mockito.when(_leftOperator.nextBlock())
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());
    Mockito.when(_rightOperator.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    List<RexExpression> joinClauses = new ArrayList<>();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    Map<String, String> hintsMap = ImmutableMap.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "BREAK",
        PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "1");
    JoinNode node = new JoinNode(1, resultSchema, leftSchema, rightSchema, JoinRelType.INNER,
        getJoinKeys(Arrays.asList(0), Arrays.asList(0)), joinClauses, getJoinHints(hintsMap));
    HashJoinOperator join =
        new HashJoinOperator(OperatorTestUtil.getDefaultContext(), _leftOperator, _rightOperator, leftSchema, node);

    TransferableBlock result = join.nextBlock();
    Mockito.verify(_rightOperator).setEarlyTerminate();
    Assert.assertFalse(result.isErrorBlock());
    Assert.assertEquals(result.getNumRows(), 1);
    Assert.assertTrue(result.getExceptions().get(QueryException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE)
        .contains("reach number of rows limit"));
  }
}
// TODO: Add more inequi join tests.
