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

import it.unimi.dsi.fastutil.HashCommon;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.query.runtime.operator.BaseJoinOperator.StatKey;
import org.apache.pinot.query.runtime.operator.factory.DefaultJoinOperatorFactory;
import org.apache.pinot.query.runtime.operator.join.ArrowJoinOutput;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * Differential semantics, capability boundaries and explicit ownership for the native join subset.
 */
public class ArrowHashJoinOperatorTest {
  private static final DataSchema INT_SCHEMA =
      new DataSchema(new String[]{"key", "value"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});

  private RootAllocator _root;
  private ArrowQueryContext _arrow;
  private OpChainExecutionContext _context;
  private List<MultiStageOperator> _operators;

  @BeforeMethod
  public void setUp() {
    _root = new RootAllocator(64L * 1024 * 1024);
    _arrow = new ArrowQueryContext(_root.newChildAllocator("join", 0, _root.getLimit()));
    _context = spy(OperatorTestUtil.getNoTracingContext());
    doReturn(true).when(_context).isArrowEnabled();
    doReturn(_arrow).when(_context).getOrCreateArrowContext();
    doReturn(Long.MAX_VALUE).when(_context).getActiveDeadlineMs();
    _operators = new ArrayList<>();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    try {
      for (MultiStageOperator operator : _operators) {
        operator.close();
      }
      assertEquals(_arrow.getLiveBlockCount(), 0, "Operators must release blocks before query-end cleanup");
      assertEquals(_arrow.getAllocator().getAllocatedMemory(), 0L);
    } finally {
      _arrow.close();
      _root.close();
    }
  }

  @DataProvider
  public Object[][] booleanVectors() {
    return new Object[][]{{true, false}, {false, true}, {true, true}};
  }

  @Test(dataProvider = "booleanVectors")
  public void testIntegerBackedBooleanKeysAndPayloads(boolean integerLeft, boolean integerRight) {
    DataSchema schema = new DataSchema(new String[]{"key", "payload"},
        new ColumnDataType[]{ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN});
    ArrowHashJoinOperator operator = join(
        new BlockListMultiStageOperator(_context, booleanBlock(schema, integerLeft)), schema,
        new BlockListMultiStageOperator(_context, booleanBlock(schema, integerRight)), schema,
        node(schema, schema, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    operator.enableArrowOutput();
    assertRows(readRows(operator, true),
        List.of(new Object[]{0, 1, 0, 1}, new Object[]{1, 0, 1, 0}));
    assertEquals(_arrow.getLiveBlockCount(), 0);
  }

  @DataProvider
  public Object[][] supportedJoins() {
    List<Object[]> cases = new ArrayList<>();
    for (JoinRelType join : List.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.SEMI, JoinRelType.ANTI)) {
      for (ColumnDataType type : List.of(ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT,
          ColumnDataType.DOUBLE, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP)) {
        cases.add(new Object[]{join, type, false});
        cases.add(new Object[]{join, type, true});
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "supportedJoins")
  public void testMatchesLegacyAcrossBlocksAndNulls(JoinRelType join, ColumnDataType keyType, boolean nativeInput) {
    DataSchema schema = new DataSchema(
        new String[]{"key", "text", "bytes", "i", "l", "f", "d", "b", "ts", "json"},
        new ColumnDataType[]{keyType, ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.INT,
            ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE, ColumnDataType.BOOLEAN,
            ColumnDataType.TIMESTAMP, ColumnDataType.JSON});
    Object[] keys = keys(keyType);
    List<Object[]> leftRows = new ArrayList<>();
    List<Object[]> rightRows = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      leftRows.add(payload(keys[i], i));
      leftRows.add(payload(keys[i], i + 1));
      if (i != 1) {
        rightRows.add(payload(keys[i], i + 3));
        if (i % 2 == 0) {
          rightRows.add(payload(keys[i], i + 4));
        }
      }
    }
    leftRows.add(payload(null, 0));
    leftRows.add(payload(null, 1));
    rightRows.add(payload(null, 2));
    JoinNode node = node(schema, schema, join, PlanNode.NodeHint.EMPTY);
    List<MseBlock> leftBlocks = blocks(schema, leftRows, 4);
    List<MseBlock> rightBlocks = blocks(schema, rightRows, 3);
    List<Object[]> expected;
    try (HashJoinOperator legacy = new HashJoinOperator(_context,
        new BlockListMultiStageOperator(_context, leftBlocks), schema,
        new BlockListMultiStageOperator(_context, rightBlocks), node)) {
      expected = readRows(legacy, false);
    }
    MultiStageOperator left = input(leftBlocks, nativeInput);
    MultiStageOperator right = input(rightBlocks, nativeInput);
    ArrowHashJoinOperator operator = join(left, schema, right, schema, node);
    operator.enableArrowOutput();
    assertRows(readRows(operator, true), expected);
    operator.close();
  }

  @Test
  public void testLinearProbeWraparoundAndFullIntegerDomain() {
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{Integer.MIN_VALUE, "minimum"});
    rows.add(new Object[]{Integer.MAX_VALUE, "maximum"});
    for (int key = 0; rows.size() < 10; key++) {
      if (((int) HashCommon.mix((long) key) & 31) == 31) {
        rows.add(new Object[]{key, "collision"});
      }
    }
    JoinNode node = node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY);
    ArrowHashJoinOperator operator = join(input(blocks(INT_SCHEMA, rows, 3), true), INT_SCHEMA,
        input(blocks(INT_SCHEMA, rows, 2), true), INT_SCHEMA, node);
    List<Object[]> actual = readRows(operator, false);
    assertEquals(actual.size(), rows.size());
    for (int i = 0; i < rows.size(); i++) {
      assertEquals(actual.get(i), new Object[]{rows.get(i)[0], rows.get(i)[1], rows.get(i)[0], rows.get(i)[1]});
    }
  }

  @DataProvider
  public Object[][] emptyJoins() {
    List<Object[]> cases = new ArrayList<>();
    for (JoinRelType join : List.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.SEMI, JoinRelType.ANTI)) {
      cases.add(new Object[]{join, false});
      cases.add(new Object[]{join, true});
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "emptyJoins")
  public void testEmptyBuildAndProbe(JoinRelType type, boolean emptyLeft) {
    List<Object[]> rows = emptyLeft ? List.of() : List.of(new Object[]{1, "one"}, new Object[]{null, null});
    ArrowHashJoinOperator operator = join(input(blocks(INT_SCHEMA, rows, 3), true), INT_SCHEMA,
        input(blocks(INT_SCHEMA, List.of(), 3), true), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, type, PlanNode.NodeHint.EMPTY));
    operator.enableArrowOutput();
    List<Object[]> result = readRows(operator, true);
    assertEquals(result.size(), !emptyLeft && (type == JoinRelType.LEFT || type == JoinRelType.ANTI) ? 2 : 0);
    if (!result.isEmpty() && type == JoinRelType.LEFT) {
      assertEquals(result.get(0), new Object[]{1, "one", null, null});
      assertEquals(result.get(1), new Object[]{null, null, null, null});
    }
  }

  @Test
  public void testDuplicateChainsSpanBoundedOutputBatches() {
    List<Object[]> rightRows = new ArrayList<>();
    int duplicates = 2 * ArrowJoinOutput.MAX_ROWS_PER_BLOCK + 1;
    for (int i = 0; i < duplicates; i++) {
      rightRows.add(new Object[]{1, "r" + i});
    }
    ArrowHashJoinOperator operator = join(
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "a"}, new Object[]{1, "b"})), true),
        INT_SCHEMA, input(blocks(INT_SCHEMA, rightRows, 333), true), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    operator.enableArrowOutput();
    List<Object[]> result = readRows(operator, true);
    assertEquals(result.size(), 2 * duplicates);
    for (int i = 0; i < result.size(); i++) {
      assertEquals(result.get(i), new Object[]{1, i < duplicates ? "a" : "b", 1, "r" + i % duplicates});
    }
  }

  @Test
  public void testDifferentDictionariesWithEqualCardinality() {
    ArrowBlock first = ArrowBlockConverter.toArrowBlock(OperatorTestUtil.block(INT_SCHEMA,
        new Object[]{1, "red"}, new Object[]{1, "red"}, new Object[]{2, "blue"}), _arrow);
    ArrowBlock second = ArrowBlockConverter.toArrowBlock(OperatorTestUtil.block(INT_SCHEMA,
        new Object[]{1, "green"}, new Object[]{2, "yellow"}, new Object[]{2, "yellow"}), _arrow);
    assertTrue(first.getDataBlock().getRoot().getVector(1).getField().getDictionary() != null);
    assertTrue(second.getDataBlock().getRoot().getVector(1).getField().getDictionary() != null);
    ArrowHashJoinOperator operator = join(
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"}, new Object[]{2, "left"})), true),
        INT_SCHEMA, new BlockListMultiStageOperator(_context, first, second), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    operator.enableArrowOutput();
    ArrowBlock output = (ArrowBlock) operator.nextBlock();
    try {
      assertTrue(output.getDataBlock().getRoot().getVector(1).getField().getDictionary() != null);
      assertTrue(output.getDataBlock().getRoot().getVector(3).getField().getDictionary() != null);
      assertEquals(output.getDataBlock().getDictionaryProvider().lookup(1).getVector().getValueCount(), 1);
      assertEquals(output.getDataBlock().getDictionaryProvider().lookup(3).getVector().getValueCount(), 4);
      assertTrue(operator.nextBlock().isSuccess());
      assertRows(output.asRowHeap().getRows(), List.of(
          new Object[]{1, "left", 1, "red"}, new Object[]{1, "left", 1, "red"},
          new Object[]{1, "left", 1, "green"}, new Object[]{2, "left", 2, "blue"},
          new Object[]{2, "left", 2, "yellow"}, new Object[]{2, "left", 2, "yellow"}));
    } finally {
      output.release();
    }
    assertTrue(operator.nextBlock().isSuccess());
    assertEquals(operator.copyStatMaps().getLong(StatKey.EMITTED_ROWS), 6L);
    assertEquals(operator.copyStatMaps().getLong(StatKey.MAX_ROWS_IN_JOIN), 6L);
  }

  @Test
  public void testCompactDictionariesAcrossBatchesOutliveBuildState() {
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 2 * ArrowJoinOutput.MAX_ROWS_PER_BLOCK; i++) {
      rows.add(new Object[]{1, "r" + i % 3});
    }
    ArrowHashJoinOperator operator = join(
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"})), true), INT_SCHEMA,
        input(List.of(new RowHeapDataBlock(rows, INT_SCHEMA)), true), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    operator.enableArrowOutput();
    ArrowBlock first = (ArrowBlock) operator.nextBlock();
    try {
      ArrowBlock second = (ArrowBlock) operator.nextBlock();
      try {
        assertTrue(operator.nextBlock().isSuccess());
        assertEquals(_arrow.getLiveBlockCount(), 2);
        int start = 0;
        for (ArrowBlock output : List.of(first, second)) {
          assertEquals(output.getDataBlock().getDictionaryProvider().lookup(3).getVector().getValueCount(), 3);
          List<Object[]> actual = output.asRowHeap().getRows();
          assertEquals(actual.size(), ArrowJoinOutput.MAX_ROWS_PER_BLOCK);
          for (int i = 0; i < actual.size(); i++) {
            assertEquals(actual.get(i), new Object[]{1, "left", 1, "r" + (start + i) % 3});
          }
          start += actual.size();
        }
      } finally {
        second.release();
      }
    } finally {
      first.release();
    }
  }

  @DataProvider
  public Object[][] highDictionaryCardinalities() {
    return new Object[][]{{3}, {4}};
  }

  @Test(dataProvider = "highDictionaryCardinalities")
  public void testHighCardinalityDictionaryFallsBackToPlainOutput(int cardinality) {
    ArrowBlock right = dictionaryBlock(1, 3, List.of("a", "b", "c", "unused").subList(0, cardinality));
    assertTrue(right.getDataBlock().getRoot().getVector(1).getField().getDictionary() != null);
    ArrowHashJoinOperator operator = join(
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"})), true), INT_SCHEMA,
        new BlockListMultiStageOperator(_context, right), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    operator.enableArrowOutput();
    ArrowBlock result = (ArrowBlock) operator.nextBlock();
    try {
      assertTrue(result.getDataBlock().getRoot().getVector(3).getField().getDictionary() == null);
      assertRows(result.asRowHeap().getRows(), List.of(
          new Object[]{1, "left", 1, "a"}, new Object[]{1, "left", 1, "b"}, new Object[]{1, "left", 1, "c"}));
    } finally {
      result.release();
    }
  }

  @Test
  public void testUnmatchedPlainBuildTailDoesNotExpandDictionaryOutput() {
    String value = "x".repeat(64 * 1024);
    ArrowBlock encoded = dictionaryBlock(1, 1024, List.of(value));
    ArrowBlock plain = ArrowBlockConverter.toArrowBlock(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{2, "unmatched"}), _arrow);
    ArrowBlock left = ArrowBlockConverter.toArrowBlock(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"}), _arrow);
    try (ArrowQueryContext limited = new ArrowQueryContext(_root.newChildAllocator("compact-output", 0, 1024 * 1024))) {
      doReturn(limited).when(_context).getOrCreateArrowContext();
      ArrowHashJoinOperator operator = join(new BlockListMultiStageOperator(_context, left), INT_SCHEMA,
          new BlockListMultiStageOperator(_context, encoded, plain), INT_SCHEMA,
          node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
      operator.enableArrowOutput();
      MseBlock block = operator.nextBlock();
      assertTrue(block instanceof ArrowBlock, "Compact output must fit without expanding a 64-KiB value per row");
      ArrowBlock output = (ArrowBlock) block;
      try {
        assertEquals(output.getNumRows(), 1024);
        assertEquals(output.getDataBlock().getDictionaryProvider().lookup(3).getVector().getValueCount(), 1);
        assertTrue(operator.nextBlock().isSuccess());
        assertEquals(output.getDataBlock().getString(0, 3), value);
        assertEquals(output.getDataBlock().getString(1023, 3), value);
      } finally {
        output.release();
      }
      operator.close();
      assertEquals(limited.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testAcceptedBuildPrefixPreservesCompactDictionary() {
    String value = "x".repeat(64 * 1024);
    ArrowBlock source = dictionaryBlock(1, 2 * ArrowJoinOutput.MAX_ROWS_PER_BLOCK, List.of(value));
    try (ArrowQueryContext limited = new ArrowQueryContext(_root.newChildAllocator("compact-prefix", 0, 1024 * 1024))) {
      int count = ArrowJoinOutput.MAX_ROWS_PER_BLOCK + 1;
      ArrowBlock prefix = ArrowJoinOutput.copyPrefix(source, count, limited, () -> { });
      try {
        assertEquals(prefix.getNumRows(), count);
        assertEquals(prefix.getDataBlock().getDictionaryProvider().lookup(1).getVector().getValueCount(), 1);
        assertEquals(prefix.getDataBlock().getString(count - 1, 1), value);
      } finally {
        prefix.release();
      }
      assertEquals(limited.getAllocator().getAllocatedMemory(), 0L);
    } finally {
      source.release();
    }
  }

  @Test
  public void testEncodingFollowsParticipatingBuildBlocksAcrossOutputs() {
    ArrowBlock encoded = dictionaryBlock(1, 3, List.of("encoded"));
    ArrowBlock plain = ArrowBlockConverter.toArrowBlock(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{2, "plain"}), _arrow);
    ArrowHashJoinOperator operator = join(input(List.of(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "first"}),
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{2, "second"})), true), INT_SCHEMA,
        new BlockListMultiStageOperator(_context, encoded, plain), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    operator.enableArrowOutput();
    ArrowBlock first = (ArrowBlock) operator.nextBlock();
    try {
      assertTrue(first.getDataBlock().getRoot().getVector(3).getField().getDictionary() != null);
      ArrowBlock second = (ArrowBlock) operator.nextBlock();
      try {
        assertTrue(second.getDataBlock().getRoot().getVector(3).getField().getDictionary() == null);
        assertTrue(operator.nextBlock().isSuccess());
        assertEquals(first.getDataBlock().getString(0, 3), "encoded");
        assertRows(second.asRowHeap().getRows(), List.<Object[]>of(new Object[]{2, "second", 2, "plain"}));
      } finally {
        second.release();
      }
    } finally {
      first.release();
    }
  }

  @Test
  public void testDefaultHeapBoundaryAndArrowOutputSurviveClose() {
    ArrowHashJoinOperator operator = simpleJoin(PlanNode.NodeHint.EMPTY);
    MseBlock.Data heap = (MseBlock.Data) operator.nextBlock();
    assertTrue(heap.isRowHeap());
    assertEquals(_arrow.getLiveBlockCount(), 1, "Only the build block remains owned");
    operator.close();
    operator.close();
    assertEquals(_arrow.getLiveBlockCount(), 0);
    assertEquals(heap.asRowHeap().getRows().get(0), new Object[]{1, "left", 1, "right"});

    ArrowHashJoinOperator nativeOutput = simpleJoin(PlanNode.NodeHint.EMPTY);
    nativeOutput.enableArrowOutput();
    ArrowBlock result = (ArrowBlock) nativeOutput.nextBlock();
    try {
      assertTrue(nativeOutput.nextBlock().isSuccess());
      assertEquals(_arrow.getLiveBlockCount(), 1, "EOS releases build state but not the consumer's output");
      nativeOutput.close();
      assertEquals(_arrow.getLiveBlockCount(), 1, "Returned output belongs to the consumer");
      assertEquals(result.asRowHeap().getRows().get(0), new Object[]{1, "left", 1, "right"});
    } finally {
      result.release();
    }
  }

  @Test
  public void testNativeJoinsComposeWithoutLeakingArrowToLegacyConsumers() {
    ArrowHashJoinOperator child = simpleJoin(PlanNode.NodeHint.EMPTY);
    DataSchema childSchema = node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY).getDataSchema();
    TrackingInput right =
        new TrackingInput(_context, List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "outer"})));
    ArrowHashJoinOperator parent = join(child, childSchema, right, INT_SCHEMA,
        node(childSchema, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    assertTrue(right._arrowOutput);
    List<Object[]> rows = readRows(parent, false);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{1, "left", 1, "right", 1, "outer"});
    parent.close();
    assertEquals(_arrow.getLiveBlockCount(), 0);
  }

  @Test
  public void testNativeChildUnderUnsupportedLegacyJoin() {
    ArrowHashJoinOperator child = simpleJoin(PlanNode.NodeHint.EMPTY);
    DataSchema childSchema = node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY).getDataSchema();
    TrackingInput right = new TrackingInput(_context, List.of(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "matched"}, new Object[]{2, "unmatched"})));
    MultiStageOperator parent = new DefaultJoinOperatorFactory().createJoinOperator(
        _context, child, plan(childSchema), right, plan(INT_SCHEMA),
        node(childSchema, INT_SCHEMA, JoinRelType.RIGHT, PlanNode.NodeHint.EMPTY));
    _operators.add(parent);
    assertTrue(parent instanceof HashJoinOperator);
    assertFalse(right._arrowOutput);
    assertRows(readRows(parent, false), List.of(
        new Object[]{1, "left", 1, "right", 1, "matched"},
        new Object[]{null, null, null, null, 2, "unmatched"}));
    parent.close();
    assertEquals(_arrow.getLiveBlockCount(), 0);
  }

  @DataProvider
  public Object[][] overflowModes() {
    return new Object[][]{{"THROW", false}, {"THROW", true}, {"BREAK", false}, {"BREAK", true}};
  }

  @Test(dataProvider = "overflowModes")
  public void testBuildLimitsBeforeRetainingRejectedRows(String mode, boolean nativeInput) {
    List<MseBlock> build = List.of(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "a"}, new Object[]{1, "b"}),
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "c"}, new Object[]{1, "d"}, new Object[]{1, "e"}),
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "discarded"}));
    ArrowHashJoinOperator operator = join(
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"})), nativeInput), INT_SCHEMA,
        input(build, nativeInput), INT_SCHEMA, node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, limit(3, mode)));
    MseBlock result = operator.nextBlock();
    if (mode.equals("THROW")) {
      assertResourceLimit(result);
      assertEquals(operator.copyStatMaps().getLong(StatKey.MAX_ROWS_IN_JOIN), 5L);
    } else {
      assertEquals(((MseBlock.Data) result).asRowHeap().getRows().size(), 3);
      assertTrue(operator.nextBlock().isSuccess());
      assertTrue(operator.copyStatMaps().getBoolean(StatKey.MAX_ROWS_IN_JOIN_REACHED));
      assertEquals(operator.copyStatMaps().getLong(StatKey.MAX_ROWS_IN_JOIN), 3L);
    }
    operator.close();
    assertEquals(_arrow.getLiveBlockCount(), 0);
  }

  @Test(dataProvider = "overflowModes")
  public void testOutputLimitsAndDraining(String mode, boolean nativeInput) {
    MultiStageOperator left = input(List.of(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "a"}, new Object[]{1, "b"}, new Object[]{1, "c"}),
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "discarded"})), nativeInput);
    ArrowHashJoinOperator operator = join(left, INT_SCHEMA,
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "x"}, new Object[]{1, "y"})), nativeInput),
        INT_SCHEMA, node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, limit(3, mode)));
    MseBlock result = operator.nextBlock();
    if (mode.equals("THROW")) {
      assertResourceLimit(result);
    } else {
      assertRows(((MseBlock.Data) result).asRowHeap().getRows(),
          List.of(new Object[]{1, "a", 1, "x"}, new Object[]{1, "a", 1, "y"}, new Object[]{1, "b", 1, "x"}));
      assertTrue(left._isEarlyTerminated);
      assertTrue(operator.nextBlock().isSuccess());
      assertTrue(operator.copyStatMaps().getBoolean(StatKey.MAX_ROWS_IN_JOIN_REACHED));
    }
    assertEquals(operator.copyStatMaps().getLong(StatKey.MAX_ROWS_IN_JOIN), 3L);
    operator.close();
    assertEquals(_arrow.getLiveBlockCount(), 0);
  }

  @Test
  public void testOutputLimitAcrossBatchesAndPerProbeBlock() {
    List<Object[]> rows = new ArrayList<>();
    int limit = ArrowJoinOutput.MAX_ROWS_PER_BLOCK + 1;
    for (int i = 0; i < limit; i++) {
      rows.add(new Object[]{1, "left"});
    }
    ArrowHashJoinOperator operator = join(
        input(List.of(new RowHeapDataBlock(rows, INT_SCHEMA)), true), INT_SCHEMA,
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "a"}, new Object[]{1, "b"})), true),
        INT_SCHEMA, node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, limit(limit, "BREAK")));
    operator.enableArrowOutput();
    assertEquals(readRows(operator, true).size(), limit);
    assertTrue(operator.copyStatMaps().getBoolean(StatKey.MAX_ROWS_IN_JOIN_REACHED));

    ArrowHashJoinOperator separateBlocks = join(
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "a"}),
            OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "b"})), true), INT_SCHEMA,
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "x"})), true), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, limit(1, "THROW")));
    assertEquals(readRows(separateBlocks, false).size(), 2);
    assertFalse(separateBlocks.copyStatMaps().getBoolean(StatKey.MAX_ROWS_IN_JOIN_REACHED));
  }

  @Test
  public void testSemiAndAntiOutputLimits() {
    for (JoinRelType type : List.of(JoinRelType.SEMI, JoinRelType.ANTI)) {
      ArrowHashJoinOperator operator = join(
          input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "a"}, new Object[]{1, "b"})), true),
          INT_SCHEMA, input(type == JoinRelType.SEMI
              ? List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "x"})) : List.of(), true),
          INT_SCHEMA, node(INT_SCHEMA, INT_SCHEMA, type, limit(1, "BREAK")));
      assertEquals(readRows(operator, false).size(), 1);
      assertTrue(operator.copyStatMaps().getBoolean(StatKey.MAX_ROWS_IN_JOIN_REACHED));
    }
  }

  @Test
  public void testInputErrorsPreserveBuildOwnershipUntilClose() {
    ErrorMseBlock error = ErrorMseBlock.fromException(new IllegalStateException("upstream failure"));
    TrackingInput left =
        new TrackingInput(_context, List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"})));
    ArrowHashJoinOperator buildError = join(left, INT_SCHEMA, input(List.of(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "right"}), error), true), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    assertSame(buildError.nextBlock(), error);
    assertEquals(left._reads, 0);
    assertEquals(_arrow.getLiveBlockCount(), 0);
    buildError.close();
    assertEquals(_arrow.getLiveBlockCount(), 0);

    ArrowHashJoinOperator probeError = join(input(List.of(error), true), INT_SCHEMA,
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "right"})), true), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    assertSame(probeError.nextBlock(), error);
    assertEquals(_arrow.getLiveBlockCount(), 0);
  }

  @Test
  public void testEarlyTerminationAndCancellation() {
    ArrowHashJoinOperator unstarted = simpleJoin(PlanNode.NodeHint.EMPTY);
    unstarted.earlyTerminate();
    assertTrue(unstarted.nextBlock().isSuccess());
    assertEquals(_arrow.getLiveBlockCount(), 0);

    List<Object[]> duplicates = new ArrayList<>();
    for (int i = 0; i <= ArrowJoinOutput.MAX_ROWS_PER_BLOCK; i++) {
      duplicates.add(new Object[]{1, "right"});
    }
    ArrowHashJoinOperator operator = join(
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"})), true), INT_SCHEMA,
        input(List.of(new RowHeapDataBlock(duplicates, INT_SCHEMA)), true), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    operator.enableArrowOutput();
    ArrowBlock first = (ArrowBlock) operator.nextBlock();
    first.release();
    assertEquals(_arrow.getLiveBlockCount(), 2, "Build and partially consumed probe remain owned");
    operator.cancel(new IllegalStateException("cancelled"));
    assertEquals(_arrow.getLiveBlockCount(), 2, "Cancellation cannot free buffers used by a running worker");
    operator.close();
    operator.close();
    assertEquals(_arrow.getLiveBlockCount(), 0);
  }

  @Test
  public void testDeadlineAfterReceivingProbeReleasesTransientInput() {
    TrackingInput left =
        new TrackingInput(_context, List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"})));
    left._afterRead = () -> doReturn(1L).when(_context).getActiveDeadlineMs();
    ArrowHashJoinOperator operator = join(left, INT_SCHEMA,
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "right"})), true), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
      MseBlock result = operator.nextBlock();
      assertTrue(result.isError());
      assertTrue(((ErrorMseBlock) result).getErrorMessages().containsKey(QueryErrorCode.EXECUTION_TIMEOUT));
    }
    assertEquals(_arrow.getLiveBlockCount(), 1);
  }

  @Test
  public void testBuildAllocationFailureReturnsResourceLimit() {
    try (ArrowQueryContext limited = new ArrowQueryContext(_root.newChildAllocator("limited-build", 0, 1))) {
      doReturn(limited).when(_context).getOrCreateArrowContext();
      TrackingInput left = new TrackingInput(_context, List.of());
      ArrowHashJoinOperator operator = join(left, INT_SCHEMA,
          new BlockListMultiStageOperator(_context, OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "right"})),
          INT_SCHEMA, node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
      assertResourceLimit(operator.nextBlock());
      assertEquals(left._reads, 0);
      assertEquals(limited.getLiveBlockCount(), 0);
      assertEquals(limited.getAllocator().getAllocatedMemory(), 0L);
      operator.close();
    }
  }

  @Test
  public void testOutputAllocationFailureReleasesPartialVectorsAndProbe() {
    ArrowBlock left = ArrowBlockConverter.toArrowBlock(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"}), _arrow);
    ArrowBlock right = ArrowBlockConverter.toArrowBlock(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "right"}), _arrow);
    try (ArrowQueryContext limited = new ArrowQueryContext(_root.newChildAllocator("limited", 0, 64))) {
      doReturn(limited).when(_context).getOrCreateArrowContext();
      ArrowHashJoinOperator operator = join(new BlockListMultiStageOperator(_context, left), INT_SCHEMA,
          new BlockListMultiStageOperator(_context, right), INT_SCHEMA,
          node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
      assertResourceLimit(operator.nextBlock());
      assertEquals(limited.getLiveBlockCount(), 0);
      assertEquals(limited.getAllocator().getAllocatedMemory(), 0L);
      assertEquals(_arrow.getLiveBlockCount(), 1);
      operator.close();
      assertEquals(_arrow.getLiveBlockCount(), 0);
    }
  }

  @DataProvider
  public Object[][] unsupportedJoins() {
    return new Object[][]{
        {JoinRelType.RIGHT, ColumnDataType.INT, List.of(0), false},
        {JoinRelType.FULL, ColumnDataType.INT, List.of(0), false},
        {JoinRelType.INNER, ColumnDataType.INT, List.of(0, 1), false},
        {JoinRelType.INNER, ColumnDataType.INT, List.of(0), true},
        {JoinRelType.INNER, ColumnDataType.STRING, List.of(0), false},
        {JoinRelType.INNER, ColumnDataType.BYTES, List.of(0), false},
        {JoinRelType.INNER, ColumnDataType.BIG_DECIMAL, List.of(0), false},
        {JoinRelType.INNER, ColumnDataType.INT_ARRAY, List.of(0), false},
        {JoinRelType.INNER, ColumnDataType.MAP, List.of(0), false},
        {JoinRelType.INNER, ColumnDataType.OBJECT, List.of(0), false}
    };
  }

  @Test(dataProvider = "unsupportedJoins")
  public void testUnsupportedPlansFallBackBeforeArrowAllocation(JoinRelType type, ColumnDataType keyType,
      List<Integer> keys, boolean residual) {
    DataSchema schema = new DataSchema(new String[]{"key", "payload"},
        new ColumnDataType[]{keyType, ColumnDataType.INT});
    JoinNode node = new JoinNode(-1, resultSchema(schema, schema, type), PlanNode.NodeHint.EMPTY, List.of(), type,
        keys, keys, residual ? List.of(new RexExpression.Literal(ColumnDataType.BOOLEAN, 1)) : List.of(),
        JoinNode.JoinStrategy.HASH);
    TrackingInput left = new TrackingInput(_context, List.of());
    TrackingInput right = new TrackingInput(_context, List.of());
    doThrow(new AssertionError("Fallback must not obtain an Arrow context")).when(_context).getOrCreateArrowContext();
    MultiStageOperator operator = new DefaultJoinOperatorFactory().createJoinOperator(
        _context, left, plan(schema), right, plan(schema), node);
    _operators.add(operator);
    assertTrue(operator instanceof HashJoinOperator);
    assertFalse(left._arrowOutput);
    assertFalse(right._arrowOutput);
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void testUnsupportedPayloadAndDefaultOff() {
    for (ColumnDataType payload : List.of(ColumnDataType.INT_ARRAY, ColumnDataType.BIG_DECIMAL,
        ColumnDataType.MAP, ColumnDataType.OBJECT)) {
      DataSchema schema = new DataSchema(new String[]{"key", "payload"},
          new ColumnDataType[]{ColumnDataType.INT, payload});
      assertLegacyFactory(schema, schema, node(schema, schema, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    }
    DataSchema longSchema =
        new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.LONG});
    assertLegacyFactory(INT_SCHEMA, longSchema,
        node(INT_SCHEMA, longSchema, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    doReturn(false).when(_context).isArrowEnabled();
    assertLegacyFactory(INT_SCHEMA, INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
  }

  @Test
  public void testOutputDictionaryAllocationFailureReleasesTransientBuffers() {
    ArrowBlock left = ArrowBlockConverter.toArrowBlock(
        OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"}), _arrow);
    List<Object[]> rows = new ArrayList<>();
    String value = "x".repeat(4096);
    for (int i = 0; i < 16; i++) {
      rows.add(new Object[]{1, value});
    }
    ArrowBlock right = ArrowBlockConverter.toArrowBlock(new RowHeapDataBlock(rows, INT_SCHEMA), _arrow);
    try (ArrowQueryContext limited = new ArrowQueryContext(_root.newChildAllocator("dictionary-output", 0, 2048))) {
      doReturn(limited).when(_context).getOrCreateArrowContext();
      ArrowHashJoinOperator operator = join(new BlockListMultiStageOperator(_context, left), INT_SCHEMA,
          new BlockListMultiStageOperator(_context, right), INT_SCHEMA,
          node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
      operator.enableArrowOutput();
      MseBlock result = operator.nextBlock();
      assertTrue(result.isError());
      assertTrue(
          ((ErrorMseBlock) result).getErrorMessages().containsKey(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED));
      assertEquals(limited.getLiveBlockCount(), 0);
      assertEquals(limited.getAllocator().getAllocatedMemory(), 0L);
      operator.close();
      assertEquals(_arrow.getLiveBlockCount(), 0);
    }
  }

  @Test
  public void testPlanningDoesNotAcquireArrowContext() {
    doThrow(new AssertionError("Planning must not acquire Arrow resources")).when(_context).getOrCreateArrowContext();
    ArrowHashJoinOperator operator = simpleJoin(PlanNode.NodeHint.EMPTY);
    operator.enableArrowOutput();
    operator.close();
  }

  @Test
  public void testFactorySelectsNativeJoin() {
    MultiStageOperator operator = new DefaultJoinOperatorFactory().createJoinOperator(_context,
        new TrackingInput(_context, List.of()), plan(INT_SCHEMA),
        new TrackingInput(_context, List.of()), plan(INT_SCHEMA),
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, PlanNode.NodeHint.EMPTY));
    _operators.add(operator);
    assertTrue(operator instanceof ArrowHashJoinOperator);
    assertTrue(operator.nextBlock().isSuccess());
    assertEquals(operator.getOperatorType(), MultiStageOperator.Type.HASH_JOIN);
  }

  private void assertLegacyFactory(DataSchema left, DataSchema right, JoinNode node) {
    doThrow(new AssertionError("Fallback must not obtain an Arrow context")).when(_context).getOrCreateArrowContext();
    MultiStageOperator operator = new DefaultJoinOperatorFactory().createJoinOperator(_context,
        new TrackingInput(_context, List.of()), plan(left), new TrackingInput(_context, List.of()), plan(right), node);
    _operators.add(operator);
    assertTrue(operator instanceof HashJoinOperator);
  }

  private ArrowHashJoinOperator simpleJoin(PlanNode.NodeHint hint) {
    return join(input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "left"})), true), INT_SCHEMA,
        input(List.of(OperatorTestUtil.block(INT_SCHEMA, new Object[]{1, "right"})), true), INT_SCHEMA,
        node(INT_SCHEMA, INT_SCHEMA, JoinRelType.INNER, hint));
  }

  private ArrowHashJoinOperator join(MultiStageOperator left, DataSchema leftSchema, MultiStageOperator right,
      DataSchema rightSchema, JoinNode node) {
    ArrowHashJoinOperator operator = new ArrowHashJoinOperator(_context, left, leftSchema, right, rightSchema, node);
    _operators.add(operator);
    return operator;
  }

  private MultiStageOperator input(List<MseBlock> blocks, boolean nativeInput) {
    return nativeInput ? new TrackingInput(_context, blocks) : new BlockListMultiStageOperator(_context, blocks);
  }

  private ArrowBlock dictionaryBlock(int key, int rows, List<String> values) {
    DictionaryEncoding encoding = new DictionaryEncoding(17, false, new ArrowType.Int(32, true));
    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(
        new Field("key", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("value", new FieldType(true, new ArrowType.Int(32, true), encoding), null))), _arrow.getAllocator());
    MapDictionaryProvider dictionaries = new MapDictionaryProvider();
    VarCharVector dictionary = new VarCharVector("values", _arrow.getAllocator());
    dictionaries.put(new Dictionary(dictionary, encoding));
    boolean transferred = false;
    try {
      root.allocateNew();
      for (int row = 0; row < rows; row++) {
        ((IntVector) root.getVector(0)).setSafe(row, key);
        ((IntVector) root.getVector(1)).setSafe(row, row % values.size());
      }
      root.setRowCount(rows);
      dictionary.allocateNew();
      for (int i = 0; i < values.size(); i++) {
        dictionary.setSafe(i, values.get(i).getBytes(StandardCharsets.UTF_8));
      }
      dictionary.setValueCount(values.size());
      ArrowBlock block = _arrow.createBlock(new ArrowDataBlock(root, INT_SCHEMA, dictionaries));
      transferred = true;
      return block;
    } finally {
      if (!transferred) {
        try {
          root.close();
        } finally {
          dictionaries.close();
        }
      }
    }
  }

  private ArrowBlock booleanBlock(DataSchema schema, boolean integerBacked) {
    List<Object[]> rows = List.of(new Object[]{0, 1}, new Object[]{1, 0}, new Object[]{null, null});
    if (!integerBacked) {
      return ArrowBlockConverter.toArrowBlock(new RowHeapDataBlock(rows, schema), _arrow);
    }
    FieldType type = FieldType.nullable(new ArrowType.Int(32, true));
    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(
        new Field("key", type, null), new Field("payload", type, null))), _arrow.getAllocator());
    root.allocateNew();
    for (int col = 0; col < schema.size(); col++) {
      IntVector vector = (IntVector) root.getVector(col);
      for (int row = 0; row < rows.size(); row++) {
        Integer value = (Integer) rows.get(row)[col];
        if (value == null) {
          vector.setNull(row);
        } else {
          vector.set(row, value);
        }
      }
    }
    root.setRowCount(rows.size());
    return _arrow.createBlock(new ArrowDataBlock(root, schema));
  }

  private static PlanNode plan(DataSchema schema) {
    PlanNode plan = mock(PlanNode.class);
    when(plan.getDataSchema()).thenReturn(schema);
    return plan;
  }

  private static JoinNode node(DataSchema left, DataSchema right, JoinRelType type, PlanNode.NodeHint hint) {
    return new JoinNode(-1, resultSchema(left, right, type), hint, List.of(), type, List.of(0), List.of(0), List.of(),
        JoinNode.JoinStrategy.HASH);
  }

  private static DataSchema resultSchema(DataSchema left, DataSchema right, JoinRelType type) {
    if (type == JoinRelType.SEMI || type == JoinRelType.ANTI) {
      return left;
    }
    String[] names = new String[left.size() + right.size()];
    ColumnDataType[] types = new ColumnDataType[names.length];
    for (int i = 0; i < names.length; i++) {
      names[i] = "c" + i;
      types[i] = i < left.size() ? left.getColumnDataType(i) : right.getColumnDataType(i - left.size());
    }
    return new DataSchema(names, types);
  }

  private static PlanNode.NodeHint limit(int rows, String mode) {
    return new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS, Map.of(
        PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, Integer.toString(rows),
        PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, mode)));
  }

  private static List<MseBlock> blocks(DataSchema schema, List<Object[]> rows, int blockSize) {
    List<MseBlock> blocks = new ArrayList<>();
    blocks.add(new RowHeapDataBlock(List.of(), schema));
    for (int start = 0; start < rows.size(); start += blockSize) {
      blocks.add(new RowHeapDataBlock(rows.subList(start, Math.min(start + blockSize, rows.size())), schema));
    }
    blocks.add(new RowHeapDataBlock(List.of(), schema));
    return blocks;
  }

  private static List<Object[]> readRows(MultiStageOperator operator, boolean arrowOutput) {
    List<Object[]> rows = new ArrayList<>();
    while (true) {
      MseBlock block = operator.nextBlock();
      if (block.isEos()) {
        assertTrue(block.isSuccess(), "Unexpected error: " + block);
        return rows;
      }
      MseBlock.Data data = (MseBlock.Data) block;
      try {
        assertEquals(data instanceof ArrowBlock, arrowOutput);
        assertTrue(data.getNumRows() > 0);
        if (operator instanceof ArrowHashJoinOperator) {
          assertTrue(data.getNumRows() <= ArrowJoinOutput.MAX_ROWS_PER_BLOCK);
        }
        rows.addAll(data.asRowHeap().getRows());
      } finally {
        if (data instanceof ArrowBlock) {
          ((ArrowBlock) data).release();
        }
      }
    }
  }

  private static void assertResourceLimit(MseBlock block) {
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages().containsKey(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED));
  }

  private static void assertRows(List<Object[]> actual, List<Object[]> expected) {
    assertEquals(actual.size(), expected.size());
    for (int row = 0; row < actual.size(); row++) {
      assertEquals(actual.get(row), expected.get(row), "Row " + row);
    }
  }

  private static Object[] payload(@Nullable Object key, int row) {
    if (row % 4 == 0) {
      return new Object[]{key, null, null, null, null, null, null, null, null, null};
    }
    return new Object[]{key, row % 3 == 0 ? "π repeated" : "value-" + row,
        new ByteArray(new byte[]{0, (byte) row, -1}), row, (long) row, row + 0.5f, row + 0.25d, row & 1,
        Long.MIN_VALUE + row, "{\"value\":" + row + "}"};
  }

  private static Object[] keys(ColumnDataType type) {
    return switch (type) {
      case INT -> new Object[]{Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE};
      case LONG, TIMESTAMP -> new Object[]{Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE};
      case FLOAT -> new Object[]{-0.0f, 0.0f, -Float.MAX_VALUE, Float.MAX_VALUE, Float.NEGATIVE_INFINITY,
          Float.POSITIVE_INFINITY, Float.NaN, Float.intBitsToFloat(0x7fc00001), Float.intBitsToFloat(0xffc00123)};
      case DOUBLE -> new Object[]{-0.0d, 0.0d, -Double.MAX_VALUE, Double.MAX_VALUE, Double.NEGATIVE_INFINITY,
          Double.POSITIVE_INFINITY, Double.NaN, Double.longBitsToDouble(0x7ff8000000000001L),
          Double.longBitsToDouble(0xfff8000000000123L)};
      case BOOLEAN -> new Object[]{0, 1};
      default -> throw new IllegalArgumentException("Unsupported test type " + type);
    };
  }

  private static final class TrackingInput extends BlockListMultiStageOperator implements ArrowBlockSource {
    private boolean _arrowOutput;
    private int _reads;
    @Nullable
    private Runnable _afterRead;

    private TrackingInput(OpChainExecutionContext context, List<MseBlock> blocks) {
      super(context, blocks);
    }

    @Override
    public void enableArrowOutput() {
      _arrowOutput = true;
    }

    @Override
    protected MseBlock getNextBlock()
        throws Exception {
      _reads++;
      MseBlock block = super.getNextBlock();
      if (block.isData() && _arrowOutput) {
        block = ArrowBlockConverter.toArrowBlock((MseBlock.Data) block, _context.getOrCreateArrowContext());
      }
      if (_afterRead != null) {
        _afterRead.run();
      }
      return block;
    }
  }
}
