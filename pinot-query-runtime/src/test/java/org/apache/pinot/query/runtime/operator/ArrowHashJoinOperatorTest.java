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

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.spi.memory.ArrowBuffers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * End-to-end tests for the Arrow execution path in {@link HashJoinOperator}.
 *
 * <p>These tests verify that when {@link ArrowBuffers#isEnabled()} is true:
 * <ol>
 *   <li>The right side is built into an {@link org.apache.pinot.query.runtime.operator.join.ArrowLookupTable}</li>
 *   <li>The probe phase produces {@link ArrowBlock} output (not {@link RowHeapDataBlock})</li>
 *   <li>The join results are numerically correct — same rows as the row-oriented path</li>
 * </ol>
 *
 * <p>Reading guide: start here to understand the Arrow execution path end-to-end.
 * Then follow the call chain:
 * <pre>
 *   HashJoinOperator.buildRightTable()   — builds ArrowLookupTable from right ArrowBlocks
 *   HashJoinOperator.buildJoinedDataBlock() — creates ArrowJoinProbe per left block
 *   ArrowJoinProbe.buildOnlyMatches()    — column-level hash probe, returns ArrowDataBlock
 * </pre>
 */
public class ArrowHashJoinOperatorTest {

  // Schema used for both left and right sides: (user_id INT, name STRING)
  private static final DataSchema CHILD_SCHEMA = new DataSchema(
      new String[]{"user_id", "name"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});

  // Result schema for a self-join: left columns + right columns
  private static final DataSchema RESULT_SCHEMA = new DataSchema(
      new String[]{"l_user_id", "l_name", "r_user_id", "r_name"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});

  // Each test gets a fresh allocator — no shared singleton state
  private ArrowBuffers _arrowBuffers;
  private BufferAllocator _allocator;

  @BeforeMethod
  public void setUp() {
    _arrowBuffers = ArrowBuffers.createForTest();
    _allocator = _arrowBuffers.newQueryAllocator("test");
  }

  @AfterMethod
  public void tearDown() {
    _allocator.close();
    _arrowBuffers.close();
  }

  // ----- inner join -----

  /**
   * Basic INNER JOIN on an integer key.
   *
   * <p>Left:  (1,"Alice"), (2,"Bob"), (3,"Charlie")
   * <p>Right: (2,"X"),     (3,"Y"),   (4,"Z")
   * <p>Expected: rows where user_id matches → (2,Bob,2,X) and (3,Charlie,3,Y)
   */
  @Test
  public void testInnerJoinOnIntKey() {
    MultiStageOperator leftInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA,
            new Object[]{1, "Alice"}, new Object[]{2, "Bob"}, new Object[]{3, "Charlie"}))
        .buildWithEos();

    MultiStageOperator rightInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{2, "X"}, new Object[]{3, "Y"}, new Object[]{4, "Z"}))
        .buildWithEos();

    HashJoinOperator op = buildOperator(leftInput, rightInput, JoinRelType.INNER, List.of(0), List.of(0));

    // The first nextBlock() call triggers build (right side) + one probe (left side)
    MseBlock result = op.nextBlock();

    // Arrow path produces ArrowBlock, not RowHeapDataBlock
    assertTrue(result instanceof ArrowBlock, "Arrow path should produce ArrowBlock output");
    assertFalse(result.isEos());

    List<Object[]> rows = ((MseBlock.Data) result).asRowHeap().getRows();
    assertEquals(rows.size(), 2, "Expected 2 matching rows");

    // Rows come out in left-scan order
    assertRow(rows.get(0), 2, "Bob", 2, "X");
    assertRow(rows.get(1), 3, "Charlie", 3, "Y");

    // Next call should return EOS
    assertTrue(op.nextBlock().isEos());
  }

  /**
   * INNER JOIN with multiple matching right-side rows for one left row (duplicate keys).
   *
   * <p>Left:  (1,"Alice")
   * <p>Right: (1,"X"), (1,"Y")   ← user_id=1 appears twice on the right
   * <p>Expected: two result rows — one for each right match
   */
  @Test
  public void testInnerJoinWithDuplicateRightKeys() {
    MultiStageOperator leftInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{1, "Alice"}))
        .buildWithEos();

    MultiStageOperator rightInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{1, "X"}, new Object[]{1, "Y"}))
        .buildWithEos();

    HashJoinOperator op = buildOperator(leftInput, rightInput, JoinRelType.INNER, List.of(0), List.of(0));

    MseBlock result = op.nextBlock();
    assertTrue(result instanceof ArrowBlock);

    List<Object[]> rows = ((MseBlock.Data) result).asRowHeap().getRows();
    assertEquals(rows.size(), 2);
    assertRow(rows.get(0), 1, "Alice", 1, "X");
    assertRow(rows.get(1), 1, "Alice", 1, "Y");
    assertTrue(op.nextBlock().isEos());
  }

  /**
   * INNER JOIN where NO rows match — result should be empty then EOS.
   */
  @Test
  public void testInnerJoinNoMatches() {
    MultiStageOperator leftInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{10, "Alice"}))
        .buildWithEos();

    MultiStageOperator rightInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{99, "X"}))
        .buildWithEos();

    HashJoinOperator op = buildOperator(leftInput, rightInput, JoinRelType.INNER, List.of(0), List.of(0));

    // No matches → first nextBlock skips the empty result and returns EOS
    MseBlock result = op.nextBlock();
    assertTrue(result.isEos(), "No-match inner join should return EOS directly");
  }

  // ----- semi / anti join -----

  /**
   * SEMI JOIN: return left rows that HAVE a match on the right.
   *
   * <p>Left:  (1,"Alice"), (2,"Bob"), (3,"Charlie")
   * <p>Right: (2,"X"), (4,"Z")
   * <p>Expected: only (2,"Bob") — the left row whose user_id appears on the right
   */
  @Test
  public void testSemiJoin() {
    MultiStageOperator leftInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA,
            new Object[]{1, "Alice"}, new Object[]{2, "Bob"}, new Object[]{3, "Charlie"}))
        .buildWithEos();

    MultiStageOperator rightInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{2, "X"}, new Object[]{4, "Z"}))
        .buildWithEos();

    HashJoinOperator op = buildOperator(leftInput, rightInput, JoinRelType.SEMI, List.of(0), List.of(0));

    MseBlock result = op.nextBlock();
    assertTrue(result instanceof ArrowBlock);

    List<Object[]> rows = ((MseBlock.Data) result).asRowHeap().getRows();
    assertEquals(rows.size(), 1);
    // Semi join returns only left-side columns
    assertEquals(rows.get(0)[0], 2);
    assertEquals(rows.get(0)[1], "Bob");
    assertTrue(op.nextBlock().isEos());
  }

  /**
   * ANTI JOIN: return left rows that have NO match on the right.
   *
   * <p>Left:  (1,"Alice"), (2,"Bob")
   * <p>Right: (2,"X")
   * <p>Expected: only (1,"Alice") — the left row whose user_id does NOT appear on the right
   */
  @Test
  public void testAntiJoin() {
    MultiStageOperator leftInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{1, "Alice"}, new Object[]{2, "Bob"}))
        .buildWithEos();

    MultiStageOperator rightInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{2, "X"}))
        .buildWithEos();

    HashJoinOperator op = buildOperator(leftInput, rightInput, JoinRelType.ANTI, List.of(0), List.of(0));

    MseBlock result = op.nextBlock();
    assertTrue(result instanceof ArrowBlock);

    List<Object[]> rows = ((MseBlock.Data) result).asRowHeap().getRows();
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(0)[1], "Alice");
    assertTrue(op.nextBlock().isEos());
  }

  // ----- multi-block inputs -----

  /**
   * INNER JOIN where left side is split across multiple blocks (realistic — each gRPC message is one block).
   *
   * <p>Left block 1: (1,"Alice"), (2,"Bob")
   * <p>Left block 2: (3,"Charlie"), (4,"Dave")
   * <p>Right:        (2,"X"), (4,"Y")
   * <p>Expected: (2,Bob,2,X) from block 1 and (4,Dave,4,Y) from block 2
   */
  @Test
  public void testInnerJoinMultipleLeftBlocks() {
    MultiStageOperator leftInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{1, "Alice"}, new Object[]{2, "Bob"}))
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{3, "Charlie"}, new Object[]{4, "Dave"}))
        .buildWithEos();

    MultiStageOperator rightInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{2, "X"}, new Object[]{4, "Y"}))
        .buildWithEos();

    HashJoinOperator op = buildOperator(leftInput, rightInput, JoinRelType.INNER, List.of(0), List.of(0));

    // One result block per left block
    MseBlock block1 = op.nextBlock();
    assertTrue(block1 instanceof ArrowBlock);
    List<Object[]> rows1 = ((MseBlock.Data) block1).asRowHeap().getRows();
    assertEquals(rows1.size(), 1);
    assertRow(rows1.get(0), 2, "Bob", 2, "X");

    MseBlock block2 = op.nextBlock();
    assertTrue(block2 instanceof ArrowBlock);
    List<Object[]> rows2 = ((MseBlock.Data) block2).asRowHeap().getRows();
    assertEquals(rows2.size(), 1);
    assertRow(rows2.get(0), 4, "Dave", 4, "Y");

    assertTrue(op.nextBlock().isEos());
  }

  /**
   * INNER JOIN where right side arrives in multiple blocks (right side is fully consumed before probe begins).
   */
  @Test
  public void testInnerJoinMultipleRightBlocks() {
    MultiStageOperator leftInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{2, "Bob"}, new Object[]{3, "Charlie"}))
        .buildWithEos();

    // Right side split across two blocks — all merged into one ArrowLookupTable before probe
    MultiStageOperator rightInput = new BlockListMultiStageOperator.Builder(CHILD_SCHEMA)
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{2, "X"}))
        .addBlock(arrowBlock(CHILD_SCHEMA, new Object[]{3, "Y"}))
        .buildWithEos();

    HashJoinOperator op = buildOperator(leftInput, rightInput, JoinRelType.INNER, List.of(0), List.of(0));

    MseBlock result = op.nextBlock();
    assertTrue(result instanceof ArrowBlock);

    List<Object[]> rows = ((MseBlock.Data) result).asRowHeap().getRows();
    assertEquals(rows.size(), 2);
    assertRow(rows.get(0), 2, "Bob", 2, "X");
    assertRow(rows.get(1), 3, "Charlie", 3, "Y");
    assertTrue(op.nextBlock().isEos());
  }

  // ----- ArrowBlockConverter -----

  /**
   * Verifies that {@link ArrowBlockConverter} correctly converts a legacy {@link RowHeapDataBlock} to an
   * {@link ArrowBlock} and that values are preserved.
   *
   * <p>This is the conversion that {@link MailboxReceiveOperator} performs on every received block.
   */
  @Test
  public void testArrowBlockConverterPreservesValues() {
    RowHeapDataBlock rowBlock = OperatorTestUtil.block(CHILD_SCHEMA,
        new Object[]{1, "Alice"},
        new Object[]{2, "Bob"});

    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);

    assertEquals(arrowBlock.getNumRows(), 2);

    List<Object[]> rows = arrowBlock.asRowHeap().getRows();
    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(0)[1], "Alice");
    assertEquals(rows.get(1)[0], 2);
    assertEquals(rows.get(1)[1], "Bob");

    arrowBlock.release();
  }

  // ----- helpers -----

  /**
   * Creates an {@link ArrowBlock} from literal row data via {@link ArrowBlockConverter}.
   * This simulates what {@link MailboxReceiveOperator} does when Arrow is enabled.
   */
  private ArrowBlock arrowBlock(DataSchema schema, Object[]... rows) {
    RowHeapDataBlock rowBlock = OperatorTestUtil.block(schema, rows);
    return ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
  }

  private HashJoinOperator buildOperator(MultiStageOperator leftInput, MultiStageOperator rightInput,
      JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys) {
    // Pass the allocator via a context that has Arrow enabled
    OpChainExecutionContext context = OperatorTestUtil.getTracingContextWithArrow(_allocator);
    return new HashJoinOperator(context, leftInput, CHILD_SCHEMA, rightInput,
        new JoinNode(-1, RESULT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), joinType, leftKeys, rightKeys,
            List.of(), JoinNode.JoinStrategy.HASH));
  }

  private static void assertRow(Object[] row, int userId1, String name1, int userId2, String name2) {
    assertEquals(row[0], userId1, "left user_id mismatch");
    assertEquals(row[1], name1, "left name mismatch");
    assertEquals(row[2], userId2, "right user_id mismatch");
    assertEquals(row[3], name2, "right name mismatch");
  }
}
