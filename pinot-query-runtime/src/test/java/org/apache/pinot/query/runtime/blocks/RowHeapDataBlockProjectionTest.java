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
package org.apache.pinot.query.runtime.blocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.core.query.aggregation.function.funnel.window.FunnelMaxStepAggregationFunction;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class RowHeapDataBlockProjectionTest {
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"i", "s", "l"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.LONG});
  private static final DataSchema PROJECTED = new DataSchema(new String[]{"l", "i"},
      new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.INT});

  private static RowHeapDataBlock block() {
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(new Object[]{i, "s" + i, i * 10L});
    }
    return new RowHeapDataBlock(rows, SCHEMA);
  }

  @Test
  public void viewHoldsTheSelectedColumns() {
    RowHeapDataBlock view = block().project(new int[]{2, 0}, PROJECTED);
    assertEquals(view.getNumRows(), 5);
    assertSame(view.getDataSchema(), PROJECTED);
    List<Object[]> rows = view.getRows();
    for (int i = 0; i < 5; i++) {
      assertEquals(rows.get(i), new Object[]{i * 10L, i});
    }
    // The rows are created once.
    assertSame(view.getRows(), rows);
  }

  /// The view serializes from the source rows. The result must be the same as serializing the projected rows.
  @Test
  public void viewSerializesLikeTheProjectedRows() {
    RowHeapDataBlock view = block().project(new int[]{2, 0}, PROJECTED);
    DataBlock serialized = view.asSerialized().getDataBlock();
    DataBlock expected = new RowHeapDataBlock(view.getRows(), PROJECTED).asSerialized().getDataBlock();
    assertRows(DataBlockExtractUtils.extractRows(serialized), DataBlockExtractUtils.extractRows(expected));
    assertEquals(serialized.getDataSchema(), PROJECTED);
  }

  @Test
  public void projectionOfAViewReadsTheOriginalRows() {
    RowHeapDataBlock view = block().project(new int[]{2, 0}, PROJECTED);
    DataSchema onlyI = new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock viewOfView = view.project(new int[]{1}, onlyI);
    List<Object[]> rows = viewOfView.getRows();
    for (int i = 0; i < 5; i++) {
      assertEquals(rows.get(i), new Object[]{i});
    }
    assertRows(DataBlockExtractUtils.extractRows(viewOfView.asSerialized().getDataBlock()), rows);
  }

  @Test
  public void invalidProjectionsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> block().project(new int[]{3, 0}, PROJECTED));
    assertThrows(IllegalArgumentException.class, () -> block().project(new int[]{0}, PROJECTED));
  }

  /// The aggregation functions of a view follow its columns: the function of an OBJECT column moves with the column,
  /// and `copyObjectColumns` on the view copies that column through it.
  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void viewRemapsTheAggregationFunctions() {
    DataSchema schema = new DataSchema(new String[]{"key", "other", "funnel"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.OBJECT});
    AggregationFunction aggFunction = new FunnelMaxStepAggregationFunction(List.of(
        ExpressionContext.forIdentifier("ts"), ExpressionContext.forLiteral(Literal.longValue(1000L)),
        ExpressionContext.forLiteral(Literal.intValue(2)), ExpressionContext.forIdentifier("step0"),
        ExpressionContext.forIdentifier("step1")), false);
    PriorityQueue<FunnelStepEvent> stepEvents = new PriorityQueue<>(List.of(new FunnelStepEvent(1000L, 0)));
    RowHeapDataBlock block = new RowHeapDataBlock(List.<Object[]>of(new Object[]{"user0", 7, stepEvents}), schema,
        new AggregationFunction[]{aggFunction});
    DataSchema projected = new DataSchema(new String[]{"funnel", "key"},
        new ColumnDataType[]{ColumnDataType.OBJECT, ColumnDataType.STRING});

    RowHeapDataBlock view = block.project(new int[]{2, 0}, projected);

    // One function per column of the view: the OBJECT column keeps its function, the key column has none.
    AggregationFunction[] viewAggFunctions = view.getAggFunctions();
    assertEquals(viewAggFunctions.length, 2);
    assertSame(viewAggFunctions[0], aggFunction);
    assertNull(viewAggFunctions[1]);
    RowHeapDataBlock copy = view.copyObjectColumns();
    Object[] copiedRow = copy.getRows().get(0);
    assertNotSame(copiedRow[0], stepEvents);
    assertEquals(((PriorityQueue<FunnelStepEvent>) copiedRow[0]).peek(), new FunnelStepEvent(1000L, 0));
    assertEquals(copiedRow[1], "user0");
  }

  /// `BroadcastExchange` shares blocks by reference, so several threads can read the same view at the same time: local
  /// receivers that read the rows and a sender that serializes the view for a remote mailbox.
  @Test
  public void viewCanBeReadByThreadsAtTheSameTime()
      throws Exception {
    List<Object[]> sourceRows = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      sourceRows.add(new Object[]{i, "s" + i, i * 10L});
    }
    List<Object[]> expectedRows = new ArrayList<>();
    for (Object[] row : sourceRows) {
      expectedRows.add(new Object[]{row[2], row[0]});
    }
    int numThreads = 4;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      for (int iteration = 0; iteration < 200; iteration++) {
        RowHeapDataBlock view = new RowHeapDataBlock(sourceRows, SCHEMA).project(new int[]{2, 0}, PROJECTED);
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        List<Future<List<Object[]>>> results = new ArrayList<>();
        for (int thread = 0; thread < numThreads; thread++) {
          boolean serialize = thread % 2 == 0;
          results.add(executor.submit(() -> {
            barrier.await();
            assertEquals(view.getNumRows(), 2000);
            return serialize ? DataBlockExtractUtils.extractRows(view.asSerialized().getDataBlock()) : view.getRows();
          }));
        }
        for (Future<List<Object[]>> result : results) {
          assertRows(result.get(10, TimeUnit.SECONDS), expectedRows);
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static void assertRows(List<Object[]> actual, List<Object[]> expected) {
    assertTrue(Arrays.deepEquals(actual.toArray(), expected.toArray()),
        Arrays.deepToString(actual.toArray()) + " != " + Arrays.deepToString(expected.toArray()));
  }
}
