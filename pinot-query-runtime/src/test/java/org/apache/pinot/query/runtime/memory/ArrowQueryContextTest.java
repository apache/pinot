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
package org.apache.pinot.query.runtime.memory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlockConverter;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/** Tests deterministic registry cleanup and the process/attempt allocator hierarchy without relying on GC. */
public class ArrowQueryContextTest {
  private static final long LIMIT = 16L * 1024 * 1024;
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.INT});

  @Test
  public void testTerminalReleaseDeregistersBeforeAllocatorClose() {
    RootAllocator root = new RootAllocator(LIMIT);
    try (ArrowBuffers buffers = new ArrowBuffers(true, root, 0, LIMIT);
        ArrowQueryContext context = buffers.newQueryContext("normal")) {
      BufferAllocator allocator = context.getAllocator();
      ArrowBlock block = block(context);
      assertEquals(context.getLiveBlockCount(), 1);
      assertTrue(allocator.getAllocatedMemory() > 0);

      block.retain();
      block.release();
      assertEquals(context.getLiveBlockCount(), 1);
      block.release();
      assertEquals(context.getLiveBlockCount(), 0);
      assertEquals(allocator.getAllocatedMemory(), 0L);
      expectThrows(IllegalStateException.class, block::release);
      expectThrows(IllegalStateException.class, block::retain);

      context.close();
      context.close();
      assertTrue(root.getChildAllocators().isEmpty(), "Zero-byte child handles must also be closed");
      assertEquals(buffers.getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testDroppedReferenceIsSweptWithoutGc() {
    RootAllocator root = new RootAllocator(LIMIT);
    try (ArrowBuffers buffers = new ArrowBuffers(true, root, 0, LIMIT);
        ArrowQueryContext context = buffers.newQueryContext("dropped")) {
      block(context);
      assertEquals(context.getLiveBlockCount(), 1);
      assertTrue(buffers.getAllocatedMemory() > 0);

      context.close();
      assertEquals(context.getLiveBlockCount(), 0);
      assertEquals(buffers.getAllocatedMemory(), 0L);
      assertTrue(root.getChildAllocators().isEmpty());
    }
  }

  @Test
  public void testSweepDropsAllRetainedReferencesAndClosesDataOnce() {
    try (ArrowBuffers buffers = new ArrowBuffers(true, new RootAllocator(LIMIT), 0, LIMIT);
        ArrowQueryContext context = buffers.newQueryContext("retained")) {
      IntVector vector = new IntVector("i", context.getAllocator());
      vector.allocateNew(1);
      vector.set(0, 7);
      vector.setValueCount(1);
      ArrowDataBlock data = spy(new ArrowDataBlock(VectorSchemaRoot.of(vector), SCHEMA));
      ArrowBlock block = context.createBlock(data);
      block.retain();
      block.retain();

      context.close();
      context.close();
      block.forceRelease();
      verify(data, times(1)).close();
      assertEquals(context.getLiveBlockCount(), 0);
      assertEquals(buffers.getAllocatedMemory(), 0L);
      expectThrows(IllegalStateException.class, block::retain);
      expectThrows(IllegalStateException.class, block::release);
      expectThrows(IllegalStateException.class, context::getAllocator);
      expectThrows(IllegalStateException.class, () -> context.createBlock(data));
    }
  }

  @Test
  public void testConcurrentForceReleaseClosesOnce()
      throws Exception {
    try (ArrowBuffers buffers = new ArrowBuffers(true, new RootAllocator(LIMIT), 0, LIMIT);
        ArrowQueryContext context = buffers.newQueryContext("concurrent-sweep");
        ExecutorService executor = Executors.newFixedThreadPool(2)) {
      IntVector vector = new IntVector("i", context.getAllocator());
      vector.allocateNew(1);
      vector.setValueCount(1);
      ArrowDataBlock data = spy(new ArrowDataBlock(VectorSchemaRoot.of(vector), SCHEMA));
      ArrowBlock block = context.createBlock(data);
      block.retain();
      CountDownLatch start = new CountDownLatch(1);
      Future<?> first = executor.submit(() -> {
        start.await();
        block.forceRelease();
        return null;
      });
      Future<?> second = executor.submit(() -> {
        start.await();
        block.forceRelease();
        return null;
      });
      start.countDown();
      first.get(10, TimeUnit.SECONDS);
      second.get(10, TimeUnit.SECONDS);
      verify(data, times(1)).close();
      assertEquals(context.getLiveBlockCount(), 0);
      assertEquals(buffers.getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testDictionaryAndEmptyBlocksAreRegistered() {
    DataSchema schema = new DataSchema(new String[]{"s"}, new ColumnDataType[]{ColumnDataType.STRING});
    try (ArrowBuffers buffers = new ArrowBuffers(true, new RootAllocator(LIMIT), 0, LIMIT);
        ArrowQueryContext context = buffers.newQueryContext("dictionary-empty")) {
      ArrowBlock dictionary = ArrowBlockConverter.toArrowBlock(
          new RowHeapDataBlock(List.<Object[]>of(new Object[]{"same"}, new Object[]{"same"}), schema), context);
      ArrowBlock empty = ArrowBlockConverter.toArrowBlock(new RowHeapDataBlock(List.of(), SCHEMA), context);
      assertEquals(dictionary.getDataBlock().getDictionaryProvider().getDictionaryIds().size(), 1);
      assertEquals(empty.getNumRows(), 0);
      assertEquals(context.getLiveBlockCount(), 2);

      context.close();
      assertEquals(context.getLiveBlockCount(), 0);
      assertEquals(buffers.getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testIncomingArrowReferenceIsNotRetainedOrReparented() {
    try (ArrowBuffers buffers = new ArrowBuffers(true, new RootAllocator(LIMIT), 0, LIMIT);
        ArrowQueryContext source = buffers.newQueryContext("source");
        ArrowQueryContext destination = buffers.newQueryContext("destination")) {
      ArrowBlock block = block(source);
      assertSame(ArrowBlockConverter.toArrowBlock(block, destination), block);
      assertEquals(source.getLiveBlockCount(), 1);
      assertEquals(destination.getLiveBlockCount(), 0);
      block.release();
      assertEquals(buffers.getAllocatedMemory(), 0L, "Conversion must not add a hidden retained reference");
    }
  }

  @Test
  public void testPartialConversionClosesCurrentVectorAndDictionaries() {
    DataSchema schema =
        new DataSchema(new String[]{"s", "i"}, new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    DataBlock data = mock(DataBlock.class);
    when(data.getDataBlockType()).thenReturn(DataBlock.Type.ROW);
    when(data.getDataSchema()).thenReturn(schema);
    when(data.getNumberOfRows()).thenReturn(2);
    when(data.getString(0, 0)).thenReturn("same");
    when(data.getString(1, 0)).thenReturn("same");
    IllegalStateException failure = new IllegalStateException("injected column read failure");
    when(data.getInt(0, 1)).thenThrow(failure);

    try (ArrowBuffers buffers = new ArrowBuffers(true, new RootAllocator(LIMIT), 0, LIMIT);
        ArrowQueryContext context = buffers.newQueryContext("failed-conversion")) {
      assertSame(expectThrows(IllegalStateException.class,
          () -> ArrowBlockConverter.toArrowBlock(new SerializedDataBlock(data), context)), failure);
      assertEquals(context.getLiveBlockCount(), 0);
      assertEquals(buffers.getAllocatedMemory(), 0L);

      assertSame(expectThrows(IllegalStateException.class,
          () -> ArrowBlockConverter.fromDataBlock(data, schema, context.getAllocator())), failure);
      assertEquals(buffers.getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testProcessCloseWaitsForAllManagedContexts() {
    RootAllocator root = spy(new RootAllocator(LIMIT));
    try (ArrowBuffers buffers = new ArrowBuffers(true, root, 0, LIMIT);
        ArrowQueryContext first = buffers.newQueryContext("same-attempt");
        ArrowQueryContext second = buffers.newQueryContext("same-attempt")) {
      assertNotEquals(first.getAllocator().getName(), second.getAllocator().getName());
      ArrowBlock block = block(first);
      buffers.close();
      buffers.close();
      verify(root, never()).close();
      expectThrows(IllegalStateException.class, () -> buffers.newQueryContext("late"));
      expectThrows(IllegalStateException.class, () -> buffers.newQueryAllocator("late"));
      assertEquals(block.getDataBlock().getInt(0, 0), 7);

      first.close();
      assertEquals(buffers.getAllocatedMemory(), 0L);
      verify(root, never()).close();
      second.close();
      verify(root, times(1)).close();
      buffers.close();
      verify(root, times(1)).close();
    }
  }

  @Test
  public void testDisabledModeRejectsAllocatorCreation() {
    try (ArrowBuffers buffers = ArrowBuffers.create(new PinotConfiguration())) {
      assertFalse(buffers.isEnabled());
      expectThrows(IllegalStateException.class, () -> buffers.newQueryContext("disabled"));
      expectThrows(IllegalStateException.class, () -> buffers.newQueryAllocator("disabled"));
      expectThrows(IllegalStateException.class, () -> buffers.newAllocator("disabled", 0, LIMIT));
      buffers.close();
      buffers.close();
    }
  }

  private static ArrowBlock block(ArrowQueryContext context) {
    return ArrowBlockConverter.toArrowBlock(new RowHeapDataBlock(List.<Object[]>of(new Object[]{7}), SCHEMA), context);
  }
}
