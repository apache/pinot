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
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.memory.ArrowBuffers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link ArrowBlockConverter} — verifies that legacy {@link RowHeapDataBlock} data survives
 * a round-trip through Arrow columnar format and back.
 */
public class ArrowBlockConverterTest {
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

  @Test
  public void testIntAndStringColumns() {
    DataSchema schema = new DataSchema(new String[]{"id", "name"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(
        rows(new Object[]{1, "Alice"}, new Object[]{2, "Bob"}), schema);

    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    assertEquals(arrowBlock.getNumRows(), 2);

    List<Object[]> rows = arrowBlock.asRowHeap().getRows();
    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(0)[1], "Alice");
    assertEquals(rows.get(1)[0], 2);
    assertEquals(rows.get(1)[1], "Bob");
  }

  @Test
  public void testAllNumericTypes() {
    DataSchema schema = new DataSchema(
        new String[]{"i", "l", "f", "d"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(
        rows(new Object[]{42, 123456789L, 3.14f, 2.718}), schema);

    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    List<Object[]> rows = arrowBlock.asRowHeap().getRows();

    assertEquals(rows.get(0)[0], 42);
    assertEquals(rows.get(0)[1], 123456789L);
    assertEquals((float) rows.get(0)[2], 3.14f, 0.001f);
    assertEquals((double) rows.get(0)[3], 2.718, 0.001);
  }

  @Test
  public void testEmptyBlock() {
    DataSchema schema = new DataSchema(new String[]{"id"},
        new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(Collections.emptyList(), schema);

    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    assertEquals(arrowBlock.getNumRows(), 0);
    arrowBlock.release();
  }

  @Test
  public void testAlreadyArrowBlockPassthrough() {
    DataSchema schema = new DataSchema(new String[]{"id"},
        new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{1}), schema);
    ArrowBlock original = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);

    // Converting an ArrowBlock should return the same instance
    ArrowBlock same = ArrowBlockConverter.toArrowBlock(original, _allocator);
    assertTrue(same == original, "Should return same ArrowBlock instance");
    same.release();
  }

  private static List<Object[]> rows(Object[]... rows) {
    List<Object[]> list = new ArrayList<>(rows.length);
    Collections.addAll(list, rows);
    return list;
  }
}
