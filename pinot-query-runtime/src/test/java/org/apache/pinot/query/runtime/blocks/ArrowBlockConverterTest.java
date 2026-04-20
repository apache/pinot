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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/**
 * Tests for {@link ArrowBlockConverter} — verifies that legacy {@link RowHeapDataBlock} data survives
 * a round-trip through Arrow columnar format and back.
 *
 * <p>{@link #testConverterRoundTripAllTypes} is the comprehensive test: every scalar type the
 * converter supports (INT, LONG, FLOAT, DOUBLE, STRING, BYTES, BOOLEAN, TIMESTAMP) round-trips
 * with both a non-null and a null row, and the Pinot {@link DataSchema} is preserved exactly — in
 * particular TIMESTAMP is not collapsed to LONG (regression guard for the
 * {@code new ArrowDataBlock(root, schema)} constructor change).
 *
 * <p>BIG_DECIMAL is intentionally <em>not</em> supported by the converter in this PR — the
 * underlying {@code extractStringColumn} utility only handles {@code DataType.STRING}. BIG_DECIMAL
 * therefore throws via {@link #testUnsupportedColumnTypesThrowWithTypeInMessage}.
 */
public class ArrowBlockConverterTest {
  private ArrowBuffers _arrowBuffers;
  private BufferAllocator _allocator;

  @BeforeMethod
  public void setUp() {
    _arrowBuffers = new ArrowBuffers(true, new RootAllocator(Long.MAX_VALUE), 0, Long.MAX_VALUE);
    _allocator = _arrowBuffers.newQueryAllocator("test");
  }

  @AfterMethod
  public void tearDown() {
    _allocator.close();
    _arrowBuffers.close();
  }

  @Test
  public void testConverterRoundTripAllTypes() {
    DataSchema schema = new DataSchema(
        new String[]{"i", "l", "f", "d", "s", "b", "bool", "ts"},
        new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
            ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP});
    // row 0 valued, row 1 all-null, row 2 valued
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(
        new Object[]{7, 70L, 0.7f, 0.7, "Alice", new ByteArray(new byte[]{1, 2}), 1, 1_700_000_000_000L},
        new Object[]{null, null, null, null, null, null, null, null},
        new Object[]{Integer.MAX_VALUE, Long.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE, "Bob",
            new ByteArray(new byte[]{3}), 0, 1_800_000_000_000L}),
        schema);

    try (ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator)) {
      assertEquals(arrowBlock.getNumRows(), 3);

      // Every Pinot ColumnDataType must round-trip unchanged. Guards the lossy Arrow→Pinot mapping
      // (TIMESTAMP and LONG both live as Int64; BIG_DECIMAL and STRING both live as Utf8).
      DataSchema out = arrowBlock.getDataSchema();
      for (int col = 0; col < schema.size(); col++) {
        assertEquals(out.getColumnDataType(col), schema.getColumnDataType(col),
            "col '" + schema.getColumnName(col) + "' type must round-trip");
      }

      DataBlock db = arrowBlock.getDataBlock();
      // Non-null rows read back correctly for every type.
      assertEquals(db.getInt(0, 0), 7);
      assertEquals(db.getInt(2, 0), Integer.MAX_VALUE);
      assertEquals(db.getLong(0, 1), 70L);
      assertEquals(db.getLong(2, 1), Long.MAX_VALUE);
      assertEquals(db.getFloat(0, 2), 0.7f, 0.0001f);
      assertEquals(db.getFloat(2, 2), Float.MAX_VALUE, 0.0001f);
      assertEquals(db.getDouble(0, 3), 0.7, 0.0001);
      assertEquals(db.getDouble(2, 3), Double.MAX_VALUE, 0.0001);
      assertEquals(db.getString(0, 4), "Alice");
      assertEquals(db.getString(2, 4), "Bob");
      assertEquals(db.getBytes(0, 5), new ByteArray(new byte[]{1, 2}));
      assertEquals(db.getBytes(2, 5), new ByteArray(new byte[]{3}));
      assertEquals(db.getInt(0, 6), 1);
      assertEquals(db.getInt(2, 6), 0);
      assertEquals(db.getLong(0, 7), 1_700_000_000_000L);
      assertEquals(db.getLong(2, 7), 1_800_000_000_000L);

      // The null row is reflected in every column's null bitmap; non-null rows are not.
      for (int col = 0; col < schema.size(); col++) {
        RoaringBitmap nullIds = db.getNullRowIds(col);
        assertNotNull(nullIds, "col " + col + " must have a null bitmap");
        assertFalse(nullIds.contains(0), "col " + col + " row 0 must be non-null");
        assertTrue(nullIds.contains(1), "col " + col + " row 1 must be null");
        assertFalse(nullIds.contains(2), "col " + col + " row 2 must be non-null");
      }
    }
  }

  @Test
  public void testRoundTripViaAsRowHeap() {
    // End-to-end: RowHeap -> ArrowBlockConverter -> ArrowBlock.asRowHeap() -> rows.
    DataSchema schema = new DataSchema(new String[]{"id", "name"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    RowHeapDataBlock source = new RowHeapDataBlock(
        rows(new Object[]{1, "Alice"}, new Object[]{2, "Bob"}), schema);

    try (ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(source, _allocator)) {
      List<Object[]> out = arrowBlock.asRowHeap().getRows();
      assertEquals(out.size(), 2);
      assertEquals(out.get(0)[0], 1);
      assertEquals(out.get(0)[1], "Alice");
      assertEquals(out.get(1)[0], 2);
      assertEquals(out.get(1)[1], "Bob");
    }
  }

  @Test
  public void testConverterHandlesSerializedDataBlockInput() {
    // Feed a SerializedDataBlock directly so the converter's serialized-columnar input path is
    // exercised without going through the RowHeap → asSerialized bridge internally.
    DataSchema schema = new DataSchema(new String[]{"id", "name"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(
        rows(new Object[]{1, "Alice"}, new Object[]{2, null}, new Object[]{3, "Carol"}), schema);
    SerializedDataBlock serialized = rowBlock.asSerialized();

    try (ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(serialized, _allocator)) {
      assertEquals(arrowBlock.getNumRows(), 3);
      DataBlock db = arrowBlock.getDataBlock();
      assertEquals(db.getInt(0, 0), 1);
      assertEquals(db.getInt(2, 0), 3);
      assertEquals(db.getString(0, 1), "Alice");
      assertNull(db.getString(1, 1));
      assertEquals(db.getString(2, 1), "Carol");
      RoaringBitmap nullIds = db.getNullRowIds(1);
      assertNotNull(nullIds);
      assertTrue(nullIds.contains(1));
    }
  }

  @Test
  public void testEmptyBlock() {
    DataSchema schema = new DataSchema(new String[]{"id"},
        new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(Collections.emptyList(), schema);

    try (ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator)) {
      assertEquals(arrowBlock.getNumRows(), 0);
    }
  }

  @Test
  public void testAlreadyArrowBlockPassthrough() {
    DataSchema schema = new DataSchema(new String[]{"id"},
        new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{1}), schema);
    try (ArrowBlock original = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator)) {
      ArrowBlock same = ArrowBlockConverter.toArrowBlock(original, _allocator);
      assertSame(same, original, "converting an ArrowBlock must return the same instance");
    }
  }

  @Test
  public void testGetNullRowIdsReturnsNullWhenNoNulls() {
    DataSchema schema = new DataSchema(new String[]{"id"},
        new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{1}, new Object[]{2}), schema);

    try (ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator)) {
      assertNull(arrowBlock.getDataBlock().getNullRowIds(0), "non-null column must return null bitmap");
    }
  }

  @Test
  public void testUnsupportedColumnTypesThrowWithTypeInMessage() {
    // Each unsupported column type must throw UnsupportedOperationException whose message names the
    // offending type — callers rely on the message to identify which column failed.
    assertUnsupported(ColumnDataType.INT_ARRAY, new int[]{1, 2, 3}, "INT_ARRAY");
    assertUnsupported(ColumnDataType.MAP, Collections.emptyMap(), "MAP");
    assertUnsupported(ColumnDataType.BIG_DECIMAL, new BigDecimal("1.0"), "BIG_DECIMAL");
  }

  @Test
  public void testIsArrowAndIsSerializedPredicates() {
    DataSchema schema = new DataSchema(new String[]{"id"},
        new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{1}), schema);
    try (ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator)) {
      assertTrue(arrowBlock.isArrow(), "isArrow() must be true");
      assertFalse(arrowBlock.isSerialized(), "isSerialized() must be false");
      assertFalse(arrowBlock.isRowHeap(), "isRowHeap() must be false");
    }
  }

  // ----- helpers -----

  private void assertUnsupported(ColumnDataType type, Object value, String typeNameInMessage) {
    DataSchema schema = new DataSchema(new String[]{"v"}, new ColumnDataType[]{type});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{value}), schema);
    UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
        () -> ArrowBlockConverter.toArrowBlock(rowBlock, _allocator));
    assertTrue(e.getMessage().contains(typeNameInMessage),
        "message should name type " + typeNameInMessage + ": " + e.getMessage());
  }

  private static List<Object[]> rows(Object[]... rows) {
    List<Object[]> list = new ArrayList<>(rows.length);
    Collections.addAll(list, rows);
    return list;
  }
}
