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
 * a round-trip through Arrow columnar format and back, plus the {@link ArrowBlock} reference-counting
 * lifetime contract (retain/release, double-free and use-after-free guards).
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
 *
 * <p>Each block is acquired at refcount 1 and {@link ArrowBlock#release() released} in a {@code finally}
 * so its off-heap buffers are freed before {@link #tearDown()} closes the allocator — Arrow's
 * {@code BufferAllocator.close()} throws if any buffer is still outstanding.
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

    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    try {
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
    } finally {
      arrowBlock.release();
    }
  }

  @Test
  public void testRoundTripViaAsRowHeap() {
    // End-to-end: RowHeap -> ArrowBlockConverter -> ArrowBlock.asRowHeap() -> rows, covering every
    // scalar type the converter supports. This is a regression guard for the ArrowBlock.asRowHeap() implementation,
    // which must produce the right Pinot internal types (e.g. BOOLEAN as Integer, BYTES as ByteArray)
    DataSchema schema = new DataSchema(
        new String[]{"i", "l", "f", "d", "s", "b", "bool", "ts"},
        new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
            ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP});
    RowHeapDataBlock source = new RowHeapDataBlock(rows(
        new Object[]{7, 70L, 0.7f, 0.7, "Alice", new ByteArray(new byte[]{1, 2}), 1, 1_700_000_000_000L},
        new Object[]{null, null, null, null, null, null, null, null},
        new Object[]{8, 80L, 0.8f, 0.8, "Bob", new ByteArray(new byte[]{3}), 0, 1_800_000_000_000L}),
        schema);

    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(source, _allocator);
    try {
      List<Object[]> out = arrowBlock.asRowHeap().getRows();
      assertEquals(out.size(), 3);

      // Row 0: values use Pinot's internal boxed types (BOOLEAN -> Integer, BYTES -> ByteArray).
      assertEquals(out.get(0)[0], 7);
      assertEquals(out.get(0)[1], 70L);
      assertEquals(out.get(0)[2], 0.7f);
      assertEquals(out.get(0)[3], 0.7);
      assertEquals(out.get(0)[4], "Alice");
      assertEquals(out.get(0)[5], new ByteArray(new byte[]{1, 2}));
      assertEquals(out.get(0)[6], 1);
      assertEquals(out.get(0)[7], 1_700_000_000_000L);

      // Row 1: every cell is null.
      for (int col = 0; col < schema.size(); col++) {
        assertNull(out.get(1)[col], "row 1 col " + col + " must be null");
      }

      assertEquals(out.get(2)[5], new ByteArray(new byte[]{3}));
      assertEquals(out.get(2)[6], 0);

      // asSerialized() must not throw — DataBlockBuilder's strict casts pass only if asRowHeap
      // emitted the right Pinot internal types.
      SerializedDataBlock serialized = arrowBlock.asSerialized();
      assertEquals(serialized.getNumRows(), 3);
      DataBlock db = serialized.getDataBlock();
      assertEquals(db.getBytes(0, 5), new ByteArray(new byte[]{1, 2}));
      assertEquals(db.getInt(0, 6), 1);
      assertEquals(db.getInt(2, 6), 0);
    } finally {
      arrowBlock.release();
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

    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(serialized, _allocator);
    try {
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
    } finally {
      arrowBlock.release();
    }
  }

  @Test
  public void testEmptyBlock() {
    DataSchema schema = new DataSchema(new String[]{"id"},
        new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(Collections.emptyList(), schema);

    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    try {
      assertEquals(arrowBlock.getNumRows(), 0);
    } finally {
      arrowBlock.release();
    }
  }

  @Test
  public void testAlreadyArrowBlockPassthrough() {
    DataSchema schema = new DataSchema(new String[]{"id"},
        new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{1}), schema);
    ArrowBlock original = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    try {
      ArrowBlock same = ArrowBlockConverter.toArrowBlock(original, _allocator);
      assertSame(same, original, "converting an ArrowBlock must return the same instance");
      // Pass-through is a no-op transfer: no extra holder, so the refcount is unchanged.
      assertEquals(original.refCount(), 1);
    } finally {
      original.release();
    }
  }

  @Test
  public void testGetNullRowIdsReturnsNullWhenNoNulls() {
    DataSchema schema = new DataSchema(new String[]{"id"},
        new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{1}, new Object[]{2}), schema);

    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    try {
      assertNull(arrowBlock.getDataBlock().getNullRowIds(0), "non-null column must return null bitmap");
    } finally {
      arrowBlock.release();
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
    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    try {
      assertTrue(arrowBlock.isArrow(), "isArrow() must be true");
      assertFalse(arrowBlock.isSerialized(), "isSerialized() must be false");
      assertFalse(arrowBlock.isRowHeap(), "isRowHeap() must be false");
    } finally {
      arrowBlock.release();
    }
  }

  @Test
  public void testRefCountRetainReleaseLifecycle() {
    DataSchema schema = new DataSchema(new String[]{"id"}, new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{1}), schema);
    ArrowBlock block = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);

    // Fresh block: the constructor's caller is the single holder.
    assertEquals(block.refCount(), 1);

    // retain() registers extra holders (e.g. broadcasting one block to N local mailboxes).
    block.retain();
    block.retain();
    assertEquals(block.refCount(), 3);

    // release() walks the count back down; buffers are freed only when the last holder lets go.
    block.release();
    block.release();
    assertEquals(block.refCount(), 1);

    // Buffers stay allocated as long as any holder remains, and are reclaimed on the final release —
    // assert the actual off-heap reclamation, not just the counter reaching 0.
    assertTrue(_allocator.getAllocatedMemory() > 0, "buffers must remain allocated while a holder is live");
    block.release();
    assertEquals(block.refCount(), 0);
    assertEquals(_allocator.getAllocatedMemory(), 0, "buffers must be reclaimed when the last holder releases");
  }

  @Test
  public void testReleaseAfterFreeThrows() {
    DataSchema schema = new DataSchema(new String[]{"id"}, new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{1}), schema);
    ArrowBlock block = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    block.release();   // refcount 1 -> 0, buffers freed

    // A second release is a double-free and must fail loudly rather than double-close the root.
    expectThrows(IllegalStateException.class, block::release);
  }

  @Test
  public void testRetainAfterFreeThrows() {
    DataSchema schema = new DataSchema(new String[]{"id"}, new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(new Object[]{1}), schema);
    ArrowBlock block = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    block.release();   // freed

    // Retaining a freed block would resurrect a dangling VectorSchemaRoot.
    expectThrows(IllegalStateException.class, block::retain);
  }

  @Test
  public void testLowCardinalityStringColumnIsDictionaryEncoded() {
    // 4 rows, 2 distinct values -> the stored column carries a DictionaryEncoding and decodes correctly.
    DataSchema schema = new DataSchema(new String[]{"s"}, new ColumnDataType[]{ColumnDataType.STRING});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(
        rows(new Object[]{"a"}, new Object[]{"b"}, new Object[]{"a"}, new Object[]{"b"}), schema);
    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    try {
      assertNotNull(arrowBlock.getDataBlock().getRoot().getVector(0).getField().getDictionary(),
          "low-cardinality string column must be dictionary-encoded");
      DataBlock db = arrowBlock.getDataBlock();
      assertEquals(db.getString(0, 0), "a");
      assertEquals(db.getString(1, 0), "b");
      assertEquals(db.getString(2, 0), "a");
      assertEquals(db.getString(3, 0), "b");
    } finally {
      arrowBlock.release();
    }
  }

  @Test
  public void testAllDistinctStringColumnStoredPlain() {
    // All values distinct -> a dictionary is pure overhead, so the column is stored as a plain VarCharVector.
    DataSchema schema = new DataSchema(new String[]{"s"}, new ColumnDataType[]{ColumnDataType.STRING});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(
        rows(new Object[]{"a"}, new Object[]{"b"}, new Object[]{"c"}), schema);
    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    try {
      assertNull(arrowBlock.getDataBlock().getRoot().getVector(0).getField().getDictionary(),
          "all-distinct string column must be stored plain (no dictionary)");
      DataBlock db = arrowBlock.getDataBlock();
      assertEquals(db.getString(0, 0), "a");
      assertEquals(db.getString(1, 0), "b");
      assertEquals(db.getString(2, 0), "c");
    } finally {
      arrowBlock.release();
    }
  }

  @Test
  public void testMultipleStringColumnsGetDistinctDictionaryIds() {
    // Regression guard for apache/pinot#18207: two dictionary-encoded columns must use distinct dictionary
    // ids, otherwise the second column would decode against the first column's dictionary.
    DataSchema schema = new DataSchema(new String[]{"s1", "s2"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(rows(
        new Object[]{"red", "circle"},
        new Object[]{"green", "square"},
        new Object[]{"red", "circle"}), schema);
    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    try {
      long id0 = arrowBlock.getDataBlock().getRoot().getVector(0).getField().getDictionary().getId();
      long id1 = arrowBlock.getDataBlock().getRoot().getVector(1).getField().getDictionary().getId();
      assertTrue(id0 != id1, "each dictionary-encoded column must have a distinct dictionary id");
      DataBlock db = arrowBlock.getDataBlock();
      assertEquals(db.getString(0, 0), "red");
      assertEquals(db.getString(1, 0), "green");
      assertEquals(db.getString(0, 1), "circle");
      assertEquals(db.getString(1, 1), "square");
      assertEquals(db.getString(2, 1), "circle");
    } finally {
      arrowBlock.release();
    }
  }

  @Test
  public void testDictionaryEncodedColumnNullsRoundTrip() {
    DataSchema schema = new DataSchema(new String[]{"s"}, new ColumnDataType[]{ColumnDataType.STRING});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(
        rows(new Object[]{"a"}, new Object[]{null}, new Object[]{"a"}, new Object[]{"b"}), schema);
    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    try {
      assertNotNull(arrowBlock.getDataBlock().getRoot().getVector(0).getField().getDictionary());
      DataBlock db = arrowBlock.getDataBlock();
      assertEquals(db.getString(0, 0), "a");
      assertNull(db.getString(1, 0));
      assertEquals(db.getString(2, 0), "a");
      assertEquals(db.getString(3, 0), "b");
      RoaringBitmap nullIds = db.getNullRowIds(0);
      assertNotNull(nullIds);
      assertTrue(nullIds.contains(1));
      assertFalse(nullIds.contains(0));
    } finally {
      arrowBlock.release();
    }
  }

  @Test
  public void testDictionaryBuffersFreedOnRelease() {
    // The dictionary vectors are extra off-heap buffers on top of the index vector; releasing the block
    // must free both, or the allocator close in tearDown would throw.
    DataSchema schema = new DataSchema(new String[]{"s"}, new ColumnDataType[]{ColumnDataType.STRING});
    RowHeapDataBlock rowBlock = new RowHeapDataBlock(
        rows(new Object[]{"a"}, new Object[]{"b"}, new Object[]{"a"}), schema);
    ArrowBlock arrowBlock = ArrowBlockConverter.toArrowBlock(rowBlock, _allocator);
    assertTrue(_allocator.getAllocatedMemory() > 0, "dictionary + index buffers must be allocated");
    arrowBlock.release();
    assertEquals(_allocator.getAllocatedMemory(), 0,
        "releasing the block must free the index vector and the dictionary buffers");
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
