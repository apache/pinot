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
package org.apache.pinot.common.datablock;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link ArrowDataBlock}. Builds {@link VectorSchemaRoot}s directly via Arrow's API.
 *
 * <p>{@link #testAllScalarTypesHappyAndNullPath} covers every scalar Pinot type that
 * {@link ArrowDataBlock} can model — INT, LONG, FLOAT, DOUBLE, STRING, BIG_DECIMAL, BYTES, and
 * BOOLEAN via BitVector — in a single comprehensive round-trip test (typed getter + null row +
 * {@code getNullRowIds} bitmap). BIG_DECIMAL is reachable here because this test writes to a
 * {@code VarCharVector} directly; {@code ArrowBlockConverter} does not currently route BIG_DECIMAL
 * (the underlying extract utility only handles {@code DataType.STRING}). Array, map, and
 * custom-object getters are expected to throw in this PR; that is verified in
 * {@link #testUnsupportedDataGettersAndLegacyAccessorsThrow}.
 */
public class ArrowDataBlockTest {
  private static final DataSchema NUMERIC_SCHEMA = new DataSchema(
      new String[]{"i", "l", "f", "d"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE});

  private RootAllocator _allocator;

  @BeforeMethod
  public void setUp() {
    _allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterMethod
  public void tearDown() {
    _allocator.close();
  }

  @Test
  public void testSchemaAndShape() {
    try (VectorSchemaRoot root = buildNumericRoot()) {
      try (ArrowDataBlock block = new ArrowDataBlock(root, NUMERIC_SCHEMA)) {
        DataSchema schema = block.getDataSchema();
        assertEquals(schema.size(), 4);
        assertEquals(schema.getColumnName(0), "i");
        assertEquals(schema.getColumnDataType(0), ColumnDataType.INT);
        assertEquals(schema.getColumnDataType(1), ColumnDataType.LONG);
        assertEquals(schema.getColumnDataType(2), ColumnDataType.FLOAT);
        assertEquals(schema.getColumnDataType(3), ColumnDataType.DOUBLE);
        assertEquals(block.getNumberOfRows(), 2);
        assertEquals(block.getNumberOfColumns(), 4);
        assertEquals(block.getDataBlockType(), DataBlock.Type.ARROW);
        // Schema is the identity stored at construction — same instance across calls.
        assertTrue(block.getDataSchema() == schema);
      }
    }
  }

  /**
   * Single comprehensive test for every scalar type the converter can produce. Each column has row 0
   * valued, row 1 null, row 2 valued. Verifies that typed getters read non-null rows correctly, that
   * object-typed getters return null for the null row, and that {@code getNullRowIds} reports exactly
   * {1} for every column — proving null-bitmap reads are vector-type-agnostic.
   */
  @Test
  public void testAllScalarTypesHappyAndNullPath() {
    Schema arrowSchema = new Schema(Arrays.asList(
        new Field("i", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("l", FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("f", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
        new Field("d", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
        new Field("s", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("bd", FieldType.nullable(new ArrowType.Utf8()), null),
        new Field("b", FieldType.nullable(new ArrowType.Binary()), null),
        new Field("bool", FieldType.nullable(new ArrowType.Bool()), null)));
    DataSchema dataSchema = new DataSchema(
        new String[]{"i", "l", "f", "d", "s", "bd", "b", "bool"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT,
            ColumnDataType.DOUBLE, ColumnDataType.STRING, ColumnDataType.BIG_DECIMAL,
            ColumnDataType.BYTES, ColumnDataType.BOOLEAN});
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, _allocator)) {
      populateAllTypesThreeRowsMiddleNull(root);
      try (ArrowDataBlock block = new ArrowDataBlock(root, dataSchema)) {
        // Every column reports row 1 (and only row 1) as null.
        for (int col = 0; col < dataSchema.size(); col++) {
          RoaringBitmap nullIds = block.getNullRowIds(col);
          assertNotNull(nullIds, "col " + col + " must have a null bitmap");
          assertFalse(nullIds.contains(0), "col " + col + " row 0 must be non-null");
          assertTrue(nullIds.contains(1), "col " + col + " row 1 must be null");
          assertFalse(nullIds.contains(2), "col " + col + " row 2 must be non-null");
        }
        // Non-null rows read back via the type-specific getter.
        assertEquals(block.getInt(0, 0), 7);
        assertEquals(block.getInt(2, 0), Integer.MAX_VALUE);
        assertEquals(block.getLong(0, 1), 70L);
        assertEquals(block.getLong(2, 1), Long.MAX_VALUE);
        assertEquals(block.getFloat(0, 2), 0.7f, 0.0001f);
        assertEquals(block.getFloat(2, 2), Float.MAX_VALUE, 0.0001f);
        assertEquals(block.getDouble(0, 3), 0.7, 0.0001);
        assertEquals(block.getDouble(2, 3), Double.MAX_VALUE, 0.0001);
        assertEquals(block.getString(0, 4), "Alice");
        assertEquals(block.getString(2, 4), "Bob");
        assertEquals(block.getBigDecimal(0, 5), new BigDecimal("123.456"));
        assertEquals(block.getBigDecimal(2, 5), new BigDecimal("0.0001"));
        assertEquals(block.getBytes(0, 6), new ByteArray(new byte[]{1, 2, 3}));
        assertEquals(block.getBytes(2, 6), new ByteArray(new byte[]{9}));
        assertEquals(block.getInt(0, 7), 1);
        assertEquals(block.getInt(2, 7), 0);
        // Object-typed getters return null for the null row.
        assertNull(block.getString(1, 4));
        assertNull(block.getBigDecimal(1, 5));
        assertNull(block.getBytes(1, 6));
      }
    }
  }

  @Test
  public void testGetNullRowIdsReturnsNullWhenNoNulls() {
    try (VectorSchemaRoot root = buildNumericRoot()) {
      try (ArrowDataBlock block = new ArrowDataBlock(root, NUMERIC_SCHEMA)) {
        for (int col = 0; col < NUMERIC_SCHEMA.size(); col++) {
          assertNull(block.getNullRowIds(col), "col " + col + " has no nulls — bitmap must be null");
        }
      }
    }
  }

  @Test
  public void testGetIntFromUnsupportedVectorThrows() {
    // getInt is only defined for IntVector / BitVector. A BigIntVector (LONG) must throw.
    Schema arrowSchema = new Schema(List.of(new Field("l", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    DataSchema dataSchema = new DataSchema(new String[]{"l"}, new ColumnDataType[]{ColumnDataType.LONG});
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, _allocator)) {
      BigIntVector lv = (BigIntVector) root.getVector(0);
      lv.allocateNew(1);
      lv.set(0, 1L);
      lv.setValueCount(1);
      root.setRowCount(1);
      try (ArrowDataBlock block = new ArrowDataBlock(root, dataSchema)) {
        assertThrows(UnsupportedOperationException.class, () -> block.getInt(0, 0));
      }
    }
  }

  @Test
  public void testUnsupportedDataGettersAndLegacyAccessorsThrow() {
    try (VectorSchemaRoot root = buildNumericRoot()) {
      try (ArrowDataBlock block = new ArrowDataBlock(root, NUMERIC_SCHEMA)) {
        // Array / map / custom-object columns are not yet modeled on ArrowDataBlock.
        assertThrows(UnsupportedOperationException.class, () -> block.getIntArray(0, 0));
        assertThrows(UnsupportedOperationException.class, () -> block.getLongArray(0, 0));
        assertThrows(UnsupportedOperationException.class, () -> block.getFloatArray(0, 0));
        assertThrows(UnsupportedOperationException.class, () -> block.getDoubleArray(0, 0));
        assertThrows(UnsupportedOperationException.class, () -> block.getStringArray(0, 0));
        assertThrows(UnsupportedOperationException.class, () -> block.getMap(0, 0));
        assertThrows(UnsupportedOperationException.class, () -> block.getCustomObject(0, 0));
        // Legacy DataBlock accessors that assume a flat-byte-buffer layout have no Arrow equivalent.
        assertThrows(UnsupportedOperationException.class, block::getStringDictionary);
        assertThrows(UnsupportedOperationException.class, block::getFixedData);
        assertThrows(UnsupportedOperationException.class, block::getVarSizeData);
        assertThrows(UnsupportedOperationException.class, block::serialize);
      }
    }
  }

  @Test
  public void testExceptionsAndNonDataApis() {
    try (VectorSchemaRoot root = buildNumericRoot()) {
      try (ArrowDataBlock block = new ArrowDataBlock(root, NUMERIC_SCHEMA)) {
        assertTrue(block.getExceptions().isEmpty());
        assertTrue(block.getMetadata().isEmpty());
        assertNotNull(block.getStatsByStage());
        assertTrue(block.getStatsByStage().isEmpty());
        block.addException(500, "Internal error");
        block.addException(400, "Bad request");
        Map<Integer, String> exceptions = block.getExceptions();
        assertEquals(exceptions.size(), 2);
        assertEquals(exceptions.get(500), "Internal error");
        assertEquals(exceptions.get(400), "Bad request");
      }
    }
  }

  @Test
  public void testCloseReleasesOffHeapMemory() {
    VectorSchemaRoot root = buildNumericRoot();
    assertTrue(_allocator.getAllocatedMemory() > 0, "allocator should have non-zero memory before close");
    ArrowDataBlock block = new ArrowDataBlock(root, NUMERIC_SCHEMA);
    block.close();
    assertEquals(_allocator.getAllocatedMemory(), 0, "all memory must be released after close()");
  }

  @Test
  public void testToStringIncludesShape() {
    try (VectorSchemaRoot root = buildNumericRoot()) {
      try (ArrowDataBlock block = new ArrowDataBlock(root, NUMERIC_SCHEMA)) {
        String s = block.toString();
        assertTrue(s.contains("\"type\": \"arrow\""));
        assertTrue(s.contains("\"numRows\": 2"));
        assertTrue(s.contains("\"numCols\": 4"));
      }
    }
  }

  // ----- helpers -----

  private VectorSchemaRoot buildNumericRoot() {
    Schema schema = new Schema(Arrays.asList(
        new Field("i", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("l", FieldType.nullable(new ArrowType.Int(64, true)), null),
        new Field("f", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
        new Field("d", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, _allocator);
    IntVector iv = (IntVector) root.getVector(0);
    BigIntVector lv = (BigIntVector) root.getVector(1);
    Float4Vector fv = (Float4Vector) root.getVector(2);
    Float8Vector dv = (Float8Vector) root.getVector(3);
    iv.allocateNew(2);
    lv.allocateNew(2);
    fv.allocateNew(2);
    dv.allocateNew(2);
    iv.set(0, 42);
    iv.set(1, Integer.MAX_VALUE);
    lv.set(0, 10L);
    lv.set(1, Long.MAX_VALUE);
    fv.set(0, 3.14f);
    fv.set(1, Float.MAX_VALUE);
    dv.set(0, 2.718);
    dv.set(1, Double.MAX_VALUE);
    iv.setValueCount(2);
    lv.setValueCount(2);
    fv.setValueCount(2);
    dv.setValueCount(2);
    root.setRowCount(2);
    return root;
  }

  /**
   * Populates the 8-column all-types root with row 0 valued, row 1 null, row 2 valued. Column order
   * must match {@link #testAllScalarTypesHappyAndNullPath}: INT, LONG, FLOAT, DOUBLE, STRING,
   * BIG_DECIMAL (Utf8), BYTES, BOOLEAN (BitVector).
   */
  private static void populateAllTypesThreeRowsMiddleNull(VectorSchemaRoot root) {
    IntVector iv = (IntVector) root.getVector(0);
    BigIntVector lv = (BigIntVector) root.getVector(1);
    Float4Vector fv = (Float4Vector) root.getVector(2);
    Float8Vector dv = (Float8Vector) root.getVector(3);
    VarCharVector sv = (VarCharVector) root.getVector(4);
    VarCharVector bdv = (VarCharVector) root.getVector(5);
    VarBinaryVector bv = (VarBinaryVector) root.getVector(6);
    BitVector boolv = (BitVector) root.getVector(7);

    iv.allocateNew(3);
    iv.set(0, 7);
    iv.setNull(1);
    iv.set(2, Integer.MAX_VALUE);
    iv.setValueCount(3);

    lv.allocateNew(3);
    lv.set(0, 70L);
    lv.setNull(1);
    lv.set(2, Long.MAX_VALUE);
    lv.setValueCount(3);

    fv.allocateNew(3);
    fv.set(0, 0.7f);
    fv.setNull(1);
    fv.set(2, Float.MAX_VALUE);
    fv.setValueCount(3);

    dv.allocateNew(3);
    dv.set(0, 0.7);
    dv.setNull(1);
    dv.set(2, Double.MAX_VALUE);
    dv.setValueCount(3);

    sv.allocateNew(3);
    sv.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
    sv.setNull(1);
    sv.setSafe(2, "Bob".getBytes(StandardCharsets.UTF_8));
    sv.setValueCount(3);

    bdv.allocateNew(3);
    bdv.setSafe(0, "123.456".getBytes(StandardCharsets.UTF_8));
    bdv.setNull(1);
    bdv.setSafe(2, "0.0001".getBytes(StandardCharsets.UTF_8));
    bdv.setValueCount(3);

    bv.allocateNew(3);
    bv.setSafe(0, new byte[]{1, 2, 3});
    bv.setNull(1);
    bv.setSafe(2, new byte[]{9});
    bv.setValueCount(3);

    boolv.allocateNew(3);
    boolv.set(0, 1);
    boolv.setNull(1);
    boolv.set(2, 0);
    boolv.setValueCount(3);

    root.setRowCount(3);
  }
}
