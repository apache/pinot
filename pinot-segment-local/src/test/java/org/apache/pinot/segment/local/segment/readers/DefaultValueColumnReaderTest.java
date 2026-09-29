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
package org.apache.pinot.segment.local.segment.readers;

import java.math.BigDecimal;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.readers.MultiValueResult;
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Comprehensive tests for [DefaultValueColumnReader].
///
/// This test validates:
/// - Single-value fields for all data types (INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL, STRING, BYTES)
/// - Multi-value fields for all data types
/// - Random access methods (get*, isNull)
/// - Value type reporting (getValueType)
/// - Boundary conditions and exception handling
/// - Array reuse optimization for multi-value fields
public class DefaultValueColumnReaderTest {

  private static final int NUM_DOCS = 100;
  private static final String COLUMN_NAME = "testColumn";

  // ========== Single-Value Field Tests ==========

  @Test
  public void testSingleValueIntColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test metadata
    assertEquals(reader.getColumnName(), COLUMN_NAME);
    assertEquals(reader.getTotalDocs(), NUM_DOCS);
    assertEquals(reader.getValueType(), PinotDataType.INT);

    // Test random access
    int expectedValue = ((Number) fieldSpec.getDefaultNullValue()).intValue();
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      assertFalse(reader.isNull(docId));
      assertEquals(reader.getInt(docId), expectedValue);
      assertEquals(reader.getValue(docId), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueLongColumn() {
    FieldSpec fieldSpec = new MetricFieldSpec(COLUMN_NAME, FieldSpec.DataType.LONG);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    assertEquals(reader.getValueType(), PinotDataType.LONG);

    // Test random access
    long expectedValue = ((Number) fieldSpec.getDefaultNullValue()).longValue();
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      assertEquals(reader.getLong(docId), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueFloatColumn() {
    FieldSpec fieldSpec = new MetricFieldSpec(COLUMN_NAME, FieldSpec.DataType.FLOAT);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    assertEquals(reader.getValueType(), PinotDataType.FLOAT);

    // Test random access
    float expectedValue = ((Number) fieldSpec.getDefaultNullValue()).floatValue();
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      assertEquals(reader.getFloat(docId), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueDoubleColumn() {
    FieldSpec fieldSpec = new MetricFieldSpec(COLUMN_NAME, FieldSpec.DataType.DOUBLE);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    assertEquals(reader.getValueType(), PinotDataType.DOUBLE);

    // Test random access
    double expectedValue = ((Number) fieldSpec.getDefaultNullValue()).doubleValue();
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      assertEquals(reader.getDouble(docId), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueBigDecimalColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.BIG_DECIMAL, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    assertEquals(reader.getValueType(), PinotDataType.BIG_DECIMAL);

    // Test random access
    BigDecimal expectedValue = (BigDecimal) fieldSpec.getDefaultNullValue();
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      assertFalse(reader.isNull(docId));
      assertEquals(reader.getBigDecimal(docId).compareTo(expectedValue), 0);
      assertEquals(((BigDecimal) reader.getValue(docId)).compareTo(expectedValue), 0);
    }

    reader.close();
  }

  @Test
  public void testSingleValueStringColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    assertEquals(reader.getValueType(), PinotDataType.STRING);

    // Test random access
    String expectedValue = (String) fieldSpec.getDefaultNullValue();
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      assertEquals(reader.getString(docId), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueBytesColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.BYTES, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    assertEquals(reader.getValueType(), PinotDataType.BYTES);

    // Test random access
    byte[] expectedValue = (byte[]) fieldSpec.getDefaultNullValue();
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      assertEquals(reader.getBytes(docId), expectedValue);
    }

    reader.close();
  }

  // ========== Multi-Value Field Tests ==========

  @Test
  public void testMultiValueIntColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    assertEquals(reader.getValueType(), PinotDataType.INT_ARRAY);

    // Test random access
    int expectedValue = ((Number) fieldSpec.getDefaultNullValue()).intValue();
    int[] expectedArray = new int[]{expectedValue};
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      MultiValueResult<int[]> mvResult = reader.getIntMV(docId);
      assertFalse(mvResult.hasNulls());
      assertEquals(mvResult.getValues(), expectedArray);
    }

    // Test that the same array instance is returned (optimization)
    int[] firstCall = reader.getIntMV(0).getValues();
    int[] secondCall = reader.getIntMV(1).getValues();
    assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueLongColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.LONG, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    assertEquals(reader.getValueType(), PinotDataType.LONG_ARRAY);

    // Test random access
    long expectedValue = ((Number) fieldSpec.getDefaultNullValue()).longValue();
    long[] expectedArray = new long[]{expectedValue};
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      MultiValueResult<long[]> mvResult = reader.getLongMV(docId);
      assertFalse(mvResult.hasNulls());
      assertEquals(mvResult.getValues(), expectedArray);
    }

    // Test array reuse
    long[] firstCall = reader.getLongMV(0).getValues();
    long[] secondCall = reader.getLongMV(1).getValues();
    assertEquals(firstCall, expectedArray);
    assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueFloatColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.FLOAT, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test random access
    float expectedValue = ((Number) fieldSpec.getDefaultNullValue()).floatValue();
    float[] expectedArray = new float[]{expectedValue};
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      MultiValueResult<float[]> mvResult = reader.getFloatMV(docId);
      assertFalse(mvResult.hasNulls());
      assertEquals(mvResult.getValues(), expectedArray);
    }

    // Test array reuse
    float[] firstCall = reader.getFloatMV(0).getValues();
    float[] secondCall = reader.getFloatMV(1).getValues();
    assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueDoubleColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.DOUBLE, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test random access
    double expectedValue = ((Number) fieldSpec.getDefaultNullValue()).doubleValue();
    double[] expectedArray = new double[]{expectedValue};
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      MultiValueResult<double[]> mvResult = reader.getDoubleMV(docId);
      assertFalse(mvResult.hasNulls());
      assertEquals(mvResult.getValues(), expectedArray);
    }

    // Test array reuse
    double[] firstCall = reader.getDoubleMV(0).getValues();
    double[] secondCall = reader.getDoubleMV(1).getValues();
    assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueStringColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test random access
    String expectedValue = (String) fieldSpec.getDefaultNullValue();
    String[] expectedArray = new String[]{expectedValue};
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      String[] result = reader.getStringMV(docId);
      assertEquals(result, expectedArray);
    }

    // Test array reuse
    String[] firstCall = reader.getStringMV(0);
    String[] secondCall = reader.getStringMV(1);
    assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueBytesColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.BYTES, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test random access
    byte[] expectedValue = (byte[]) fieldSpec.getDefaultNullValue();
    byte[][] expectedArray = new byte[][]{expectedValue};
    for (int docId = 0; docId < reader.getTotalDocs(); docId++) {
      byte[][] result = reader.getBytesMV(docId);
      assertEquals(result.length, expectedArray.length);
      assertEquals(result[0], expectedArray[0]);
    }

    // Test array reuse
    byte[][] firstCall = reader.getBytesMV(0);
    byte[][] secondCall = reader.getBytesMV(1);
    assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  // ========== Edge Cases and Error Handling ==========

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testRandomAccessOutOfBounds() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // This should throw IndexOutOfBoundsException
    reader.getInt(NUM_DOCS);
    reader.close();
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testRandomAccessNegativeIndex() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // This should throw IndexOutOfBoundsException
    reader.getInt(-1);
    reader.close();
  }

  @Test
  public void testIsNullAlwaysReturnsFalse() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Default values are never null
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      assertFalse(reader.isNull(docId));
    }

    reader.close();
  }

  @Test
  public void testGetValueMethod() {
    // Test single-value field
    FieldSpec svFieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader svReader = new DefaultValueColumnReader(COLUMN_NAME, 5, svFieldSpec);

    int expectedSvValue = ((Number) svFieldSpec.getDefaultNullValue()).intValue();
    for (int docId = 0; docId < svReader.getTotalDocs(); docId++) {
      Object value = svReader.getValue(docId);
      assertEquals(value, expectedSvValue);
    }
    svReader.close();

    // Test multi-value field
    FieldSpec mvFieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, false);
    DefaultValueColumnReader mvReader = new DefaultValueColumnReader(COLUMN_NAME, 5, mvFieldSpec);

    int expectedMvValue = ((Number) mvFieldSpec.getDefaultNullValue()).intValue();
    Object[] expectedArray = new Object[]{expectedMvValue};
    for (int docId = 0; docId < mvReader.getTotalDocs(); docId++) {
      Object value = mvReader.getValue(docId);
      assertTrue(value instanceof Object[]);
      assertEquals((Object[]) value, expectedArray);
    }
    mvReader.close();
  }

  @Test
  public void testGetTotalDocs() {
    for (int numDocs : new int[]{0, 1, 10, 100, 1000}) {
      FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
      DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, numDocs, fieldSpec);
      assertEquals(reader.getTotalDocs(), numDocs);
      reader.close();
    }
  }

  @Test
  public void testEmptyColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 0, fieldSpec);

    assertEquals(reader.getTotalDocs(), 0);

    reader.close();
  }

  @Test
  public void testSingleDocColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 1, fieldSpec);

    int expectedValue = ((Number) fieldSpec.getDefaultNullValue()).intValue();

    assertEquals(reader.getTotalDocs(), 1);
    assertEquals(reader.getInt(0), expectedValue);

    reader.close();
  }

  @Test
  public void testAllTypeIndicators() {
    // Test INT
    FieldSpec intSpec = new DimensionFieldSpec("int_col", FieldSpec.DataType.INT, true);
    DefaultValueColumnReader intReader = new DefaultValueColumnReader("int_col", 1, intSpec);
    assertEquals(intReader.getValueType(), PinotDataType.INT);
    intReader.close();

    // Test LONG
    FieldSpec longSpec = new DimensionFieldSpec("long_col", FieldSpec.DataType.LONG, true);
    DefaultValueColumnReader longReader = new DefaultValueColumnReader("long_col", 1, longSpec);
    assertEquals(longReader.getValueType(), PinotDataType.LONG);
    longReader.close();

    // Test FLOAT
    FieldSpec floatSpec = new DimensionFieldSpec("float_col", FieldSpec.DataType.FLOAT, true);
    DefaultValueColumnReader floatReader = new DefaultValueColumnReader("float_col", 1, floatSpec);
    assertEquals(floatReader.getValueType(), PinotDataType.FLOAT);
    floatReader.close();

    // Test DOUBLE
    FieldSpec doubleSpec = new DimensionFieldSpec("double_col", FieldSpec.DataType.DOUBLE, true);
    DefaultValueColumnReader doubleReader = new DefaultValueColumnReader("double_col", 1, doubleSpec);
    assertEquals(doubleReader.getValueType(), PinotDataType.DOUBLE);
    doubleReader.close();

    // Test BIG_DECIMAL
    FieldSpec bigDecimalSpec = new DimensionFieldSpec("big_decimal_col", FieldSpec.DataType.BIG_DECIMAL, true);
    DefaultValueColumnReader bigDecimalReader = new DefaultValueColumnReader("big_decimal_col", 1, bigDecimalSpec);
    assertEquals(bigDecimalReader.getValueType(), PinotDataType.BIG_DECIMAL);
    bigDecimalReader.close();

    // Test STRING
    FieldSpec stringSpec = new DimensionFieldSpec("string_col", FieldSpec.DataType.STRING, true);
    DefaultValueColumnReader stringReader = new DefaultValueColumnReader("string_col", 1, stringSpec);
    assertEquals(stringReader.getValueType(), PinotDataType.STRING);
    stringReader.close();

    // Test BYTES
    FieldSpec bytesSpec = new DimensionFieldSpec("bytes_col", FieldSpec.DataType.BYTES, true);
    DefaultValueColumnReader bytesReader = new DefaultValueColumnReader("bytes_col", 1, bytesSpec);
    assertEquals(bytesReader.getValueType(), PinotDataType.BYTES);
    bytesReader.close();
  }
}
