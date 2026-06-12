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
import java.util.Arrays;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.readers.MultiValueResult;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Comprehensive tests for [DefaultValueColumnReader].
///
/// This test validates:
/// - Single-value fields for all data types (INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL, STRING, BYTES)
/// - Multi-value fields for all data types
/// - Sequential access methods (next*, hasNext, skipNext, isNextNull)
/// - Random access methods (get*, isNull)
/// - Type indicator methods (isInt, isLong, isBigDecimal, etc.)
/// - Rewind functionality
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
    Assert.assertEquals(reader.getColumnName(), COLUMN_NAME);
    Assert.assertEquals(reader.getTotalDocs(), NUM_DOCS);
    Assert.assertTrue(reader.isInt());
    Assert.assertFalse(reader.isLong());
    Assert.assertFalse(reader.isString());

    // Test sequential access with nextInt
    int expectedValue = ((Number) fieldSpec.getDefaultNullValue()).intValue();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertTrue(reader.hasNext());
      Assert.assertFalse(reader.isNextNull());
      Assert.assertEquals(reader.nextInt(), expectedValue);
    }
    Assert.assertFalse(reader.hasNext());

    // Test rewind and next()
    reader.rewind();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(reader.next(), expectedValue);
    }

    // Test random access
    reader.rewind();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertFalse(reader.isNull(i));
      Assert.assertEquals(reader.getInt(i), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueLongColumn() {
    FieldSpec fieldSpec = new MetricFieldSpec(COLUMN_NAME, FieldSpec.DataType.LONG);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    Assert.assertFalse(reader.isInt());
    Assert.assertTrue(reader.isLong());
    Assert.assertFalse(reader.isString());

    // Test sequential access with nextLong
    long expectedValue = ((Number) fieldSpec.getDefaultNullValue()).longValue();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertTrue(reader.hasNext());
      Assert.assertEquals(reader.nextLong(), expectedValue);
    }

    // Test random access
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(reader.getLong(i), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueFloatColumn() {
    FieldSpec fieldSpec = new MetricFieldSpec(COLUMN_NAME, FieldSpec.DataType.FLOAT);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    Assert.assertTrue(reader.isFloat());
    Assert.assertFalse(reader.isDouble());

    // Test sequential access with nextFloat
    float expectedValue = ((Number) fieldSpec.getDefaultNullValue()).floatValue();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(reader.nextFloat(), expectedValue);
    }

    // Test random access
    reader.rewind();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(reader.getFloat(i), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueDoubleColumn() {
    FieldSpec fieldSpec = new MetricFieldSpec(COLUMN_NAME, FieldSpec.DataType.DOUBLE);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    Assert.assertFalse(reader.isFloat());
    Assert.assertTrue(reader.isDouble());

    // Test sequential access with nextDouble
    double expectedValue = ((Number) fieldSpec.getDefaultNullValue()).doubleValue();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(reader.nextDouble(), expectedValue);
    }

    // Test random access
    reader.rewind();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(reader.getDouble(i), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueBigDecimalColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.BIG_DECIMAL, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    Assert.assertFalse(reader.isDouble());
    Assert.assertTrue(reader.isBigDecimal());
    Assert.assertFalse(reader.isString());

    // Test sequential access with nextBigDecimal
    BigDecimal expectedValue = (BigDecimal) fieldSpec.getDefaultNullValue();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertTrue(reader.hasNext());
      Assert.assertFalse(reader.isNextNull());
      Assert.assertEquals(reader.nextBigDecimal().compareTo(expectedValue), 0);
    }
    Assert.assertFalse(reader.hasNext());

    // Test rewind and next()
    reader.rewind();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(((BigDecimal) reader.next()).compareTo(expectedValue), 0);
    }

    // Test random access
    reader.rewind();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertFalse(reader.isNull(i));
      Assert.assertEquals(reader.getBigDecimal(i).compareTo(expectedValue), 0);
    }

    reader.close();
  }

  @Test
  public void testSingleValueStringColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    Assert.assertTrue(reader.isString());
    Assert.assertFalse(reader.isBytes());

    // Test sequential access with nextString
    String expectedValue = (String) fieldSpec.getDefaultNullValue();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(reader.nextString(), expectedValue);
    }

    // Test random access
    reader.rewind();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(reader.getString(i), expectedValue);
    }

    reader.close();
  }

  @Test
  public void testSingleValueBytesColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.BYTES, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    Assert.assertFalse(reader.isString());
    Assert.assertTrue(reader.isBytes());

    // Test sequential access with nextBytes
    byte[] expectedValue = (byte[]) fieldSpec.getDefaultNullValue();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertTrue(Arrays.equals(reader.nextBytes(), expectedValue));
    }

    // Test random access
    reader.rewind();
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertTrue(Arrays.equals(reader.getBytes(i), expectedValue));
    }

    reader.close();
  }

  // ========== Multi-Value Field Tests ==========

  @Test
  public void testMultiValueIntColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    Assert.assertTrue(reader.isInt());

    // Test sequential access with nextIntMV
    int expectedValue = ((Number) fieldSpec.getDefaultNullValue()).intValue();
    int[] expectedArray = new int[]{expectedValue};
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertTrue(reader.hasNext());
      MultiValueResult<int[]> mvResult = reader.nextIntMV();
      Assert.assertFalse(mvResult.hasNulls());
      Assert.assertTrue(Arrays.equals(mvResult.getValues(), expectedArray));
    }

    // Test random access
    reader.rewind();
    for (int i = 0; i < NUM_DOCS; i++) {
      MultiValueResult<int[]> mvResult = reader.getIntMV(i);
      Assert.assertFalse(mvResult.hasNulls());
      Assert.assertTrue(Arrays.equals(mvResult.getValues(), expectedArray));
    }

    // Test that the same array instance is returned (optimization)
    reader.rewind();
    int[] firstCall = reader.getIntMV(0).getValues();
    int[] secondCall = reader.getIntMV(1).getValues();
    Assert.assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueLongColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.LONG, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test type indicators
    Assert.assertTrue(reader.isLong());

    // Test sequential access with nextLongMV
    long expectedValue = ((Number) fieldSpec.getDefaultNullValue()).longValue();
    long[] expectedArray = new long[]{expectedValue};
    for (int i = 0; i < NUM_DOCS; i++) {
      MultiValueResult<long[]> mvResult = reader.nextLongMV();
      Assert.assertFalse(mvResult.hasNulls());
      Assert.assertTrue(Arrays.equals(mvResult.getValues(), expectedArray));
    }

    // Test random access and array reuse
    reader.rewind();
    long[] firstCall = reader.getLongMV(0).getValues();
    long[] secondCall = reader.getLongMV(1).getValues();
    Assert.assertTrue(Arrays.equals(firstCall, expectedArray));
    Assert.assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueFloatColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.FLOAT, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test sequential access with nextFloatMV
    float expectedValue = ((Number) fieldSpec.getDefaultNullValue()).floatValue();
    float[] expectedArray = new float[]{expectedValue};
    for (int i = 0; i < NUM_DOCS; i++) {
      MultiValueResult<float[]> mvResult = reader.nextFloatMV();
      Assert.assertFalse(mvResult.hasNulls());
      Assert.assertTrue(Arrays.equals(mvResult.getValues(), expectedArray));
    }

    // Test random access and array reuse
    reader.rewind();
    float[] firstCall = reader.getFloatMV(0).getValues();
    float[] secondCall = reader.getFloatMV(1).getValues();
    Assert.assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueDoubleColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.DOUBLE, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test sequential access with nextDoubleMV
    double expectedValue = ((Number) fieldSpec.getDefaultNullValue()).doubleValue();
    double[] expectedArray = new double[]{expectedValue};
    for (int i = 0; i < NUM_DOCS; i++) {
      MultiValueResult<double[]> mvResult = reader.nextDoubleMV();
      Assert.assertFalse(mvResult.hasNulls());
      Assert.assertTrue(Arrays.equals(mvResult.getValues(), expectedArray));
    }

    // Test random access and array reuse
    reader.rewind();
    double[] firstCall = reader.getDoubleMV(0).getValues();
    double[] secondCall = reader.getDoubleMV(1).getValues();
    Assert.assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueStringColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test sequential access with nextStringMV
    String expectedValue = (String) fieldSpec.getDefaultNullValue();
    String[] expectedArray = new String[]{expectedValue};
    for (int i = 0; i < NUM_DOCS; i++) {
      String[] result = reader.nextStringMV();
      Assert.assertTrue(Arrays.equals(result, expectedArray));
    }

    // Test random access and array reuse
    reader.rewind();
    String[] firstCall = reader.getStringMV(0);
    String[] secondCall = reader.getStringMV(1);
    Assert.assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  @Test
  public void testMultiValueBytesColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.BYTES, false);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // Test sequential access with nextBytesMV
    byte[] expectedValue = (byte[]) fieldSpec.getDefaultNullValue();
    byte[][] expectedArray = new byte[][]{expectedValue};
    for (int i = 0; i < NUM_DOCS; i++) {
      byte[][] result = reader.nextBytesMV();
      Assert.assertEquals(result.length, expectedArray.length);
      Assert.assertTrue(Arrays.equals(result[0], expectedArray[0]));
    }

    // Test random access and array reuse
    reader.rewind();
    byte[][] firstCall = reader.getBytesMV(0);
    byte[][] secondCall = reader.getBytesMV(1);
    Assert.assertSame(firstCall, secondCall, "Multi-value arrays should be reused");

    reader.close();
  }

  // ========== Edge Cases and Error Handling ==========

  @Test(expectedExceptions = IllegalStateException.class)
  public void testNextPastEnd() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 5, fieldSpec);

    // Read all values
    for (int i = 0; i < 5; i++) {
      reader.nextInt();
    }

    // This should throw IllegalStateException
    reader.nextInt();
    reader.close();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testIsNextNullPastEnd() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 5, fieldSpec);

    // Read all values
    for (int i = 0; i < 5; i++) {
      reader.nextInt();
    }

    // This should throw IllegalStateException
    reader.isNextNull();
    reader.close();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testSkipNextPastEnd() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 5, fieldSpec);

    // Read all values
    for (int i = 0; i < 5; i++) {
      reader.skipNext();
    }

    // This should throw IllegalStateException
    reader.skipNext();
    reader.close();
  }

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
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertFalse(reader.isNull(i));
      Assert.assertFalse(reader.isNextNull());
      reader.skipNext();
    }

    reader.close();
  }

  @Test
  public void testSkipNext() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 10, fieldSpec);

    int expectedValue = ((Number) fieldSpec.getDefaultNullValue()).intValue();

    // Skip every other value
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(reader.nextInt(), expectedValue);
      reader.skipNext();
    }

    Assert.assertFalse(reader.hasNext());
    reader.close();
  }

  @Test
  public void testRewind() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 10, fieldSpec);

    int expectedValue = ((Number) fieldSpec.getDefaultNullValue()).intValue();

    // Read all values
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(reader.nextInt(), expectedValue);
    }
    Assert.assertFalse(reader.hasNext());

    // Rewind and read again
    reader.rewind();
    Assert.assertTrue(reader.hasNext());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(reader.nextInt(), expectedValue);
    }
    Assert.assertFalse(reader.hasNext());

    reader.close();
  }

  @Test
  public void testNextObjectMethod() {
    // Test single-value field
    FieldSpec svFieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader svReader = new DefaultValueColumnReader(COLUMN_NAME, 5, svFieldSpec);

    int expectedSvValue = ((Number) svFieldSpec.getDefaultNullValue()).intValue();
    for (int i = 0; i < 5; i++) {
      Object value = svReader.next();
      Assert.assertEquals(value, expectedSvValue);
    }
    svReader.close();

    // Test multi-value field
    FieldSpec mvFieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, false);
    DefaultValueColumnReader mvReader = new DefaultValueColumnReader(COLUMN_NAME, 5, mvFieldSpec);

    int expectedMvValue = ((Number) mvFieldSpec.getDefaultNullValue()).intValue();
    Object[] expectedArray = new Object[]{expectedMvValue};
    for (int i = 0; i < 5; i++) {
      Object value = mvReader.next();
      Assert.assertTrue(value instanceof Object[]);
      Assert.assertTrue(Arrays.equals((Object[]) value, expectedArray));
    }
    mvReader.close();
  }

  @Test
  public void testGetTotalDocs() {
    for (int numDocs : new int[]{0, 1, 10, 100, 1000}) {
      FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
      DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, numDocs, fieldSpec);
      Assert.assertEquals(reader.getTotalDocs(), numDocs);
      reader.close();
    }
  }

  @Test
  public void testEmptyColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 0, fieldSpec);

    Assert.assertEquals(reader.getTotalDocs(), 0);
    Assert.assertFalse(reader.hasNext());

    reader.close();
  }

  @Test
  public void testSingleDocColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 1, fieldSpec);

    int expectedValue = ((Number) fieldSpec.getDefaultNullValue()).intValue();

    Assert.assertTrue(reader.hasNext());
    Assert.assertEquals(reader.nextInt(), expectedValue);
    Assert.assertFalse(reader.hasNext());

    reader.rewind();
    Assert.assertEquals(reader.getInt(0), expectedValue);

    reader.close();
  }

  @Test
  public void testAllTypeIndicators() {
    // Test INT
    FieldSpec intSpec = new DimensionFieldSpec("int_col", FieldSpec.DataType.INT, true);
    DefaultValueColumnReader intReader = new DefaultValueColumnReader("int_col", 1, intSpec);
    Assert.assertTrue(intReader.isInt());
    Assert.assertFalse(intReader.isLong());
    Assert.assertFalse(intReader.isFloat());
    Assert.assertFalse(intReader.isDouble());
    Assert.assertFalse(intReader.isBigDecimal());
    Assert.assertFalse(intReader.isString());
    Assert.assertFalse(intReader.isBytes());
    intReader.close();

    // Test LONG
    FieldSpec longSpec = new DimensionFieldSpec("long_col", FieldSpec.DataType.LONG, true);
    DefaultValueColumnReader longReader = new DefaultValueColumnReader("long_col", 1, longSpec);
    Assert.assertFalse(longReader.isInt());
    Assert.assertTrue(longReader.isLong());
    longReader.close();

    // Test FLOAT
    FieldSpec floatSpec = new DimensionFieldSpec("float_col", FieldSpec.DataType.FLOAT, true);
    DefaultValueColumnReader floatReader = new DefaultValueColumnReader("float_col", 1, floatSpec);
    Assert.assertTrue(floatReader.isFloat());
    Assert.assertFalse(floatReader.isDouble());
    floatReader.close();

    // Test DOUBLE
    FieldSpec doubleSpec = new DimensionFieldSpec("double_col", FieldSpec.DataType.DOUBLE, true);
    DefaultValueColumnReader doubleReader = new DefaultValueColumnReader("double_col", 1, doubleSpec);
    Assert.assertFalse(doubleReader.isFloat());
    Assert.assertTrue(doubleReader.isDouble());
    doubleReader.close();

    // Test BIG_DECIMAL
    FieldSpec bigDecimalSpec = new DimensionFieldSpec("big_decimal_col", FieldSpec.DataType.BIG_DECIMAL, true);
    DefaultValueColumnReader bigDecimalReader = new DefaultValueColumnReader("big_decimal_col", 1, bigDecimalSpec);
    Assert.assertFalse(bigDecimalReader.isDouble());
    Assert.assertTrue(bigDecimalReader.isBigDecimal());
    Assert.assertFalse(bigDecimalReader.isString());
    bigDecimalReader.close();

    // Test STRING
    FieldSpec stringSpec = new DimensionFieldSpec("string_col", FieldSpec.DataType.STRING, true);
    DefaultValueColumnReader stringReader = new DefaultValueColumnReader("string_col", 1, stringSpec);
    Assert.assertTrue(stringReader.isString());
    Assert.assertFalse(stringReader.isBytes());
    stringReader.close();

    // Test BYTES
    FieldSpec bytesSpec = new DimensionFieldSpec("bytes_col", FieldSpec.DataType.BYTES, true);
    DefaultValueColumnReader bytesReader = new DefaultValueColumnReader("bytes_col", 1, bytesSpec);
    Assert.assertFalse(bytesReader.isString());
    Assert.assertTrue(bytesReader.isBytes());
    Assert.assertFalse(bytesReader.isBigDecimal());
    bytesReader.close();
  }

  @Test
  public void testMultipleReadsFromSamePosition() {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 10, fieldSpec);

    int expectedValue = ((Number) fieldSpec.getDefaultNullValue()).intValue();

    // Read from the same random access position multiple times
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(reader.getInt(0), expectedValue);
      Assert.assertEquals(reader.getInt(5), expectedValue);
      Assert.assertEquals(reader.getInt(9), expectedValue);
    }

    reader.close();
  }
}
