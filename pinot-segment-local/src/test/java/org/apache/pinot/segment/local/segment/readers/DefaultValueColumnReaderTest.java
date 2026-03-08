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

import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.readers.MultiValueResult;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Comprehensive tests for DefaultValueColumnReader.
 *
 * <p>This test validates:
 * <ul>
 *   <li>Single-value fields for all data types (INT, LONG, FLOAT, DOUBLE, STRING, BYTES)</li>
 *   <li>Multi-value fields for all data types</li>
 *   <li>Sequential access methods (next*, hasNext, skipNext, isNextNull)</li>
 *   <li>Random access methods (get*, isNull)</li>
 *   <li>Type indicator methods (isInt, isLong, etc.)</li>
 *   <li>Rewind functionality</li>
 *   <li>Boundary conditions and exception handling</li>
 *   <li>Array reuse optimization for multi-value fields</li>
 * </ul>
 */
public class DefaultValueColumnReaderTest {

  private static final int NUM_DOCS = 100;
  private static final String COLUMN_NAME = "testColumn";

  // ========== Single-Value Field Tests ==========

  @Test
  public void testSingleValueIntColumn() throws IOException {
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
  public void testSingleValueLongColumn() throws IOException {
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
  public void testSingleValueFloatColumn() throws IOException {
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
  public void testSingleValueDoubleColumn() throws IOException {
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
  public void testSingleValueStringColumn() throws IOException {
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
  public void testSingleValueBytesColumn() throws IOException {
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
  public void testMultiValueIntColumn() throws IOException {
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
  public void testMultiValueLongColumn() throws IOException {
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
  public void testMultiValueFloatColumn() throws IOException {
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
  public void testMultiValueDoubleColumn() throws IOException {
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
  public void testMultiValueStringColumn() throws IOException {
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
  public void testMultiValueBytesColumn() throws IOException {
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
  public void testNextPastEnd() throws IOException {
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
  public void testIsNextNullPastEnd() throws IOException {
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
  public void testSkipNextPastEnd() throws IOException {
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
  public void testRandomAccessOutOfBounds() throws IOException {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // This should throw IndexOutOfBoundsException
    reader.getInt(NUM_DOCS);
    reader.close();
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testRandomAccessNegativeIndex() throws IOException {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, NUM_DOCS, fieldSpec);

    // This should throw IndexOutOfBoundsException
    reader.getInt(-1);
    reader.close();
  }

  @Test
  public void testIsNullAlwaysReturnsFalse() throws IOException {
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
  public void testSkipNext() throws IOException {
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
  public void testRewind() throws IOException {
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
  public void testNextObjectMethod() throws IOException {
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
  public void testGetTotalDocs() throws IOException {
    for (int numDocs : new int[]{0, 1, 10, 100, 1000}) {
      FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
      DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, numDocs, fieldSpec);
      Assert.assertEquals(reader.getTotalDocs(), numDocs);
      reader.close();
    }
  }

  @Test
  public void testEmptyColumn() throws IOException {
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.INT, true);
    DefaultValueColumnReader reader = new DefaultValueColumnReader(COLUMN_NAME, 0, fieldSpec);

    Assert.assertEquals(reader.getTotalDocs(), 0);
    Assert.assertFalse(reader.hasNext());

    reader.close();
  }

  @Test
  public void testSingleDocColumn() throws IOException {
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
  public void testAllTypeIndicators() throws IOException {
    // Test INT
    FieldSpec intSpec = new DimensionFieldSpec("int_col", FieldSpec.DataType.INT, true);
    DefaultValueColumnReader intReader = new DefaultValueColumnReader("int_col", 1, intSpec);
    Assert.assertTrue(intReader.isInt());
    Assert.assertFalse(intReader.isLong());
    Assert.assertFalse(intReader.isFloat());
    Assert.assertFalse(intReader.isDouble());
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
    bytesReader.close();
  }

  @Test
  public void testMultipleReadsFromSamePosition() throws IOException {
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
