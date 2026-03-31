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
package org.apache.pinot.segment.local.columntransformer;

import java.math.BigDecimal;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


/**
 * Comprehensive tests for DataTypeColumnTransformer.
 * Tests data type conversions, isNoOp optimization, and error handling.
 */
public class DataTypeColumnTransformerTest {
  private static final String COLUMN_NAME = "testColumn";

  private static class MockColumnReaderBuilder {
    private boolean _isSingleValue = true;
    private boolean _isInt;
    private boolean _isLong;
    private boolean _isFloat;
    private boolean _isDouble;
    private boolean _isBigDecimal;
    private boolean _isString;
    private boolean _isBytes;

    MockColumnReaderBuilder multiValue() {
      _isSingleValue = false;
      return this;
    }

    MockColumnReaderBuilder asInt() {
      _isInt = true;
      return this;
    }

    MockColumnReaderBuilder asLong() {
      _isLong = true;
      return this;
    }

    MockColumnReaderBuilder asFloat() {
      _isFloat = true;
      return this;
    }

    MockColumnReaderBuilder asDouble() {
      _isDouble = true;
      return this;
    }

    MockColumnReaderBuilder asBigDecimal() {
      _isBigDecimal = true;
      return this;
    }

    MockColumnReaderBuilder asString() {
      _isString = true;
      return this;
    }

    MockColumnReaderBuilder asBytes() {
      _isBytes = true;
      return this;
    }

    ColumnReader build() {
      ColumnReader reader = Mockito.mock(ColumnReader.class);
      when(reader.getColumnName()).thenReturn(COLUMN_NAME);
      when(reader.isSingleValue()).thenReturn(_isSingleValue);
      when(reader.isInt()).thenReturn(_isInt);
      when(reader.isLong()).thenReturn(_isLong);
      when(reader.isFloat()).thenReturn(_isFloat);
      when(reader.isDouble()).thenReturn(_isDouble);
      when(reader.isBigDecimal()).thenReturn(_isBigDecimal);
      when(reader.isString()).thenReturn(_isString);
      when(reader.isBytes()).thenReturn(_isBytes);
      return reader;
    }
  }

  private static MockColumnReaderBuilder mockColumnReader() {
    return new MockColumnReaderBuilder();
  }

  // isNoOp - SV matching types (in order: INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL, STRING, BYTES)

  @Test
  public void testIsNoOpForMatchingIntTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asInt().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both INT");
  }

  @Test
  public void testIsNoOpForMatchingLongTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asLong().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both LONG");
  }

  @Test
  public void testIsNoOpForMatchingFloatTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asFloat().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both FLOAT");
  }

  @Test
  public void testIsNoOpForMatchingDoubleTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asDouble().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both DOUBLE");
  }

  @Test
  public void testIsNoOpForMatchingBigDecimalTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.BIG_DECIMAL)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asBigDecimal().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both BIG_DECIMAL");
  }

  @Test
  public void testIsNoOpForMatchingStringTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both STRING");
  }

  @Test
  public void testIsNoOpForMatchingBytesTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.BYTES)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asBytes().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both BYTES");
  }

  // isNoOp - MV matching types (in order: INT, LONG, FLOAT, DOUBLE, STRING, BYTES)

  @Test
  public void testIsNoOpForMatchingIntMVTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().multiValue().asInt().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both INT[]");
  }

  @Test
  public void testIsNoOpForMatchingLongMVTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().multiValue().asLong().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both LONG[]");
  }

  @Test
  public void testIsNoOpForMatchingStringMVTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().multiValue().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both STRING[]");
  }

  @Test
  public void testIsNoOpForMatchingBytesMVTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.BYTES)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().multiValue().asBytes().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertTrue(transformer.isNoOp(), "Should be no-op when source and dest are both BYTES[]");
  }

  // isNoOp - non-matching types

  @Test
  public void testIsNotNoOpForDifferentTypes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asInt().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertFalse(transformer.isNoOp(), "Should not be no-op when converting INT to LONG");
  }

  @Test
  public void testIsNotNoOpForBytesMismatch() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asBytes().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertFalse(transformer.isNoOp(), "Should not be no-op when source is BYTES but dest is STRING");
  }

  @Test
  public void testIsNotNoOpForBytesMVMismatch() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().multiValue().asBytes().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertFalse(transformer.isNoOp(), "Should not be no-op when source is BYTES[] but dest is STRING[]");
  }

  @Test
  public void testIsNotNoOpForJson() {
    // JSON type should not be no-op
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.JSON)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    assertFalse(transformer.isNoOp());
  }

  // transform tests

  @Test
  public void testTransformNullValue() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asInt().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform(null);
    assertNull(result, "Null values should pass through as null");
  }

  @Test
  public void testTransformStringToInt() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform("42");
    assertEquals(result, 42);
  }

  @Test
  public void testTransformStringToLong() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform("1234567890");
    assertEquals(result, 1234567890L);
  }

  @Test
  public void testTransformStringToFloat() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform("3.14");
    assertEquals(result, 3.14f);
  }

  @Test
  public void testTransformStringToDouble() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform("3.14159");
    assertEquals(result, 3.14159);
  }

  @Test
  public void testTransformStringToBigDecimal() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.BIG_DECIMAL)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform("123.456");
    assertTrue(result instanceof BigDecimal);
    assertEquals(((BigDecimal) result).compareTo(new BigDecimal("123.456")), 0);
  }

  @Test
  public void testTransformStringToBoolean() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.BOOLEAN)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform("true");
    assertEquals(result, 1); // Boolean true is stored as 1

    result = transformer.transform("false");
    assertEquals(result, 0); // Boolean false is stored as 0
  }

  @Test
  public void testTransformIntToLong() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asInt().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform(42);
    assertEquals(result, 42L);
  }

  @Test
  public void testTransformIntToString() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asInt().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform(42);
    assertEquals(result, "42");
  }

  @Test
  public void testTransformLongToTimestamp() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.TIMESTAMP)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asLong().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    long timestampValue = 1609459200000L; // 2021-01-01 00:00:00 UTC
    Object result = transformer.transform(timestampValue);
    assertEquals(result, timestampValue);
  }

  @Test
  public void testTransformFloatToDouble() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asFloat().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform(3.14f);
    assertTrue(result instanceof Double);
    assertEquals((Double) result, 3.14, 0.001);
  }

  @Test
  public void testTransformDoubleToFloat() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asDouble().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform(3.14159);
    assertTrue(result instanceof Float);
    assertEquals((Float) result, 3.14159f, 0.001f);
  }

  @Test
  public void testTransformBooleanToString() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform(true);
    assertEquals(result, "true");
  }

  @Test
  public void testTransformBytesToString() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asBytes().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    byte[] bytes = "test".getBytes();
    Object result = transformer.transform(bytes);
    assertTrue(result instanceof String);
  }

  @Test
  public void testTransformStringArrayToIntArray() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().multiValue().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform(new Object[]{"1", "2", "3"});
    Object[] intArray = (Object[]) result;
    assertEquals(intArray.length, 3);
    assertEquals(intArray[0], 1);
    assertEquals(intArray[1], 2);
    assertEquals(intArray[2], 3);
  }

  @Test
  public void testTransformEmptyArray() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    // Empty arrays should be standardized to null
    Object result = transformer.transform(new Object[0]);
    assertNull(result);
  }

  @Test
  public void testTransformSingleElementArray() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    // Single element arrays should be unwrapped
    Object result = transformer.transform(new Object[]{"42"});
    assertEquals(result, 42);
  }

  @Test
  public void testTransformPreservesValue() {
    // When source and dest types match (no-op case), value should still be transformed
    // to ensure any internal representation conversion happens
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asInt().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    Object result = transformer.transform(42);
    assertEquals(result, 42);
  }

  @Test
  public void testMVToSVConversionError() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    // Try to convert multi-value array to single-value
    expectThrows(RuntimeException.class, () -> transformer.transform(new Object[]{"1", "2", "3"}));
  }

  @Test
  public void testInvalidConversionWithContinueOnErrorFalse() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    // Default table config has continueOnError = false
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    // Try to convert invalid string to int
    expectThrows(RuntimeException.class, () -> transformer.transform("not_a_number"));
  }

  @Test
  public void testInvalidConversionWithContinueOnErrorTrue() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN_NAME, FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor(COLUMN_NAME);

    // Set continueOnError = true
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setContinueOnError(true);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setIngestionConfig(ingestionConfig)
        .build();
    ColumnReader reader = mockColumnReader().asString().build();
    DataTypeColumnTransformer transformer = new DataTypeColumnTransformer(tableConfig, fieldSpec, reader);

    // Try to convert invalid string to int - should return null
    Object result = transformer.transform("not_a_number");
    assertNull(result, "Invalid conversion should return null when continueOnError=true");
  }
}
