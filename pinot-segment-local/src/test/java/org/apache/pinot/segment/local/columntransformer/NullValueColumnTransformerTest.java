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

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Comprehensive tests for NullValueColumnTransformer.
 * Tests null value handling and default value substitution for all data types.
 */
public class NullValueColumnTransformerTest {

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  @Test
  public void testIsNoOp() {
    // NullValueColumnTransformer should never be a no-op since it always needs to handle null values
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("intCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    assertFalse(transformer.isNoOp(), "NullValueColumnTransformer should never be a no-op");
  }

  @Test
  public void testTransformNullToDefaultInt() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("intCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertEquals(result, fieldSpec.getDefaultNullValue());
    assertEquals(result, Integer.MIN_VALUE);
  }

  @Test
  public void testTransformNullToDefaultLong() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("longCol", FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("longCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertEquals(result, fieldSpec.getDefaultNullValue());
    assertEquals(result, Long.MIN_VALUE);
  }

  @Test
  public void testTransformNullToDefaultFloat() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("floatCol", FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("floatCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertEquals(result, fieldSpec.getDefaultNullValue());
    assertEquals(result, Float.NEGATIVE_INFINITY);
  }

  @Test
  public void testTransformNullToDefaultDouble() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("doubleCol", FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("doubleCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertEquals(result, fieldSpec.getDefaultNullValue());
    assertEquals(result, Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testTransformNullToDefaultString() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertEquals(result, fieldSpec.getDefaultNullValue());
    assertEquals(result, "null");
  }

  @Test
  public void testTransformNullToDefaultBytes() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("bytesCol", FieldSpec.DataType.BYTES)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("bytesCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertTrue(result instanceof byte[]);
    assertEquals(result, fieldSpec.getDefaultNullValue());
  }

  @Test
  public void testTransformNullToDefaultBoolean() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("boolCol", FieldSpec.DataType.BOOLEAN)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("boolCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertEquals(result, fieldSpec.getDefaultNullValue());
  }

  @Test
  public void testTransformNullToDefaultTimestamp() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("timestampCol", FieldSpec.DataType.TIMESTAMP)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("timestampCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertEquals(result, fieldSpec.getDefaultNullValue());
  }

  @Test
  public void testTransformNullToDefaultIntMV() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("intMVCol", FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("intMVCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 1);
    assertEquals(resultArray[0], Integer.MIN_VALUE);
  }

  @Test
  public void testTransformNullToDefaultLongMV() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("longMVCol", FieldSpec.DataType.LONG)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("longMVCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 1);
    assertEquals(resultArray[0], Long.MIN_VALUE);
  }

  @Test
  public void testTransformNullToDefaultFloatMV() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("floatMVCol", FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("floatMVCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 1);
    assertEquals(resultArray[0], Float.NEGATIVE_INFINITY);
  }

  @Test
  public void testTransformNullToDefaultDoubleMV() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("doubleMVCol", FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("doubleMVCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 1);
    assertEquals(resultArray[0], Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testTransformNullToDefaultStringMV() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("stringMVCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringMVCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 1);
    assertEquals(resultArray[0], "null");
  }

  @Test
  public void testTransformEmptyObjectArrayToNull() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    
    // Empty Object[] should be treated as null
    Object result = transformer.transform(new Object[0]);

    assertNotNull(result);
    assertEquals(result, "null");
  }

  @Test
  public void testTransformEmptyObjectArrayToNullForMV() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("stringMVCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringMVCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    
    // Empty Object[] should be treated as null
    Object result = transformer.transform(new Object[0]);

    assertNotNull(result);
    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 1);
    assertEquals(resultArray[0], "null");
  }

  @Test
  public void testNonNullValuePassThrough() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("intCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    
    Integer testValue = 42;
    Object result = transformer.transform(testValue);

    assertEquals(result, testValue);
  }

  @Test
  public void testNonNullStringPassThrough() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    
    String testValue = "test string";
    Object result = transformer.transform(testValue);

    assertEquals(result, testValue);
  }

  @Test
  public void testNonNullMVPassThrough() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("intMVCol", FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("intMVCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    
    int[] testValue = new int[]{1, 2, 3};
    Object result = transformer.transform(testValue);

    assertEquals(result, testValue);
  }

  @Test
  public void testTimeColumnWithValidDefault() {
    // Create a time column with a valid default value
    Schema schema = new Schema.SchemaBuilder()
        .addDateTime("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .build();
    
    FieldSpec fieldSpec = schema.getFieldSpecFor("timeCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(tableConfig, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    // Should use the field spec's default value since it's valid
    assertTrue(result instanceof Long);
  }

  @Test
  public void testTimeColumnWithInvalidDefault() {
    // Create a time column with an invalid default value (far future)
    Schema schema = new Schema.SchemaBuilder()
        .addDateTimeField("timeCol", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS") // Invalid default - too far in future
        .build();
    
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setTimeColumnName("timeCol")
        .build();
    
    FieldSpec fieldSpec = schema.getFieldSpecFor("timeCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(tableConfig, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    // Should use current time instead of the invalid default
    assertTrue(result instanceof Long);
    long resultTime = (Long) result;
    long currentTime = System.currentTimeMillis();
    // Result should be close to current time (within 1 second)
    assertTrue(Math.abs(resultTime - currentTime) < 1000, 
        "Time should be close to current time, got: " + resultTime + ", current: " + currentTime);
  }

  @Test
  public void testCustomDefaultNullValue() {
    // Test with a field that has a custom default null value
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("customIntCol", FieldSpec.DataType.INT, 999) // Custom default
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("customIntCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertEquals(result, 999);
  }

  @Test
  public void testCustomDefaultNullValueString() {
    // Test with a string field that has a custom default null value
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("customStringCol", FieldSpec.DataType.STRING, "MISSING") // Custom default
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("customStringCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(null);

    assertNotNull(result);
    assertEquals(result, "MISSING");
  }

  @Test
  public void testZeroValue() {
    // Zero should not be treated as null
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("intCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform(0);

    assertEquals(result, 0);
  }

  @Test
  public void testEmptyString() {
    // Empty string should not be treated as null
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringCol");

    NullValueColumnTransformer transformer = new NullValueColumnTransformer(TABLE_CONFIG, fieldSpec, schema);
    Object result = transformer.transform("");

    assertEquals(result, "");
  }
}

