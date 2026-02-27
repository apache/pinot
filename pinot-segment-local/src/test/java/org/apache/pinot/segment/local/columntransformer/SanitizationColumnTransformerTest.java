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

import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.MaxLengthExceedStrategy;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for SanitizationColumnTransformer.
 */
public class SanitizationColumnTransformerTest {

  @Test
  public void testIsNoOpForInt() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("intCol");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    assertTrue(transformer.isNoOp(), "INT column should be no-op");
  }

  @Test
  public void testIsNotNoOpForString() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringCol");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    assertFalse(transformer.isNoOp(), "STRING column should not be no-op");
  }

  @Test
  public void testIsNotNoOpForJson() {
    DimensionFieldSpec bytesFieldSpec = new DimensionFieldSpec("jsonCol", FieldSpec.DataType.JSON, true);
    bytesFieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);
    Schema schema = new Schema.SchemaBuilder()
        .addField(bytesFieldSpec)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("jsonCol");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    assertFalse(transformer.isNoOp(), "JSON column should not be no-op");
  }

  @Test
  public void testIsNotNoOpForBytes() {
    DimensionFieldSpec bytesFieldSpec = new DimensionFieldSpec("bytesCol", FieldSpec.DataType.BYTES, true);
    bytesFieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);
    Schema schema = new Schema.SchemaBuilder()
        .addField(bytesFieldSpec)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("bytesCol");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    // BYTES with TRIM_LENGTH strategy should not be no-op
    assertFalse(transformer.isNoOp(), "BYTES column with TRIM_LENGTH strategy should not be no-op");
  }

  @Test
  public void testTransformStringNoChange() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringCol");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "hello";
    Object result = transformer.transform(value);

    assertEquals(result, value);
  }

  @Test
  public void testTransformStringWithNullCharacter() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringCol");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "hello\0world";
    Object result = transformer.transform(value);

    assertEquals(result, "hello", "Null character should be trimmed");
  }

  @Test
  public void testTransformNull() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringCol");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    Object result = transformer.transform(null);

    assertNull(result);
  }

  @Test
  public void testTransformStringExceedsMaxLengthWithTrimStrategy() {
    // Create a field spec with max length and TRIM_LENGTH strategy
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("stringCol", FieldSpec.DataType.STRING, true);
    fieldSpec.setMaxLength(7);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "hello world";
    Object result = transformer.transform(value);

    assertEquals(result, "hello w");
  }

  @Test
  public void testTransformStringExceedsMaxLengthWithSubstituteStrategy() {
    // Create a field spec with max length and SUBSTITUTE_DEFAULT_VALUE strategy
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("stringCol", FieldSpec.DataType.STRING, true);
    fieldSpec.setMaxLength(5);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE);
    fieldSpec.setDefaultNullValue("default");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "hello world";
    Object result = transformer.transform(value);

    assertEquals(result, "default");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testTransformStringExceedsMaxLengthWithErrorStrategy() {
    // Create a field spec with max length and ERROR strategy
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("stringCol", FieldSpec.DataType.STRING, true);
    fieldSpec.setMaxLength(5);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "hello world";
    transformer.transform(value); // Should throw
  }

  @Test
  public void testTransformBytes() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("bytesCol", FieldSpec.DataType.BYTES, true);
    fieldSpec.setMaxLength(3);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    byte[] value = new byte[]{1, 2, 3, 4, 5};
    Object result = transformer.transform(value);

    assertEquals(result, new byte[]{1, 2, 3});
  }

  @Test
  public void testTransformBytesNoChange() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("bytesCol", FieldSpec.DataType.BYTES, true);
    fieldSpec.setMaxLength(10);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    byte[] value = new byte[]{1, 2, 3};
    Object result = transformer.transform(value);

    assertEquals(result, value);
  }

  @Test
  public void testTransformMultiValueString() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("stringMVCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringMVCol");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    Object[] values = new Object[]{"hello\0world", "test"};
    Object result = transformer.transform(values);

    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 2);
    assertEquals(resultArray[0], "hello");
    assertEquals(resultArray[1], "test");
  }

  @Test
  public void testTransformMultiValueBytes() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("bytesMVCol", FieldSpec.DataType.BYTES, false);
    fieldSpec.setMaxLength(3);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    Object[] values = new Object[]{new byte[]{1, 2, 3, 4, 5}, new byte[]{6, 7}};
    Object result = transformer.transform(values);

    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 2);
    assertEquals(resultArray[0], new byte[]{1, 2, 3});
    assertEquals(resultArray[1], new byte[]{6, 7});
  }

  // --- JSON max length tests (4 strategies) ---

  @Test
  public void testIsNoOpForJsonWithNoActionStrategy() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("jsonCol", FieldSpec.DataType.JSON, true);
    fieldSpec.setMaxLength(10);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.NO_ACTION);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    assertTrue(transformer.isNoOp(), "JSON column with NO_ACTION strategy should be no-op");
  }

  @Test
  public void testTransformJsonExceedsMaxLengthWithTrimStrategy() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("jsonCol", FieldSpec.DataType.JSON, true);
    fieldSpec.setMaxLength(10);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.TRIM_LENGTH);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "{\"first\": \"daffy\", \"last\": \"duck\"}";
    Object result = transformer.transform(value);

    assertEquals(result, "{\"first\": ");
  }

  @Test
  public void testTransformJsonExceedsMaxLengthWithSubstituteStrategy() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("jsonCol", FieldSpec.DataType.JSON, true);
    fieldSpec.setMaxLength(10);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE);
    fieldSpec.setDefaultNullValue("null");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "{\"first\": \"daffy\", \"last\": \"duck\"}";
    Object result = transformer.transform(value);

    assertEquals(result, "null");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testTransformJsonExceedsMaxLengthWithErrorStrategy() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("jsonCol", FieldSpec.DataType.JSON, true);
    fieldSpec.setMaxLength(10);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "{\"first\": \"daffy\", \"last\": \"duck\"}";
    transformer.transform(value); // Should throw
  }

  // --- Bytes SUBSTITUTE_DEFAULT_VALUE and ERROR tests ---

  @Test
  public void testTransformBytesExceedsMaxLengthWithSubstituteStrategy() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("bytesCol", FieldSpec.DataType.BYTES, true);
    fieldSpec.setMaxLength(3);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE);
    fieldSpec.setDefaultNullValue(new byte[0]);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    byte[] value = new byte[]{1, 2, 3, 4, 5};
    Object result = transformer.transform(value);

    assertEquals(result, new byte[0]);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testTransformBytesExceedsMaxLengthWithErrorStrategy() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("bytesCol", FieldSpec.DataType.BYTES, true);
    fieldSpec.setMaxLength(3);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    byte[] value = new byte[]{1, 2, 3, 4, 5};
    transformer.transform(value); // Should throw
  }

  // --- String null character with ERROR and SUBSTITUTE strategies ---

  @Test(expectedExceptions = IllegalStateException.class)
  public void testTransformStringWithNullCharacterAndErrorStrategy() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("stringCol", FieldSpec.DataType.STRING, true);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "hello\0world";
    transformer.transform(value); // Should throw
  }

  @Test
  public void testTransformStringWithNullCharacterAndSubstituteStrategy() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("stringCol", FieldSpec.DataType.STRING, true);
    fieldSpec.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.SUBSTITUTE_DEFAULT_VALUE);
    fieldSpec.setDefaultNullValue("null");

    SanitizationColumnTransformer transformer = new SanitizationColumnTransformer(fieldSpec);
    String value = "hello\0world";
    Object result = transformer.transform(value);

    assertEquals(result, "null");
  }
}
