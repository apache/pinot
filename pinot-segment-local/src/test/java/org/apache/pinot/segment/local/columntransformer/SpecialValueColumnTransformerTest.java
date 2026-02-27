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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * Tests for SpecialValueColumnTransformer.
 */
public class SpecialValueColumnTransformerTest {

  @Test
  public void testIsNoOpForInt() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("intCol", FieldSpec.DataType.INT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("intCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    assertTrue(transformer.isNoOp(), "INT column should be no-op");
  }

  @Test
  public void testIsNoOpForString() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("stringCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    assertTrue(transformer.isNoOp(), "STRING column should be no-op");
  }

  @Test
  public void testIsNotNoOpForFloat() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("floatCol", FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("floatCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    assertFalse(transformer.isNoOp(), "FLOAT column should not be no-op");
  }

  @Test
  public void testIsNotNoOpForDouble() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("doubleCol", FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("doubleCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    assertFalse(transformer.isNoOp(), "DOUBLE column should not be no-op");
  }

  @Test
  public void testIsNotNoOpForBigDecimalWithoutAllowTrailingZeros() {
    Schema schema = new Schema.SchemaBuilder()
        .addMetricField("bigDecimalCol", FieldSpec.DataType.BIG_DECIMAL)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("bigDecimalCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    assertFalse(transformer.isNoOp(), "BIG_DECIMAL column without allowTrailingZeros should not be no-op");
  }

  @Test
  public void testTransformFloatNaN() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("floatCol", FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("floatCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Object result = transformer.transform(Float.NaN);

    assertNull(result, "Float.NaN should be transformed to null");
  }

  @Test
  public void testTransformDoubleNaN() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("doubleCol", FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("doubleCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Object result = transformer.transform(Double.NaN);

    assertNull(result, "Double.NaN should be transformed to null");
  }

  @Test
  public void testTransformNegativeZeroFloat() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("floatCol", FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("floatCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Object result = transformer.transform(-0.0f);

    assertEquals(result, 0.0f);
    assertEquals(Float.floatToRawIntBits((Float) result), Float.floatToRawIntBits(0.0f));
  }

  @Test
  public void testTransformNegativeZeroDouble() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("doubleCol", FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("doubleCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Object result = transformer.transform(-0.0d);

    assertEquals(result, 0.0d);
    assertEquals(Double.doubleToRawLongBits((Double) result), Double.doubleToRawLongBits(0.0d));
  }

  @Test
  public void testTransformNormalFloat() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("floatCol", FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("floatCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Float value = 1.5f;
    Object result = transformer.transform(value);

    assertSame(result, value);
  }

  @Test
  public void testTransformNull() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("floatCol", FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("floatCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Object result = transformer.transform(null);

    assertNull(result);
  }

  @Test
  public void testTransformBigDecimalStripTrailingZeros() {
    Schema schema = new Schema.SchemaBuilder()
        .addMetricField("bigDecimalCol", FieldSpec.DataType.BIG_DECIMAL)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("bigDecimalCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    BigDecimal value = new BigDecimal("123.4500");
    Object result = transformer.transform(value);

    assertEquals(result, new BigDecimal("123.45"));
  }

  @Test
  public void testTransformMultiValueWithNaN() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("floatMVCol", FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("floatMVCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Object[] values = new Object[]{1.0f, Float.NaN, 2.0f};
    Object result = transformer.transform(values);

    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 2);
    assertEquals(resultArray[0], 1.0f);
    assertEquals(resultArray[1], 2.0f);
  }

  @Test
  public void testTransformMultiValueWithNegativeZero() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("doubleMVCol", FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("doubleMVCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Object[] values = new Object[]{-0.0d, 1.0d, 2.0d};
    Object result = transformer.transform(values);

    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 3);
    assertEquals(resultArray[0], 0.0d);
    assertEquals(Double.doubleToRawLongBits((Double) resultArray[0]), Double.doubleToRawLongBits(0.0d));
  }

  @Test
  public void testTransformMultiValueFloatWithNaNAndNegativeZero() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("floatMVCol", FieldSpec.DataType.FLOAT)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("floatMVCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Object[] values = new Object[]{-0.0f, Float.NaN, 2.0f};
    Object result = transformer.transform(values);

    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 2);
    assertEquals(resultArray[0], 0.0f);
    assertEquals(Float.floatToRawIntBits((Float) resultArray[0]), Float.floatToRawIntBits(0.0f));
    assertEquals(resultArray[1], 2.0f);
  }

  @Test
  public void testTransformMultiValueDoubleWithNaN() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("doubleMVCol", FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("doubleMVCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Object[] values = new Object[]{-0.0d, Double.NaN, 2.0d};
    Object result = transformer.transform(values);

    assertTrue(result instanceof Object[]);
    Object[] resultArray = (Object[]) result;
    assertEquals(resultArray.length, 2);
    assertEquals(resultArray[0], 0.0d);
    assertEquals(Double.doubleToRawLongBits((Double) resultArray[0]), Double.doubleToRawLongBits(0.0d));
    assertEquals(resultArray[1], 2.0d);
  }

  @Test
  public void testTransformBigDecimalZeroVariants() {
    Schema schema = new Schema.SchemaBuilder()
        .addMetricField("bigDecimalCol", FieldSpec.DataType.BIG_DECIMAL)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("bigDecimalCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);

    assertEquals(transformer.transform(new BigDecimal("0")), BigDecimal.ZERO);
    assertEquals(transformer.transform(new BigDecimal("0.0")), BigDecimal.ZERO);
    assertEquals(transformer.transform(new BigDecimal("0E-18")), BigDecimal.ZERO);
  }

  @Test
  public void testTransformNormalDouble() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("doubleCol", FieldSpec.DataType.DOUBLE)
        .build();
    FieldSpec fieldSpec = schema.getFieldSpecFor("doubleCol");

    SpecialValueColumnTransformer transformer = new SpecialValueColumnTransformer(fieldSpec);
    Double value = 1.5d;
    Object result = transformer.transform(value);

    assertSame(result, value);
  }
}
