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
package org.apache.pinot.query.planner.serde;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class RexExpressionSerDeTest {
  private static final List<ColumnDataType> SUPPORTED_DATE_TYPES =
      List.of(ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
          ColumnDataType.BIG_DECIMAL, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.STRING,
          ColumnDataType.BYTES, ColumnDataType.TIMESTAMP_NTZ, ColumnDataType.DATE, ColumnDataType.TIME,
          ColumnDataType.INT_ARRAY, ColumnDataType.LONG_ARRAY, ColumnDataType.FLOAT_ARRAY,
          ColumnDataType.DOUBLE_ARRAY, ColumnDataType.BOOLEAN_ARRAY, ColumnDataType.TIMESTAMP_ARRAY,
          ColumnDataType.STRING_ARRAY, ColumnDataType.TIMESTAMP_NTZ_ARRAY, ColumnDataType.DATE_ARRAY,
          ColumnDataType.TIME_ARRAY, ColumnDataType.UNKNOWN);
  private static final Random RANDOM = new Random();

  @Test
  public void testNullLiteral() {
    for (ColumnDataType dataType : SUPPORTED_DATE_TYPES) {
      verifyLiteralSerDe(new RexExpression.Literal(dataType, null));
    }
  }

  @Test
  public void testIntLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.INT, RANDOM.nextInt()));
  }

  @Test
  public void testLongLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.LONG, RANDOM.nextLong()));
  }

  @Test
  public void testFloatLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.FLOAT, RANDOM.nextFloat()));
  }

  @Test
  public void testDoubleLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.DOUBLE, RANDOM.nextDouble()));
  }

  @Test
  public void testBigDecimalLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.BIG_DECIMAL,
        RANDOM.nextBoolean() ? BigDecimal.valueOf(RANDOM.nextLong()) : BigDecimal.valueOf(RANDOM.nextDouble())));
  }

  @Test
  public void testBooleanLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.BOOLEAN, BooleanUtils.toInt(RANDOM.nextBoolean())));
  }

  @Test
  public void testTimestampLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.TIMESTAMP, RANDOM.nextLong()));
  }

  @Test
  public void testTimestampNTZLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.TIMESTAMP_NTZ, RANDOM.nextLong()));
  }

  @Test
  public void testDateLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.DATE, RANDOM.nextLong()));
  }

  @Test
  public void testTimeLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.TIME, RANDOM.nextLong()));
  }

  @Test
  public void testStringLiteral() {
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.STRING, RandomStringUtils.random(RANDOM.nextInt(10))));
  }

  @Test
  public void testBytesLiteral() {
    byte[] bytes = new byte[RANDOM.nextInt(10)];
    RANDOM.nextBytes(bytes);
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.BYTES, new ByteArray(bytes)));
  }

  @Test
  public void testIntArrayLiteral() {
    int[] values = new int[RANDOM.nextInt(10)];
    for (int i = 0; i < values.length; i++) {
      values[i] = RANDOM.nextInt();
    }
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.INT_ARRAY, values));
  }

  @Test
  public void testLongArrayLiteral() {
    long[] values = new long[RANDOM.nextInt(10)];
    for (int i = 0; i < values.length; i++) {
      values[i] = RANDOM.nextLong();
    }
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.LONG_ARRAY, values));
  }

  @Test
  public void testFloatArrayLiteral() {
    float[] values = new float[RANDOM.nextInt(10)];
    for (int i = 0; i < values.length; i++) {
      values[i] = RANDOM.nextFloat();
    }
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.FLOAT_ARRAY, values));
  }

  @Test
  public void testDoubleArrayLiteral() {
    double[] values = new double[RANDOM.nextInt(10)];
    for (int i = 0; i < values.length; i++) {
      values[i] = RANDOM.nextDouble();
    }
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.DOUBLE_ARRAY, values));
  }

  @Test
  public void testBooleanArrayLiteral() {
    int[] values = new int[RANDOM.nextInt(10)];
    for (int i = 0; i < values.length; i++) {
      values[i] = BooleanUtils.toInt(RANDOM.nextBoolean());
    }
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.BOOLEAN_ARRAY, values));
  }

  @Test
  public void testTimestampArrayLiteral() {
    long[] values = new long[RANDOM.nextInt(10)];
    for (int i = 0; i < values.length; i++) {
      values[i] = RANDOM.nextLong();
    }
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.TIMESTAMP_ARRAY, values));
  }

  @Test
  public void testStringArrayLiteral() {
    String[] values = new String[RANDOM.nextInt(10)];
    for (int i = 0; i < values.length; i++) {
      values[i] = RandomStringUtils.random(RANDOM.nextInt(10));
    }
    verifyLiteralSerDe(new RexExpression.Literal(ColumnDataType.STRING_ARRAY, values));
  }

  private void verifyLiteralSerDe(RexExpression.Literal literal) {
    assertEquals(literal,
        ProtoExpressionToRexExpression.convertLiteral(RexExpressionToProtoExpression.convertLiteral(literal)));
  }
}
