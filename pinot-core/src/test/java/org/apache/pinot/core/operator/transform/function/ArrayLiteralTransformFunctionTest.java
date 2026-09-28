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
package org.apache.pinot.core.operator.transform.function;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class ArrayLiteralTransformFunctionTest {
  private static final int NUM_DOCS = 100;
  private AutoCloseable _mocks;

  @Mock
  private ProjectionBlock _projectionBlock;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_projectionBlock.getNumDocs()).thenReturn(NUM_DOCS);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testIntArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      arrayExpressions.add(ExpressionContext.forLiteral(DataType.INT, i));
    }

    ArrayLiteralTransformFunction intArray = new ArrayLiteralTransformFunction(arrayExpressions);
    Assert.assertEquals(intArray.getResultMetadata().getDataType(), DataType.INT);
    Assert.assertEquals(intArray.getIntArrayLiteral(), new int[]{
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9
    });
  }

  @Test
  public void testLongArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      arrayExpressions.add(ExpressionContext.forLiteral(DataType.LONG, (long) i));
    }

    ArrayLiteralTransformFunction longArray = new ArrayLiteralTransformFunction(arrayExpressions);
    Assert.assertEquals(longArray.getResultMetadata().getDataType(), DataType.LONG);
    Assert.assertEquals(longArray.getLongArrayLiteral(), new long[]{
        0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L
    });
  }

  @Test
  public void testFloatArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      arrayExpressions.add(ExpressionContext.forLiteral(DataType.FLOAT, (float) i));
    }

    ArrayLiteralTransformFunction floatArray = new ArrayLiteralTransformFunction(arrayExpressions);
    Assert.assertEquals(floatArray.getResultMetadata().getDataType(), DataType.FLOAT);
    Assert.assertEquals(floatArray.getFloatArrayLiteral(), new float[]{
        0f, 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f
    });
  }

  @Test
  public void testDoubleArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      arrayExpressions.add(ExpressionContext.forLiteral(DataType.DOUBLE, (double) i));
    }

    ArrayLiteralTransformFunction doubleArray = new ArrayLiteralTransformFunction(arrayExpressions);
    Assert.assertEquals(doubleArray.getResultMetadata().getDataType(), DataType.DOUBLE);
    Assert.assertEquals(doubleArray.getDoubleArrayLiteral(), new double[]{
        0d, 1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d
    });
  }

  @Test
  public void testBigDecimalArrayLiteralTransformFunction() {
    BigDecimal[] expected = new BigDecimal[]{new BigDecimal("123.45"), new BigDecimal("678.90")};
    List<ExpressionContext> arrayExpressions = List.of(
        ExpressionContext.forLiteral(DataType.BIG_DECIMAL, expected[0]),
        ExpressionContext.forLiteral(DataType.BIG_DECIMAL, expected[1]));

    List<ArrayLiteralTransformFunction> bigDecimalArrays = List.of(
        new ArrayLiteralTransformFunction(arrayExpressions),
        new ArrayLiteralTransformFunction(new LiteralContext(DataType.BIG_DECIMAL, expected)));
    for (ArrayLiteralTransformFunction bigDecimalArray : bigDecimalArrays) {
      Assert.assertEquals(bigDecimalArray.getResultMetadata().getDataType(), DataType.BIG_DECIMAL);
      Assert.assertFalse(bigDecimalArray.getResultMetadata().isSingleValue());
      Assert.assertEquals(bigDecimalArray.getBigDecimalArrayLiteral(), expected);

      BigDecimal[][] values = bigDecimalArray.transformToBigDecimalValuesMV(_projectionBlock);
      Assert.assertEquals(values.length, NUM_DOCS);
      for (BigDecimal[] value : values) {
        Assert.assertEquals(value, expected);
      }

      String[][] stringValues = bigDecimalArray.transformToStringValuesMV(_projectionBlock);
      Assert.assertEquals(stringValues.length, NUM_DOCS);
      for (String[] stringValue : stringValues) {
        Assert.assertEquals(stringValue, new String[]{"123.45", "678.90"});
      }

      double[][] doubleValues = bigDecimalArray.transformToDoubleValuesMV(_projectionBlock);
      Assert.assertEquals(doubleValues.length, NUM_DOCS);
      for (double[] doubleValue : doubleValues) {
        Assert.assertEquals(doubleValue, new double[]{123.45, 678.90});
      }
    }
  }

  @Test
  public void testBooleanArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = List.of(
        ExpressionContext.forLiteral(DataType.BOOLEAN, true),
        ExpressionContext.forLiteral(DataType.BOOLEAN, false));

    List<ArrayLiteralTransformFunction> booleanArrays = List.of(
        new ArrayLiteralTransformFunction(arrayExpressions),
        new ArrayLiteralTransformFunction(new LiteralContext(DataType.BOOLEAN, new boolean[]{true, false})),
        new ArrayLiteralTransformFunction(new LiteralContext(DataType.BOOLEAN, new Boolean[]{true, false})));
    for (ArrayLiteralTransformFunction booleanArray : booleanArrays) {
      Assert.assertEquals(booleanArray.getResultMetadata().getDataType(), DataType.BOOLEAN);
      Assert.assertFalse(booleanArray.getResultMetadata().isSingleValue());
      Assert.assertEquals(booleanArray.getIntArrayLiteral(), new int[]{1, 0});

      int[][] values = booleanArray.transformToIntValuesMV(_projectionBlock);
      Assert.assertEquals(values.length, NUM_DOCS);
      for (int[] value : values) {
        Assert.assertEquals(value, new int[]{1, 0});
      }

      String[][] stringValues = booleanArray.transformToStringValuesMV(_projectionBlock);
      Assert.assertEquals(stringValues.length, NUM_DOCS);
      for (String[] stringValue : stringValues) {
        Assert.assertEquals(stringValue, new String[]{"true", "false"});
      }
    }
  }

  @Test
  public void testTimestampArrayLiteralTransformFunction() {
    Timestamp[] timestamps = new Timestamp[]{new Timestamp(1000L), new Timestamp(2000L)};
    List<ExpressionContext> arrayExpressions = List.of(
        ExpressionContext.forLiteral(DataType.TIMESTAMP, timestamps[0]),
        ExpressionContext.forLiteral(DataType.TIMESTAMP, timestamps[1]));

    List<ArrayLiteralTransformFunction> timestampArrays = List.of(
        new ArrayLiteralTransformFunction(arrayExpressions),
        new ArrayLiteralTransformFunction(new LiteralContext(DataType.TIMESTAMP, timestamps)),
        new ArrayLiteralTransformFunction(new LiteralContext(DataType.TIMESTAMP, new Long[]{1000L, 2000L})),
        new ArrayLiteralTransformFunction(new LiteralContext(DataType.TIMESTAMP, new long[]{1000L, 2000L})));
    for (ArrayLiteralTransformFunction timestampArray : timestampArrays) {
      Assert.assertEquals(timestampArray.getResultMetadata().getDataType(), DataType.TIMESTAMP);
      Assert.assertFalse(timestampArray.getResultMetadata().isSingleValue());
      Assert.assertEquals(timestampArray.getLongArrayLiteral(), new long[]{1000L, 2000L});

      long[][] values = timestampArray.transformToLongValuesMV(_projectionBlock);
      Assert.assertEquals(values.length, NUM_DOCS);
      for (long[] value : values) {
        Assert.assertEquals(value, new long[]{1000L, 2000L});
      }

      String[][] stringValues = timestampArray.transformToStringValuesMV(_projectionBlock);
      Assert.assertEquals(stringValues.length, NUM_DOCS);
      for (String[] stringValue : stringValues) {
        Assert.assertEquals(stringValue, new String[]{timestamps[0].toString(), timestamps[1].toString()});
      }
    }
  }

  @Test
  public void testStringArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      arrayExpressions.add(ExpressionContext.forLiteral(new Literal(Literal._Fields.STRING_VALUE, String.valueOf(i))));
    }

    ArrayLiteralTransformFunction stringArray = new ArrayLiteralTransformFunction(arrayExpressions);
    Assert.assertEquals(stringArray.getResultMetadata().getDataType(), DataType.STRING);
    Assert.assertEquals(stringArray.getStringArrayLiteral(), new String[]{
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"
    });
  }

  @Test
  public void testBytesArrayLiteralTransformFunction() {
    byte[][] expected = {BytesUtils.toBytes("00"), BytesUtils.toBytes("deadbeef")};
    List<ExpressionContext> arrayExpressions = List.of(
        ExpressionContext.forLiteral(DataType.BYTES, expected[0]),
        ExpressionContext.forLiteral(DataType.BYTES, expected[1]));

    List<ArrayLiteralTransformFunction> bytesArrays = List.of(new ArrayLiteralTransformFunction(arrayExpressions),
        new ArrayLiteralTransformFunction(new LiteralContext(RequestUtils.getLiteral(expected))));
    for (ArrayLiteralTransformFunction bytesArray : bytesArrays) {
      Assert.assertEquals(bytesArray.getResultMetadata().getDataType(), DataType.BYTES);
      Assert.assertFalse(bytesArray.getResultMetadata().isSingleValue());
      Assert.assertEquals(bytesArray.getBytesArrayLiteral(), expected);

      byte[][][] values = bytesArray.transformToBytesValuesMV(_projectionBlock);
      Assert.assertEquals(values.length, NUM_DOCS);
      for (byte[][] value : values) {
        Assert.assertEquals(value, expected);
      }
    }
  }

  @Test
  public void testUuidArrayLiteralTransformFunction() {
    UUID[] uuids = new UUID[]{
        UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
        UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    };
    byte[][] expectedBytes = new byte[][]{UuidUtils.toBytes(uuids[0]), UuidUtils.toBytes(uuids[1])};
    List<ExpressionContext> arrayExpressions = List.of(
        ExpressionContext.forLiteral(DataType.UUID, uuids[0]),
        ExpressionContext.forLiteral(DataType.UUID, uuids[1]));

    List<ArrayLiteralTransformFunction> uuidArrays = List.of(
        new ArrayLiteralTransformFunction(arrayExpressions),
        new ArrayLiteralTransformFunction(new LiteralContext(DataType.UUID, uuids)),
        new ArrayLiteralTransformFunction(new LiteralContext(DataType.UUID, expectedBytes)));
    for (ArrayLiteralTransformFunction uuidArray : uuidArrays) {
      Assert.assertEquals(uuidArray.getResultMetadata().getDataType(), DataType.UUID);
      Assert.assertFalse(uuidArray.getResultMetadata().isSingleValue());
      Assert.assertEquals(uuidArray.getBytesArrayLiteral(), expectedBytes);

      byte[][][] values = uuidArray.transformToBytesValuesMV(_projectionBlock);
      Assert.assertEquals(values.length, NUM_DOCS);
      for (byte[][] value : values) {
        Assert.assertEquals(value, expectedBytes);
      }

      String[][] stringValues = uuidArray.transformToStringValuesMV(_projectionBlock);
      Assert.assertEquals(stringValues.length, NUM_DOCS);
      for (String[] stringValue : stringValues) {
        Assert.assertEquals(stringValue, new String[]{
            uuids[0].toString(),
            uuids[1].toString()
        });
      }
    }
  }

  @Test
  public void testEmptyArrayTransform() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    ArrayLiteralTransformFunction emptyLiteral = new ArrayLiteralTransformFunction(arrayExpressions);
    Assert.assertEquals(emptyLiteral.getIntArrayLiteral(), new int[0]);
    Assert.assertEquals(emptyLiteral.getLongArrayLiteral(), new long[0]);
    Assert.assertEquals(emptyLiteral.getFloatArrayLiteral(), new float[0]);
    Assert.assertEquals(emptyLiteral.getDoubleArrayLiteral(), new double[0]);
    Assert.assertEquals(emptyLiteral.getBigDecimalArrayLiteral(), new BigDecimal[0]);
    Assert.assertEquals(emptyLiteral.getStringArrayLiteral(), new String[0]);
    Assert.assertEquals(emptyLiteral.getBytesArrayLiteral(), new byte[0][]);

    int[][] ints = emptyLiteral.transformToIntValuesMV(_projectionBlock);
    Assert.assertEquals(ints.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(ints[i].length, 0);
    }

    long[][] longs = emptyLiteral.transformToLongValuesMV(_projectionBlock);
    Assert.assertEquals(longs.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(longs[i].length, 0);
    }

    float[][] floats = emptyLiteral.transformToFloatValuesMV(_projectionBlock);
    Assert.assertEquals(floats.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(floats[i].length, 0);
    }

    double[][] doubles = emptyLiteral.transformToDoubleValuesMV(_projectionBlock);
    Assert.assertEquals(doubles.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(doubles[i].length, 0);
    }

    BigDecimal[][] bigDecimals = emptyLiteral.transformToBigDecimalValuesMV(_projectionBlock);
    Assert.assertEquals(bigDecimals.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(bigDecimals[i].length, 0);
    }

    String[][] strings = emptyLiteral.transformToStringValuesMV(_projectionBlock);
    Assert.assertEquals(strings.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(strings[i].length, 0);
    }

    byte[][][] bytes = emptyLiteral.transformToBytesValuesMV(_projectionBlock);
    Assert.assertEquals(bytes.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(bytes[i].length, 0);
    }
  }
}
