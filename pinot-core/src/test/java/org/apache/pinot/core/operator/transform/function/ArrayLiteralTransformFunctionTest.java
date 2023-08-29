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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.spi.data.FieldSpec.DataType;
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
      arrayExpressions.add(ExpressionContext.forLiteralContext(DataType.INT, i));
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
      arrayExpressions.add(ExpressionContext.forLiteralContext(DataType.LONG, (long) i));
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
      arrayExpressions.add(ExpressionContext.forLiteralContext(DataType.FLOAT, (double) i));
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
      arrayExpressions.add(ExpressionContext.forLiteralContext(DataType.DOUBLE, (double) i));
    }

    ArrayLiteralTransformFunction doubleArray = new ArrayLiteralTransformFunction(arrayExpressions);
    Assert.assertEquals(doubleArray.getResultMetadata().getDataType(), DataType.DOUBLE);
    Assert.assertEquals(doubleArray.getDoubleArrayLiteral(), new double[]{
        0d, 1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d
    });
  }

  @Test
  public void testStringArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      arrayExpressions.add(
          ExpressionContext.forLiteralContext(new Literal(Literal._Fields.STRING_VALUE, String.valueOf(i))));
    }

    ArrayLiteralTransformFunction stringArray = new ArrayLiteralTransformFunction(arrayExpressions);
    Assert.assertEquals(stringArray.getResultMetadata().getDataType(), DataType.STRING);
    Assert.assertEquals(stringArray.getStringArrayLiteral(), new String[]{
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"
    });
  }

  @Test
  public void testEmptyArrayTransform() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    ArrayLiteralTransformFunction emptyLiteral = new ArrayLiteralTransformFunction(arrayExpressions);
    Assert.assertEquals(emptyLiteral.getIntArrayLiteral(), new int[0]);
    Assert.assertEquals(emptyLiteral.getLongArrayLiteral(), new long[0]);
    Assert.assertEquals(emptyLiteral.getFloatArrayLiteral(), new float[0]);
    Assert.assertEquals(emptyLiteral.getDoubleArrayLiteral(), new double[0]);
    Assert.assertEquals(emptyLiteral.getStringArrayLiteral(), new String[0]);

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

    String[][] strings = emptyLiteral.transformToStringValuesMV(_projectionBlock);
    Assert.assertEquals(strings.length, NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      Assert.assertEquals(strings[i].length, 0);
    }
  }
}
