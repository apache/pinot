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

public class GenerateArrayTransformFunctionTest {
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
  public void testGenerateIntArrayTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    int[] inputArray = {0, 10, 1};
    for (int j : inputArray) {
      arrayExpressions.add(ExpressionContext.forLiteral(DataType.INT, j));
    }

    GenerateArrayTransformFunction intArray = new GenerateArrayTransformFunction(arrayExpressions);
    Assert.assertEquals(intArray.getResultMetadata().getDataType(), DataType.INT);
    Assert.assertEquals(intArray.getIntArrayLiteral(), new int[]{
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    });
  }

  @Test
  public void testGenerateLongArrayTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    int[] inputArray = {0, 10, 1};
    for (int j : inputArray) {
      arrayExpressions.add(ExpressionContext.forLiteral(DataType.LONG, (long) j));
    }

    GenerateArrayTransformFunction longArray = new GenerateArrayTransformFunction(arrayExpressions);
    Assert.assertEquals(longArray.getResultMetadata().getDataType(), DataType.LONG);
    Assert.assertEquals(longArray.getLongArrayLiteral(), new long[]{
        0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L
    });
  }

  @Test
  public void testGenerateFloatArrayTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    int[] inputArray = {0, 10, 1};
    for (int j : inputArray) {
      arrayExpressions.add(ExpressionContext.forLiteral(DataType.FLOAT, (float) j));
    }

    GenerateArrayTransformFunction floatArray = new GenerateArrayTransformFunction(arrayExpressions);
    Assert.assertEquals(floatArray.getResultMetadata().getDataType(), DataType.FLOAT);
    Assert.assertEquals(floatArray.getFloatArrayLiteral(), new float[]{
        0f, 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f
    });
  }

  @Test
  public void testGenerateDoubleArrayTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    int[] inputArray = {0, 10, 1};
    for (int j : inputArray) {
      arrayExpressions.add(ExpressionContext.forLiteral(DataType.DOUBLE, (double) j));
    }

    GenerateArrayTransformFunction doubleArray = new GenerateArrayTransformFunction(arrayExpressions);
    Assert.assertEquals(doubleArray.getResultMetadata().getDataType(), DataType.DOUBLE);
    Assert.assertEquals(doubleArray.getDoubleArrayLiteral(), new double[]{
        0d, 1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d
    });
  }
  @Test
  public void testGenerateEmptyArrayTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    GenerateArrayTransformFunction emptyLiteral = new GenerateArrayTransformFunction(arrayExpressions);
    Assert.assertEquals(emptyLiteral.getIntArrayLiteral(), new int[0]);
    Assert.assertEquals(emptyLiteral.getLongArrayLiteral(), new long[0]);
    Assert.assertEquals(emptyLiteral.getFloatArrayLiteral(), new float[0]);
    Assert.assertEquals(emptyLiteral.getDoubleArrayLiteral(), new double[0]);

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
  }
  @Test
  public void testGenerateIntArrayTransformFunctionWithIncorrectStepValue() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    int[] inputArray = {0, 10, -1};
    for (int j : inputArray) {
      arrayExpressions.add(ExpressionContext.forLiteral(DataType.INT, j));
    }

    try {
      GenerateArrayTransformFunction intArray = new GenerateArrayTransformFunction(arrayExpressions);
      Assert.fail();
    } catch (IllegalStateException ignored) {
    }
  }
}
