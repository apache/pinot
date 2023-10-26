package org.apache.pinot.core.operator.transform.function;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;

public class ArrayGenerateTransformFunctionTest {
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
    int[] inputArray = {0, 10, 1};
    for (int i = 0; i < inputArray.length; i++) {
      arrayExpressions.add(ExpressionContext.forLiteralContext(FieldSpec.DataType.INT, inputArray[i]));
    }

    ArrayGenerateTransformFunction intArray = new ArrayGenerateTransformFunction(arrayExpressions);
    Assert.assertEquals(intArray.getResultMetadata().getDataType(), FieldSpec.DataType.INT);
    Assert.assertEquals(intArray.getIntArrayLiteral(), new int[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    });
  }

  @Test
  public void testLongArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    int[] inputArray = {0, 10, 1};
    for (int i = 0; i < inputArray.length; i++) {
      arrayExpressions.add(ExpressionContext.forLiteralContext(FieldSpec.DataType.LONG, (long) inputArray[i]));
    }

    ArrayGenerateTransformFunction longArray = new ArrayGenerateTransformFunction(arrayExpressions);
    Assert.assertEquals(longArray.getResultMetadata().getDataType(), FieldSpec.DataType.LONG);
    Assert.assertEquals(longArray.getLongArrayLiteral(), new long[]{
            0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L
    });
  }

  @Test
  public void testFloatArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    int[] inputArray = {0, 10, 1};
    for (int i = 0; i < inputArray.length; i++) {
      arrayExpressions.add(ExpressionContext.forLiteralContext(FieldSpec.DataType.FLOAT, (float) inputArray[i]));
    }

    ArrayGenerateTransformFunction floatArray = new ArrayGenerateTransformFunction(arrayExpressions);
    Assert.assertEquals(floatArray.getResultMetadata().getDataType(), FieldSpec.DataType.FLOAT);
    Assert.assertEquals(floatArray.getFloatArrayLiteral(), new float[]{
            0f, 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f
    });
  }

  @Test
  public void testDoubleArrayLiteralTransformFunction() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    int[] inputArray = {0, 10, 1};
    for (int i = 0; i < inputArray.length; i++) {
      arrayExpressions.add(ExpressionContext.forLiteralContext(FieldSpec.DataType.DOUBLE, (double) inputArray[i]));
    }

    ArrayGenerateTransformFunction doubleArray = new ArrayGenerateTransformFunction(arrayExpressions);
    Assert.assertEquals(doubleArray.getResultMetadata().getDataType(), FieldSpec.DataType.DOUBLE);
    Assert.assertEquals(doubleArray.getDoubleArrayLiteral(), new double[]{
            0d, 1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d
    });
  }
  @Test
  public void testEmptyArrayTransform() {
    List<ExpressionContext> arrayExpressions = new ArrayList<>();
    ArrayGenerateTransformFunction emptyLiteral = new ArrayGenerateTransformFunction(arrayExpressions);
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
}