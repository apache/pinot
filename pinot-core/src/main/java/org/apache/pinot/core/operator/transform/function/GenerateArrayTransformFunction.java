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

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.ArrayFunctions;
import org.apache.pinot.common.function.scalar.array.GenerateArrayScalarFunction;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;

public class GenerateArrayTransformFunction implements TransformFunction {
  public static final String FUNCTION_NAME = "generateArray";

  private final DataType _dataType;

  private final int[] _intArrayLiteral;
  private final long[] _longArrayLiteral;
  private final float[] _floatArrayLiteral;
  private final double[] _doubleArrayLiteral;

  // NOTE:
  // This class can be shared across multiple threads, and the result arrays are lazily initialized and cached. They
  // need to be declared as volatile to ensure instructions are not reordered, or some threads might see uninitialized
  // arrays.
  private volatile int[][] _intArrayResult;
  private volatile long[][] _longArrayResult;
  private volatile float[][] _floatArrayResult;
  private volatile double[][] _doubleArrayResult;

  public GenerateArrayTransformFunction(List<ExpressionContext> literalContexts) {
    Preconditions.checkNotNull(literalContexts);
    if (literalContexts.isEmpty()) {
      _dataType = DataType.UNKNOWN;
      _intArrayLiteral = new int[0];
      _longArrayLiteral = new long[0];
      _floatArrayLiteral = new float[0];
      _doubleArrayLiteral = new double[0];
      return;
    }
    Preconditions.checkState(literalContexts.size() == 2 || literalContexts.size() == 3,
        "GenerateArrayTransformFunction takes only 2 or 3 arguments, found: %s", literalContexts.size());
    for (ExpressionContext literalContext : literalContexts) {
      Preconditions.checkState(literalContext.getType() == ExpressionContext.Type.LITERAL,
          "GenerateArrayTransformFunction only takes literals as arguments, found: %s", literalContext);
    }
    // Generate the widest of the argument types, as the multi-stage engine does. A leaf stage projection is evaluated
    // here, and its result must have the type the multi-stage planner inferred.
    ColumnDataType[] argumentTypes = new ColumnDataType[literalContexts.size()];
    for (int i = 0; i < argumentTypes.length; i++) {
      argumentTypes[i] = ColumnDataType.fromDataTypeSV(literalContexts.get(i).getLiteral().getType());
    }
    ColumnDataType elementType = GenerateArrayScalarFunction.getElementType(argumentTypes);
    Preconditions.checkState(elementType != null, "Illegal argument types for GenerateArrayTransformFunction: %s",
        literalContexts);
    _dataType = elementType.toDataType();
    LiteralContext start = literalContexts.get(0).getLiteral();
    LiteralContext end = literalContexts.get(1).getLiteral();
    LiteralContext inc = literalContexts.size() == 3 ? literalContexts.get(2).getLiteral() : null;
    int[] intArray = null;
    long[] longArray = null;
    float[] floatArray = null;
    double[] doubleArray = null;
    switch (elementType) {
      case INT:
        intArray = inc != null
            ? ArrayFunctions.generateIntArray(start.getIntValue(), end.getIntValue(), inc.getIntValue())
            : ArrayFunctions.generateIntArray(start.getIntValue(), end.getIntValue());
        break;
      case LONG:
        longArray = inc != null
            ? ArrayFunctions.generateLongArray(start.getLongValue(), end.getLongValue(), inc.getLongValue())
            : ArrayFunctions.generateLongArray(start.getLongValue(), end.getLongValue());
        break;
      case FLOAT:
        floatArray = inc != null
            ? ArrayFunctions.generateFloatArray(start.getFloatValue(), end.getFloatValue(), inc.getFloatValue())
            : ArrayFunctions.generateFloatArray(start.getFloatValue(), end.getFloatValue());
        break;
      case DOUBLE:
        doubleArray = inc != null
            ? ArrayFunctions.generateDoubleArray(start.getDoubleValue(), end.getDoubleValue(), inc.getDoubleValue())
            : ArrayFunctions.generateDoubleArray(start.getDoubleValue(), end.getDoubleValue());
        break;
      default:
        throw new IllegalStateException("Unexpected element type for GenerateArrayTransformFunction: " + elementType);
    }
    _intArrayLiteral = intArray;
    _longArrayLiteral = longArray;
    _floatArrayLiteral = floatArray;
    _doubleArrayLiteral = doubleArray;
  }

  public int[] getIntArrayLiteral() {
    return _intArrayLiteral;
  }

  public long[] getLongArrayLiteral() {
    return _longArrayLiteral;
  }

  public float[] getFloatArrayLiteral() {
    return _floatArrayLiteral;
  }

  public double[] getDoubleArrayLiteral() {
    return _doubleArrayLiteral;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    if (arguments.size() < 2) {
      throw new IllegalArgumentException("At least 2 arguments are required for generateArray function");
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(_dataType, false, false);
  }

  @Override
  public int[] transformToDictIdsSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToDictIdsMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    int[][] intArrayResult = _intArrayResult;
    if (intArrayResult == null || intArrayResult.length < numDocs) {
      intArrayResult = new int[numDocs][];
      int[] intArrayLiteral = _intArrayLiteral;
      if (intArrayLiteral == null) {
        switch (_dataType) {
          case LONG:
            intArrayLiteral = new int[_longArrayLiteral.length];
            for (int i = 0; i < _longArrayLiteral.length; i++) {
              intArrayLiteral[i] = (int) _longArrayLiteral[i];
            }
            break;
          case FLOAT:
            intArrayLiteral = new int[_floatArrayLiteral.length];
            for (int i = 0; i < _floatArrayLiteral.length; i++) {
              intArrayLiteral[i] = (int) _floatArrayLiteral[i];
            }
            break;
          case DOUBLE:
            intArrayLiteral = new int[_doubleArrayLiteral.length];
            for (int i = 0; i < _doubleArrayLiteral.length; i++) {
              intArrayLiteral[i] = (int) _doubleArrayLiteral[i];
            }
            break;
          default:
            throw new IllegalStateException("Unable to convert data type: " + _dataType + " to in array");
        }
      }
      Arrays.fill(intArrayResult, intArrayLiteral);
      _intArrayResult = intArrayResult;
    }
    return intArrayResult;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    long[][] longArrayResult = _longArrayResult;
    if (longArrayResult == null || longArrayResult.length < numDocs) {
      longArrayResult = new long[numDocs][];
      long[] longArrayLiteral = _longArrayLiteral;
      if (longArrayLiteral == null) {
        switch (_dataType) {
          case INT:
            longArrayLiteral = new long[_intArrayLiteral.length];
            for (int i = 0; i < _intArrayLiteral.length; i++) {
              longArrayLiteral[i] = _intArrayLiteral[i];
            }
            break;
          case FLOAT:
            longArrayLiteral = new long[_floatArrayLiteral.length];
            for (int i = 0; i < _floatArrayLiteral.length; i++) {
              longArrayLiteral[i] = (long) _floatArrayLiteral[i];
            }
            break;
          case DOUBLE:
            longArrayLiteral = new long[_doubleArrayLiteral.length];
            for (int i = 0; i < _doubleArrayLiteral.length; i++) {
              longArrayLiteral[i] = (long) _doubleArrayLiteral[i];
            }
            break;
          default:
            throw new IllegalStateException("Unable to convert data type: " + _dataType + " to long array");
        }
      }
      Arrays.fill(longArrayResult, longArrayLiteral);
      _longArrayResult = longArrayResult;
    }
    return longArrayResult;
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    float[][] floatArrayResult = _floatArrayResult;
    if (floatArrayResult == null || floatArrayResult.length < numDocs) {
      floatArrayResult = new float[numDocs][];
      float[] floatArrayLiteral = _floatArrayLiteral;
      if (floatArrayLiteral == null) {
        switch (_dataType) {
          case INT:
            floatArrayLiteral = new float[_intArrayLiteral.length];
            for (int i = 0; i < _intArrayLiteral.length; i++) {
              floatArrayLiteral[i] = _intArrayLiteral[i];
            }
            break;
          case LONG:
            floatArrayLiteral = new float[_longArrayLiteral.length];
            for (int i = 0; i < _longArrayLiteral.length; i++) {
              floatArrayLiteral[i] = _longArrayLiteral[i];
            }
            break;
          case DOUBLE:
            floatArrayLiteral = new float[_doubleArrayLiteral.length];
            for (int i = 0; i < _doubleArrayLiteral.length; i++) {
              floatArrayLiteral[i] = (float) _doubleArrayLiteral[i];
            }
            break;
          default:
            throw new IllegalStateException("Unable to convert data type: " + _dataType + " to float array");
        }
      }
      Arrays.fill(floatArrayResult, floatArrayLiteral);
      _floatArrayResult = floatArrayResult;
    }
    return floatArrayResult;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    double[][] doubleArrayResult = _doubleArrayResult;
    if (doubleArrayResult == null || doubleArrayResult.length < numDocs) {
      doubleArrayResult = new double[numDocs][];
      double[] doubleArrayLiteral = _doubleArrayLiteral;
      if (doubleArrayLiteral == null) {
        switch (_dataType) {
          case INT:
            doubleArrayLiteral = new double[_intArrayLiteral.length];
            for (int i = 0; i < _intArrayLiteral.length; i++) {
              doubleArrayLiteral[i] = _intArrayLiteral[i];
            }
            break;
          case LONG:
            doubleArrayLiteral = new double[_longArrayLiteral.length];
            for (int i = 0; i < _longArrayLiteral.length; i++) {
              doubleArrayLiteral[i] = _longArrayLiteral[i];
            }
            break;
          case FLOAT:
            doubleArrayLiteral = new double[_floatArrayLiteral.length];
            for (int i = 0; i < _floatArrayLiteral.length; i++) {
              doubleArrayLiteral[i] = _floatArrayLiteral[i];
            }
            break;
          default:
            throw new IllegalStateException("Unable to convert data type: " + _dataType + " to double array");
        }
      }
      Arrays.fill(doubleArrayResult, doubleArrayLiteral);
      _doubleArrayResult = doubleArrayResult;
    }
    return doubleArrayResult;
  }

  @Override
  public BigDecimal[][] transformToBigDecimalValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock block) {
    // Treat all unknown type values as null regardless of the value.
    if (_dataType != DataType.UNKNOWN) {
      return null;
    }
    int length = block.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0L, length);
    return bitmap;
  }
}
