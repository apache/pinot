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
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.roaringbitmap.RoaringBitmap;


/// The `LiteralTransformFunction` class is a special transform function which is a wrapper on top of a
/// LITERAL. The data type is inferred from the literal string.
public class ArrayLiteralTransformFunction implements TransformFunction {
  public static final String FUNCTION_NAME = "arrayValueConstructor";

  private final DataType _dataType;

  private final int[] _intArrayLiteral;
  private final long[] _longArrayLiteral;
  private final float[] _floatArrayLiteral;
  private final double[] _doubleArrayLiteral;
  private final BigDecimal[] _bigDecimalArrayLiteral;
  private final String[] _stringArrayLiteral;
  private final byte[][] _bytesArrayLiteral;

  // NOTE:
  // This class can be shared across multiple threads, and the result arrays are lazily initialized and cached. They
  // need to be declared as volatile to ensure instructions are not reordered, or some threads might see uninitialized
  // arrays.
  private volatile int[][] _intArrayResult;
  private volatile long[][] _longArrayResult;
  private volatile float[][] _floatArrayResult;
  private volatile double[][] _doubleArrayResult;
  private volatile BigDecimal[][] _bigDecimalArrayResult;
  private volatile String[][] _stringArrayResult;
  private volatile byte[][][] _bytesArrayResult;

  public ArrayLiteralTransformFunction(LiteralContext literalContext) {
    _dataType = literalContext.getType();
    Object value = literalContext.getValue();
    if (value == null) {
      _intArrayLiteral = new int[0];
      _longArrayLiteral = new long[0];
      _floatArrayLiteral = new float[0];
      _doubleArrayLiteral = new double[0];
      _bigDecimalArrayLiteral = new BigDecimal[0];
      _stringArrayLiteral = new String[0];
      _bytesArrayLiteral = new byte[0][];
      return;
    }
    switch (_dataType) {
      case INT:
        _intArrayLiteral = (int[]) value;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case LONG:
        _longArrayLiteral = (long[]) value;
        _intArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case FLOAT:
        _floatArrayLiteral = (float[]) value;
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case DOUBLE:
        _doubleArrayLiteral = (double[]) value;
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case BIG_DECIMAL:
        _bigDecimalArrayLiteral = (BigDecimal[]) value;
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case BOOLEAN:
        if (value instanceof boolean[]) {
          boolean[] boolArr = (boolean[]) value;
          _intArrayLiteral = new int[boolArr.length];
          for (int i = 0; i < boolArr.length; i++) {
            _intArrayLiteral[i] = boolArr[i] ? 1 : 0;
          }
        } else if (value instanceof Boolean[]) {
          Boolean[] boolArr = (Boolean[]) value;
          _intArrayLiteral = new int[boolArr.length];
          for (int i = 0; i < boolArr.length; i++) {
            _intArrayLiteral[i] = Boolean.TRUE.equals(boolArr[i]) ? 1 : 0;
          }
        } else {
          _intArrayLiteral = (int[]) value;
        }
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case TIMESTAMP:
        if (value instanceof Timestamp[]) {
          Timestamp[] timestampArr = (Timestamp[]) value;
          _longArrayLiteral = new long[timestampArr.length];
          for (int i = 0; i < timestampArr.length; i++) {
            _longArrayLiteral[i] = timestampArr[i].getTime();
          }
        } else if (value instanceof Long[]) {
          Long[] longArr = (Long[]) value;
          _longArrayLiteral = new long[longArr.length];
          for (int i = 0; i < longArr.length; i++) {
            _longArrayLiteral[i] = longArr[i];
          }
        } else {
          _longArrayLiteral = (long[]) value;
        }
        _intArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case STRING:
        _stringArrayLiteral = (String[]) value;
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case BYTES:
        _bytesArrayLiteral = (byte[][]) value;
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        break;
      case UUID:
        if (value instanceof UUID[]) {
          UUID[] uuidArr = (UUID[]) value;
          _bytesArrayLiteral = new byte[uuidArr.length][];
          for (int i = 0; i < uuidArr.length; i++) {
            _bytesArrayLiteral[i] = UuidUtils.toBytes(uuidArr[i]);
          }
        } else {
          _bytesArrayLiteral = (byte[][]) value;
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for ArrayLiteralTransformFunction: " + _dataType + ", literal context: "
                + literalContext);
    }
  }

  public ArrayLiteralTransformFunction(List<ExpressionContext> literalContexts) {
    Preconditions.checkNotNull(literalContexts);
    if (literalContexts.isEmpty()) {
      _dataType = DataType.UNKNOWN;
      _intArrayLiteral = new int[0];
      _longArrayLiteral = new long[0];
      _floatArrayLiteral = new float[0];
      _doubleArrayLiteral = new double[0];
      _bigDecimalArrayLiteral = new BigDecimal[0];
      _stringArrayLiteral = new String[0];
      _bytesArrayLiteral = new byte[0][];
      return;
    }
    for (ExpressionContext literalContext : literalContexts) {
      Preconditions.checkState(literalContext.getType() == ExpressionContext.Type.LITERAL,
          "ArrayLiteralTransformFunction only takes literals as arguments, found: %s", literalContext);
    }
    _dataType = literalContexts.get(0).getLiteral().getType();
    switch (_dataType) {
      case INT:
        _intArrayLiteral = new int[literalContexts.size()];
        for (int i = 0; i < _intArrayLiteral.length; i++) {
          _intArrayLiteral[i] = literalContexts.get(i).getLiteral().getIntValue();
        }
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case LONG:
        _longArrayLiteral = new long[literalContexts.size()];
        for (int i = 0; i < _longArrayLiteral.length; i++) {
          _longArrayLiteral[i] = literalContexts.get(i).getLiteral().getLongValue();
        }
        _intArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case FLOAT:
        _floatArrayLiteral = new float[literalContexts.size()];
        for (int i = 0; i < _floatArrayLiteral.length; i++) {
          _floatArrayLiteral[i] = literalContexts.get(i).getLiteral().getFloatValue();
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case DOUBLE:
        _doubleArrayLiteral = new double[literalContexts.size()];
        for (int i = 0; i < _doubleArrayLiteral.length; i++) {
          _doubleArrayLiteral[i] = literalContexts.get(i).getLiteral().getDoubleValue();
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case BIG_DECIMAL:
        _bigDecimalArrayLiteral = new BigDecimal[literalContexts.size()];
        for (int i = 0; i < _bigDecimalArrayLiteral.length; i++) {
          _bigDecimalArrayLiteral[i] = literalContexts.get(i).getLiteral().getBigDecimalValue();
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case BOOLEAN:
        _intArrayLiteral = new int[literalContexts.size()];
        for (int i = 0; i < _intArrayLiteral.length; i++) {
          _intArrayLiteral[i] = literalContexts.get(i).getLiteral().getBooleanValue() ? 1 : 0;
        }
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case TIMESTAMP:
        _longArrayLiteral = new long[literalContexts.size()];
        for (int i = 0; i < _longArrayLiteral.length; i++) {
          _longArrayLiteral[i] = literalContexts.get(i).getLiteral().getLongValue();
        }
        _intArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case STRING:
        _stringArrayLiteral = new String[literalContexts.size()];
        for (int i = 0; i < _stringArrayLiteral.length; i++) {
          _stringArrayLiteral[i] = literalContexts.get(i).getLiteral().getStringValue();
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _bytesArrayLiteral = null;
        break;
      case BYTES:
      case UUID:
        _bytesArrayLiteral = new byte[literalContexts.size()][];
        for (int i = 0; i < _bytesArrayLiteral.length; i++) {
          _bytesArrayLiteral[i] = literalContexts.get(i).getLiteral().getBytesValue();
        }
        _intArrayLiteral = null;
        _longArrayLiteral = null;
        _floatArrayLiteral = null;
        _doubleArrayLiteral = null;
        _bigDecimalArrayLiteral = null;
        _stringArrayLiteral = null;
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for ArrayLiteralTransformFunction: " + _dataType + ", literal contexts: "
                + Arrays.toString(literalContexts.toArray()));
    }
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

  public String[] getStringArrayLiteral() {
    return _stringArrayLiteral;
  }

  public BigDecimal[] getBigDecimalArrayLiteral() {
    return _bigDecimalArrayLiteral;
  }

  public byte[][] getBytesArrayLiteral() {
    return _bytesArrayLiteral;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
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
          case TIMESTAMP:
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
          case BIG_DECIMAL:
            intArrayLiteral = new int[_bigDecimalArrayLiteral.length];
            for (int i = 0; i < _bigDecimalArrayLiteral.length; i++) {
              intArrayLiteral[i] = _bigDecimalArrayLiteral[i].intValue();
            }
            break;
          case STRING:
            intArrayLiteral = new int[_stringArrayLiteral.length];
            for (int i = 0; i < _stringArrayLiteral.length; i++) {
              intArrayLiteral[i] = Integer.parseInt(_stringArrayLiteral[i]);
            }
            break;
          default:
            throw new IllegalStateException("Unable to convert data type: " + _dataType + " to int array");
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
          case BOOLEAN:
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
          case BIG_DECIMAL:
            longArrayLiteral = new long[_bigDecimalArrayLiteral.length];
            for (int i = 0; i < _bigDecimalArrayLiteral.length; i++) {
              longArrayLiteral[i] = _bigDecimalArrayLiteral[i].longValue();
            }
            break;
          case STRING:
            longArrayLiteral = new long[_stringArrayLiteral.length];
            for (int i = 0; i < _stringArrayLiteral.length; i++) {
              longArrayLiteral[i] = Long.parseLong(_stringArrayLiteral[i]);
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
          case BOOLEAN:
            floatArrayLiteral = new float[_intArrayLiteral.length];
            for (int i = 0; i < _intArrayLiteral.length; i++) {
              floatArrayLiteral[i] = _intArrayLiteral[i];
            }
            break;
          case LONG:
          case TIMESTAMP:
            floatArrayLiteral = new float[_longArrayLiteral.length];
            for (int i = 0; i < _longArrayLiteral.length; i++) {
              floatArrayLiteral[i] = (float) _longArrayLiteral[i];
            }
            break;
          case DOUBLE:
            floatArrayLiteral = new float[_doubleArrayLiteral.length];
            for (int i = 0; i < _doubleArrayLiteral.length; i++) {
              floatArrayLiteral[i] = (float) _doubleArrayLiteral[i];
            }
            break;
          case BIG_DECIMAL:
            floatArrayLiteral = new float[_bigDecimalArrayLiteral.length];
            for (int i = 0; i < _bigDecimalArrayLiteral.length; i++) {
              floatArrayLiteral[i] = _bigDecimalArrayLiteral[i].floatValue();
            }
            break;
          case STRING:
            floatArrayLiteral = new float[_stringArrayLiteral.length];
            for (int i = 0; i < _stringArrayLiteral.length; i++) {
              floatArrayLiteral[i] = Float.parseFloat(_stringArrayLiteral[i]);
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
          case BOOLEAN:
            doubleArrayLiteral = new double[_intArrayLiteral.length];
            for (int i = 0; i < _intArrayLiteral.length; i++) {
              doubleArrayLiteral[i] = _intArrayLiteral[i];
            }
            break;
          case LONG:
          case TIMESTAMP:
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
          case BIG_DECIMAL:
            doubleArrayLiteral = new double[_bigDecimalArrayLiteral.length];
            for (int i = 0; i < _bigDecimalArrayLiteral.length; i++) {
              doubleArrayLiteral[i] = _bigDecimalArrayLiteral[i].doubleValue();
            }
            break;
          case STRING:
            doubleArrayLiteral = new double[_stringArrayLiteral.length];
            for (int i = 0; i < _stringArrayLiteral.length; i++) {
              doubleArrayLiteral[i] = Double.parseDouble(_stringArrayLiteral[i]);
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
    int numDocs = valueBlock.getNumDocs();
    BigDecimal[][] bigDecimalArrayResult = _bigDecimalArrayResult;
    if (bigDecimalArrayResult == null || bigDecimalArrayResult.length < numDocs) {
      bigDecimalArrayResult = new BigDecimal[numDocs][];
      BigDecimal[] bigDecimalArrayLiteral = _bigDecimalArrayLiteral;
      if (bigDecimalArrayLiteral == null) {
        switch (_dataType) {
          case INT:
          case BOOLEAN:
            bigDecimalArrayLiteral = new BigDecimal[_intArrayLiteral.length];
            for (int i = 0; i < _intArrayLiteral.length; i++) {
              bigDecimalArrayLiteral[i] = BigDecimal.valueOf(_intArrayLiteral[i]);
            }
            break;
          case LONG:
          case TIMESTAMP:
            bigDecimalArrayLiteral = new BigDecimal[_longArrayLiteral.length];
            for (int i = 0; i < _longArrayLiteral.length; i++) {
              bigDecimalArrayLiteral[i] = BigDecimal.valueOf(_longArrayLiteral[i]);
            }
            break;
          case FLOAT:
            bigDecimalArrayLiteral = new BigDecimal[_floatArrayLiteral.length];
            for (int i = 0; i < _floatArrayLiteral.length; i++) {
              bigDecimalArrayLiteral[i] = BigDecimal.valueOf(_floatArrayLiteral[i]);
            }
            break;
          case DOUBLE:
            bigDecimalArrayLiteral = new BigDecimal[_doubleArrayLiteral.length];
            for (int i = 0; i < _doubleArrayLiteral.length; i++) {
              bigDecimalArrayLiteral[i] = BigDecimal.valueOf(_doubleArrayLiteral[i]);
            }
            break;
          case STRING:
            bigDecimalArrayLiteral = new BigDecimal[_stringArrayLiteral.length];
            for (int i = 0; i < _stringArrayLiteral.length; i++) {
              bigDecimalArrayLiteral[i] = new BigDecimal(_stringArrayLiteral[i]);
            }
            break;
          default:
            throw new IllegalStateException("Unable to convert data type: " + _dataType + " to BigDecimal array");
        }
      }
      Arrays.fill(bigDecimalArrayResult, bigDecimalArrayLiteral);
      _bigDecimalArrayResult = bigDecimalArrayResult;
    }
    return bigDecimalArrayResult;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    String[][] stringArrayResult = _stringArrayResult;
    if (stringArrayResult == null || stringArrayResult.length < numDocs) {
      stringArrayResult = new String[numDocs][];
      String[] stringArrayLiteral = _stringArrayLiteral;
      if (stringArrayLiteral == null) {
        switch (_dataType) {
          case INT:
            stringArrayLiteral = new String[_intArrayLiteral.length];
            for (int i = 0; i < _intArrayLiteral.length; i++) {
              stringArrayLiteral[i] = Integer.toString(_intArrayLiteral[i]);
            }
            break;
          case LONG:
            stringArrayLiteral = new String[_longArrayLiteral.length];
            for (int i = 0; i < _longArrayLiteral.length; i++) {
              stringArrayLiteral[i] = Long.toString(_longArrayLiteral[i]);
            }
            break;
          case FLOAT:
            stringArrayLiteral = new String[_floatArrayLiteral.length];
            for (int i = 0; i < _floatArrayLiteral.length; i++) {
              stringArrayLiteral[i] = Float.toString(_floatArrayLiteral[i]);
            }
            break;
          case DOUBLE:
            stringArrayLiteral = new String[_doubleArrayLiteral.length];
            for (int i = 0; i < _doubleArrayLiteral.length; i++) {
              stringArrayLiteral[i] = Double.toString(_doubleArrayLiteral[i]);
            }
            break;
          case BIG_DECIMAL:
            stringArrayLiteral = new String[_bigDecimalArrayLiteral.length];
            for (int i = 0; i < _bigDecimalArrayLiteral.length; i++) {
              stringArrayLiteral[i] = _bigDecimalArrayLiteral[i].toPlainString();
            }
            break;
          case BOOLEAN:
            stringArrayLiteral = new String[_intArrayLiteral.length];
            for (int i = 0; i < _intArrayLiteral.length; i++) {
              stringArrayLiteral[i] = Boolean.toString(_intArrayLiteral[i] == 1);
            }
            break;
          case TIMESTAMP:
            stringArrayLiteral = new String[_longArrayLiteral.length];
            for (int i = 0; i < _longArrayLiteral.length; i++) {
              stringArrayLiteral[i] = new Timestamp(_longArrayLiteral[i]).toString();
            }
            break;
          case BYTES:
            stringArrayLiteral = new String[_bytesArrayLiteral.length];
            for (int i = 0; i < _bytesArrayLiteral.length; i++) {
              stringArrayLiteral[i] = BytesUtils.toHexString(_bytesArrayLiteral[i]);
            }
            break;
          case UUID:
            stringArrayLiteral = new String[_bytesArrayLiteral.length];
            for (int i = 0; i < _bytesArrayLiteral.length; i++) {
              stringArrayLiteral[i] = UuidUtils.toUUID(_bytesArrayLiteral[i]).toString();
            }
            break;
          default:
            throw new IllegalStateException("Unable to convert data type: " + _dataType + " to string array");
        }
      }
      Arrays.fill(stringArrayResult, stringArrayLiteral);
      _stringArrayResult = stringArrayResult;
    }
    return stringArrayResult;
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    byte[][][] bytesArrayResult = _bytesArrayResult;
    if (bytesArrayResult == null || bytesArrayResult.length < numDocs) {
      bytesArrayResult = new byte[numDocs][][];
      byte[][] bytesArrayLiteral = _bytesArrayLiteral;
      if (bytesArrayLiteral == null) {
        switch (_dataType) {
          case BIG_DECIMAL:
            bytesArrayLiteral = new byte[_bigDecimalArrayLiteral.length][];
            for (int i = 0; i < _bigDecimalArrayLiteral.length; i++) {
              bytesArrayLiteral[i] = BigDecimalUtils.serialize(_bigDecimalArrayLiteral[i]);
            }
            break;
          case STRING:
            bytesArrayLiteral = new byte[_stringArrayLiteral.length][];
            for (int i = 0; i < _stringArrayLiteral.length; i++) {
              bytesArrayLiteral[i] = BytesUtils.toBytes(_stringArrayLiteral[i]);
            }
            break;
          default:
            throw new IllegalStateException("Unable to convert data type: " + _dataType + " to bytes array");
        }
      }
      Arrays.fill(bytesArrayResult, bytesArrayLiteral);
      _bytesArrayResult = bytesArrayResult;
    }
    return bytesArrayResult;
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    // Treat all unknown type values as null regardless of the value.
    if (_dataType != DataType.UNKNOWN) {
      return null;
    }
    int length = valueBlock.getNumDocs();
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(0L, length);
    return bitmap;
  }
}
