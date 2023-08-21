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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.utils.ArrayCopyUtils;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.roaringbitmap.RoaringBitmap;


/**
 * Base class for transform function providing the default implementation for all data types.
 */
public abstract class BaseTransformFunction implements TransformFunction {
  protected static final TransformResultMetadata INT_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.INT, true, false);
  protected static final TransformResultMetadata LONG_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.LONG, true, false);
  protected static final TransformResultMetadata FLOAT_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.FLOAT, true, false);
  protected static final TransformResultMetadata DOUBLE_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.DOUBLE, true, false);
  protected static final TransformResultMetadata BIG_DECIMAL_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BIG_DECIMAL, true, false);
  protected static final TransformResultMetadata BOOLEAN_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BOOLEAN, true, false);
  protected static final TransformResultMetadata TIMESTAMP_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.TIMESTAMP, true, false);
  protected static final TransformResultMetadata STRING_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.STRING, true, false);
  protected static final TransformResultMetadata JSON_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.JSON, true, false);
  protected static final TransformResultMetadata BYTES_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BYTES, true, false);
  protected static final TransformResultMetadata VECTOR_SV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.VECTOR, true, false);

  protected static final TransformResultMetadata INT_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.INT, false, false);
  protected static final TransformResultMetadata LONG_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.LONG, false, false);
  protected static final TransformResultMetadata FLOAT_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.FLOAT, false, false);
  protected static final TransformResultMetadata DOUBLE_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.DOUBLE, false, false);
  // TODO: Support MV BIG_DECIMAL
  protected static final TransformResultMetadata BIG_DECIMAL_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BIG_DECIMAL, false, false);
  protected static final TransformResultMetadata BOOLEAN_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BOOLEAN, false, false);
  protected static final TransformResultMetadata TIMESTAMP_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.TIMESTAMP, false, false);
  protected static final TransformResultMetadata STRING_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.STRING, false, false);
  protected static final TransformResultMetadata JSON_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.JSON, false, false);
  protected static final TransformResultMetadata BYTES_MV_NO_DICTIONARY_METADATA =
      new TransformResultMetadata(DataType.BYTES, false, false);
  protected static final TransformResultMetadata UNKNOWN_METADATA =
      new TransformResultMetadata(DataType.UNKNOWN, true, false);

  // These buffers are used to hold the result for different result types. When the subclass overrides a method, it can
  // reuse the buffer for that method. E.g. if transformToIntValuesSV is overridden, the result can be written into
  // _intValuesSV.
  protected int[] _intValuesSV;
  protected long[] _longValuesSV;
  protected float[] _floatValuesSV;
  protected double[] _doubleValuesSV;
  protected BigDecimal[] _bigDecimalValuesSV;
  protected Vector[] _vectorValuesSV;
  protected String[] _stringValuesSV;
  protected byte[][] _bytesValuesSV;
  protected int[][] _intValuesMV;
  protected long[][] _longValuesMV;
  protected float[][] _floatValuesMV;
  protected double[][] _doubleValuesMV;
  protected String[][] _stringValuesMV;
  protected byte[][][] _bytesValuesMV;

  protected List<TransformFunction> _arguments;
  protected boolean _nullHandlingEnabled;

  protected void fillResultUnknown(int length) {
    for (int i = 0; i < length; i++) {
      _intValuesSV[i] = NullValuePlaceHolder.INT;
    }
  }

  // NOTE: this init has to be called for default getNullBitmap() implementation to be effective.
  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    _arguments = arguments;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    init(arguments, columnContextMap);
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public int[] transformToDictIdsSV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int[][] transformToDictIdsMV(ValueBlock valueBlock) {
    throw new UnsupportedOperationException();
  }

  protected void initIntValuesSV(int length) {
    if (_intValuesSV == null || _intValuesSV.length < length) {
      _intValuesSV = new int[length];
    }
  }

  protected void initZeroFillingIntValuesSV(int length) {
    if (_intValuesSV == null || _intValuesSV.length < length) {
      _intValuesSV = new int[length];
    } else {
      Arrays.fill(_intValuesSV, 0, length, 0);
    }
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(valueBlock);
      dictionary.readIntValues(dictIds, length, _intValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case LONG:
          long[] longValues = transformToLongValuesSV(valueBlock);
          ArrayCopyUtils.copy(longValues, _intValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(valueBlock);
          ArrayCopyUtils.copy(floatValues, _intValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(valueBlock);
          ArrayCopyUtils.copy(doubleValues, _intValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(valueBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _intValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(valueBlock);
          ArrayCopyUtils.copy(stringValues, _intValuesSV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _intValuesSV[i] = NullValuePlaceHolder.INT;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as INT", resultDataType));
      }
    }
    return _intValuesSV;
  }

  protected void initLongValuesSV(int length) {
    if (_longValuesSV == null || _longValuesSV.length < length) {
      _longValuesSV = new long[length];
    }
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initLongValuesSV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(valueBlock);
      dictionary.readLongValues(dictIds, length, _longValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case INT:
          int[] intValues = transformToIntValuesSV(valueBlock);
          ArrayCopyUtils.copy(intValues, _longValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(valueBlock);
          ArrayCopyUtils.copy(floatValues, _longValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(valueBlock);
          ArrayCopyUtils.copy(doubleValues, _longValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(valueBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _longValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(valueBlock);
          ArrayCopyUtils.copy(stringValues, _longValuesSV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _longValuesSV[i] = NullValuePlaceHolder.LONG;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as LONG", resultDataType));
      }
    }
    return _longValuesSV;
  }

  protected void initFloatValuesSV(int length) {
    if (_floatValuesSV == null || _floatValuesSV.length < length) {
      _floatValuesSV = new float[length];
    }
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initFloatValuesSV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(valueBlock);
      dictionary.readFloatValues(dictIds, length, _floatValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case INT:
          int[] intValues = transformToIntValuesSV(valueBlock);
          ArrayCopyUtils.copy(intValues, _floatValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(valueBlock);
          ArrayCopyUtils.copy(longValues, _floatValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(valueBlock);
          ArrayCopyUtils.copy(doubleValues, _floatValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(valueBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _floatValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(valueBlock);
          ArrayCopyUtils.copy(stringValues, _floatValuesSV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _floatValuesSV[i] = NullValuePlaceHolder.FLOAT;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as FLOAT", resultDataType));
      }
    }
    return _floatValuesSV;
  }

  protected void initDoubleValuesSV(int length) {
    if (_doubleValuesSV == null || _doubleValuesSV.length < length) {
      _doubleValuesSV = new double[length];
    }
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(valueBlock);
      dictionary.readDoubleValues(dictIds, length, _doubleValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case INT:
          int[] intValues = transformToIntValuesSV(valueBlock);
          ArrayCopyUtils.copy(intValues, _doubleValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(valueBlock);
          ArrayCopyUtils.copy(longValues, _doubleValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(valueBlock);
          ArrayCopyUtils.copy(floatValues, _doubleValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(valueBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _doubleValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(valueBlock);
          ArrayCopyUtils.copy(stringValues, _doubleValuesSV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _doubleValuesSV[i] = NullValuePlaceHolder.DOUBLE;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as DOUBLE", resultDataType));
      }
    }
    return _doubleValuesSV;
  }

  protected void initBigDecimalValuesSV(int length) {
    if (_bigDecimalValuesSV == null || _bigDecimalValuesSV.length < length) {
      _bigDecimalValuesSV = new BigDecimal[length];
    }
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initBigDecimalValuesSV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(valueBlock);
      dictionary.readBigDecimalValues(dictIds, length, _bigDecimalValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case INT:
          int[] intValues = transformToIntValuesSV(valueBlock);
          ArrayCopyUtils.copy(intValues, _bigDecimalValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(valueBlock);
          ArrayCopyUtils.copy(longValues, _bigDecimalValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(valueBlock);
          ArrayCopyUtils.copy(floatValues, _bigDecimalValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(valueBlock);
          ArrayCopyUtils.copy(doubleValues, _bigDecimalValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(valueBlock);
          ArrayCopyUtils.copy(stringValues, _bigDecimalValuesSV, length);
          break;
        case BYTES:
          byte[][] bytesValues = transformToBytesValuesSV(valueBlock);
          ArrayCopyUtils.copy(bytesValues, _bigDecimalValuesSV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _bigDecimalValuesSV[i] = NullValuePlaceHolder.BIG_DECIMAL;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as BIG_DECIMAL", resultDataType));
      }
    }
    return _bigDecimalValuesSV;
  }

  protected void initStringValuesSV(int length) {
    if (_stringValuesSV == null || _stringValuesSV.length < length) {
      _stringValuesSV = new String[length];
    }
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initStringValuesSV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(valueBlock);
      dictionary.readStringValues(dictIds, length, _stringValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case INT:
          int[] intValues = transformToIntValuesSV(valueBlock);
          ArrayCopyUtils.copy(intValues, _stringValuesSV, length);
          break;
        case LONG:
          long[] longValues = transformToLongValuesSV(valueBlock);
          ArrayCopyUtils.copy(longValues, _stringValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = transformToFloatValuesSV(valueBlock);
          ArrayCopyUtils.copy(floatValues, _stringValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = transformToDoubleValuesSV(valueBlock);
          ArrayCopyUtils.copy(doubleValues, _stringValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(valueBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _stringValuesSV, length);
          break;
        case BYTES:
          byte[][] bytesValues = transformToBytesValuesSV(valueBlock);
          ArrayCopyUtils.copy(bytesValues, _stringValuesSV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _stringValuesSV[i] = NullValuePlaceHolder.STRING;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as STRING", resultDataType));
      }
    }
    return _stringValuesSV;
  }

  protected void initBytesValuesSV(int length) {
    if (_bytesValuesSV == null || _bytesValuesSV.length < length) {
      _bytesValuesSV = new byte[length][];
    }
  }

  @Override
  public byte[][] transformToBytesValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initBytesValuesSV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(valueBlock);
      dictionary.readBytesValues(dictIds, length, _bytesValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = transformToBigDecimalValuesSV(valueBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _bytesValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(valueBlock);
          ArrayCopyUtils.copy(stringValues, _bytesValuesSV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _bytesValuesSV[i] = NullValuePlaceHolder.BYTES;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as BYTES", resultDataType));
      }
    }
    return _bytesValuesSV;
  }

  protected void initVectorValuesSV(int length) {
    if (_vectorValuesSV == null || _vectorValuesSV.length < length) {
      _vectorValuesSV = new Vector[length];
    }
  }

  @Override
  public Vector[] transformToVectorValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initVectorValuesSV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[] dictIds = transformToDictIdsSV(valueBlock);
      dictionary.readVectorValues(dictIds, length, _vectorValuesSV);
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case BYTES:
          byte[][] byteValues = transformToBytesValuesSV(valueBlock);
          ArrayCopyUtils.copy(byteValues, _vectorValuesSV, length);
          break;
        case STRING:
          String[] stringValues = transformToStringValuesSV(valueBlock);
          ArrayCopyUtils.copy(stringValues, _vectorValuesSV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _vectorValuesSV[i] = (Vector) DataSchema.ColumnDataType.VECTOR.getNullPlaceholder();
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read SV %s as VECTOR", resultDataType));
      }
    }
    return _vectorValuesSV;
  }

  protected void initIntValuesMV(int length) {
    if (_intValuesMV == null || _intValuesMV.length < length) {
      _intValuesMV = new int[length][];
    }
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesMV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(valueBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        int[] intValues = new int[numValues];
        dictionary.readIntValues(dictIds, numValues, intValues);
        _intValuesMV[i] = intValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(valueBlock);
          ArrayCopyUtils.copy(longValuesMV, _intValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(valueBlock);
          ArrayCopyUtils.copy(floatValuesMV, _intValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(valueBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _intValuesMV, length);
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(valueBlock);
          ArrayCopyUtils.copy(stringValuesMV, _intValuesMV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _intValuesMV[i] = NullValuePlaceHolder.INT_ARRAY;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as INT", resultDataType));
      }
    }
    return _intValuesMV;
  }

  protected void initLongValuesMV(int length) {
    if (_longValuesMV == null || _longValuesMV.length < length) {
      _longValuesMV = new long[length][];
    }
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initLongValuesMV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(valueBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        long[] longValues = new long[numValues];
        dictionary.readLongValues(dictIds, numValues, longValues);
        _longValuesMV[i] = longValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(valueBlock);
          ArrayCopyUtils.copy(intValuesMV, _longValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(valueBlock);
          ArrayCopyUtils.copy(floatValuesMV, _longValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(valueBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _longValuesMV, length);
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(valueBlock);
          ArrayCopyUtils.copy(stringValuesMV, _longValuesMV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _longValuesMV[i] = NullValuePlaceHolder.LONG_ARRAY;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as LONG", resultDataType));
      }
    }
    return _longValuesMV;
  }

  protected void initFloatValuesMV(int length) {
    if (_floatValuesMV == null || _floatValuesMV.length < length) {
      _floatValuesMV = new float[length][];
    }
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initFloatValuesMV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(valueBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        float[] floatValues = new float[numValues];
        dictionary.readFloatValues(dictIds, numValues, floatValues);
        _floatValuesMV[i] = floatValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(valueBlock);
          ArrayCopyUtils.copy(intValuesMV, _floatValuesMV, length);
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(valueBlock);
          ArrayCopyUtils.copy(longValuesMV, _floatValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(valueBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _floatValuesMV, length);
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(valueBlock);
          ArrayCopyUtils.copy(stringValuesMV, _floatValuesMV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _floatValuesMV[i] = NullValuePlaceHolder.FLOAT_ARRAY;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as FLOAT", resultDataType));
      }
    }
    return _floatValuesMV;
  }

  protected void initDoubleValuesMV(int length) {
    if (_doubleValuesMV == null || _doubleValuesMV.length < length) {
      _doubleValuesMV = new double[length][];
    }
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesMV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(valueBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        double[] doubleValues = new double[numValues];
        dictionary.readDoubleValues(dictIds, numValues, doubleValues);
        _doubleValuesMV[i] = doubleValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType.getStoredType()) {
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(valueBlock);
          ArrayCopyUtils.copy(intValuesMV, _doubleValuesMV, length);
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(valueBlock);
          ArrayCopyUtils.copy(longValuesMV, _doubleValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(valueBlock);
          ArrayCopyUtils.copy(floatValuesMV, _doubleValuesMV, length);
          break;
        case STRING:
          String[][] stringValuesMV = transformToStringValuesMV(valueBlock);
          ArrayCopyUtils.copy(stringValuesMV, _doubleValuesMV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _doubleValuesMV[i] = NullValuePlaceHolder.DOUBLE_ARRAY;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as DOUBLE", resultDataType));
      }
    }
    return _doubleValuesMV;
  }

  protected void initStringValuesMV(int length) {
    if (_stringValuesMV == null || _stringValuesMV.length < length) {
      _stringValuesMV = new String[length][];
    }
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(valueBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        String[] stringValues = new String[numValues];
        dictionary.readStringValues(dictIds, numValues, stringValues);
        _stringValuesMV[i] = stringValues;
      }
    } else {
      DataType resultDataType = getResultMetadata().getDataType();
      switch (resultDataType) {
        case INT:
          int[][] intValuesMV = transformToIntValuesMV(valueBlock);
          ArrayCopyUtils.copy(intValuesMV, _stringValuesMV, length);
          break;
        case LONG:
          long[][] longValuesMV = transformToLongValuesMV(valueBlock);
          ArrayCopyUtils.copy(longValuesMV, _stringValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = transformToFloatValuesMV(valueBlock);
          ArrayCopyUtils.copy(floatValuesMV, _stringValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = transformToDoubleValuesMV(valueBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _stringValuesMV, length);
          break;
        case UNKNOWN:
          // Copy the values to ensure behaviour consistency with non null-handling.
          for (int i = 0; i < length; i++) {
            _stringValuesMV[i] = NullValuePlaceHolder.STRING_ARRAY;
          }
          break;
        default:
          throw new IllegalStateException(String.format("Cannot read MV %s as STRING", resultDataType));
      }
    }
    return _stringValuesMV;
  }

  protected void initBytesValuesMV(int length) {
    if (_bytesValuesMV == null || _bytesValuesMV.length < length) {
      _bytesValuesMV = new byte[length][][];
    }
  }

  @Override
  public byte[][][] transformToBytesValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initBytesValuesMV(length);
    Dictionary dictionary = getDictionary();
    if (dictionary != null) {
      int[][] dictIdsMV = transformToDictIdsMV(valueBlock);
      for (int i = 0; i < length; i++) {
        int[] dictIds = dictIdsMV[i];
        int numValues = dictIds.length;
        byte[][] bytesValues = new byte[numValues][];
        dictionary.readBytesValues(dictIds, numValues, bytesValues);
        _bytesValuesMV[i] = bytesValues;
      }
    } else {
      assert getResultMetadata().getDataType().getStoredType() == DataType.STRING;
      String[][] stringValuesMV = transformToStringValuesMV(valueBlock);
      ArrayCopyUtils.copy(stringValuesMV, _bytesValuesMV, length);
    }
    return _bytesValuesMV;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    // TODO: _arguments shouldn't be null if all the transform functions call the init().
    if (_arguments == null) {
      return null;
    }
    RoaringBitmap bitmap = new RoaringBitmap();
    for (TransformFunction arg : _arguments) {
      RoaringBitmap argBitmap = arg.getNullBitmap(valueBlock);
      if (argBitmap != null) {
        bitmap.or(argBitmap);
      }
    }
    if (bitmap.isEmpty()) {
      return null;
    }
    return bitmap;
  }
}
