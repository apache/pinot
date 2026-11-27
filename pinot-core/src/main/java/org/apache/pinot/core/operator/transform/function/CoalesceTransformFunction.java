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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.roaringbitmap.RoaringBitmap;


/// The `CoalesceTransformFunction` implements the Coalesce operator.
///
/// The result is first non-null value in the argument list.
/// If all arguments are null, return null.
///
/// Supports both single-value and multi-value arguments. If any argument is multi-value, the output is multi-value;
/// single-value fallback arguments are wrapped as single-element arrays for rows where earlier arguments are null,
/// while non-null empty arrays are preserved as-is.
///
/// Note: arguments have to be compatible type.
/// Number of arguments has to be greater than 0.
///
/// Expected result:
/// Coalesce(nullColumn, columnA): columnA
/// Coalesce(columnA, nullColumn): columnA
/// Coalesce(nullColumnA, nullColumnB): null
public class CoalesceTransformFunction extends BaseTransformFunction {
  private static final TransformResultMetadata UNKNOWN_MV_METADATA =
      new TransformResultMetadata(DataType.UNKNOWN, false, false);

  private TransformFunction[] _transformFunctions;
  private boolean[] _argIsSingleValue;
  private boolean[] _argIsLiteral;
  private Object[] _literalMVValues;
  private boolean _isSingleValue;
  private DataType _dataType;
  private TransformResultMetadata _resultMetadata;

  /// Returns a bit map of corresponding column. Short-circuits once a non-null argument is encountered, since
  /// subsequent arguments cannot be reached for any document in the block.
  private static RoaringBitmap[] getNullBitMaps(ValueBlock valueBlock, TransformFunction[] transformFunctions) {
    RoaringBitmap[] roaringBitmaps = new RoaringBitmap[transformFunctions.length];
    for (int i = 0; i < roaringBitmaps.length; i++) {
      RoaringBitmap nullBitmap = transformFunctions[i].getNullBitmap(valueBlock);
      if (nullBitmap == null || nullBitmap.isEmpty()) {
        break;
      }
      roaringBitmaps[i] = nullBitmap;
    }
    return roaringBitmaps;
  }

  /// Get compatible data type of left and right.
  /// When left or right is numerical, we check both data types are numerical and widen the type.
  /// Otherwise, return string type.
  ///
  /// @param left  data type
  /// @param right data type
  /// @return compatible data type.
  private static DataType getCompatibleType(DataType left, DataType right) {
    if (left.isNumeric() && right.isNumeric()) {
      if (left == DataType.BIG_DECIMAL || right == DataType.BIG_DECIMAL) {
        return DataType.BIG_DECIMAL;
      }
      if (left == DataType.DOUBLE || right == DataType.DOUBLE) {
        return DataType.DOUBLE;
      }
      if (left == DataType.FLOAT || right == DataType.FLOAT) {
        return DataType.FLOAT;
      }
      if (left == DataType.LONG || right == DataType.LONG) {
        return DataType.LONG;
      }
      return DataType.INT;
    }
    if (left == DataType.UNKNOWN) {
      return right;
    }
    if (right == DataType.UNKNOWN) {
      return left;
    }
    return DataType.STRING;
  }

  /// Get transform int results based on store type.
  private int[] getIntTransformResults(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, _transformFunctions);
    int[][] data = new int[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (data[j] == null) {
          data[j] = _transformFunctions[j].transformToIntValuesSV(valueBlock);
        }
        hasNonNullValue = true;
        _intValuesSV[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        _intValuesSV[i] = NullValuePlaceHolder.INT;
      }
    }
    return _intValuesSV;
  }

  /// Get transform long results based on store type.
  private long[] getLongTransformResults(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initLongValuesSV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, _transformFunctions);
    long[][] data = new long[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (data[j] == null) {
          data[j] = _transformFunctions[j].transformToLongValuesSV(valueBlock);
        }
        hasNonNullValue = true;
        _longValuesSV[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        _longValuesSV[i] = NullValuePlaceHolder.LONG;
      }
    }
    return _longValuesSV;
  }

  /// Get transform float results based on store type.
  private float[] getFloatTransformResults(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initFloatValuesSV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, _transformFunctions);
    float[][] data = new float[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (data[j] == null) {
          data[j] = _transformFunctions[j].transformToFloatValuesSV(valueBlock);
        }
        hasNonNullValue = true;
        _floatValuesSV[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        _floatValuesSV[i] = NullValuePlaceHolder.FLOAT;
      }
    }
    return _floatValuesSV;
  }

  /// Get transform double results based on store type.
  private double[] getDoubleTransformResults(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, _transformFunctions);
    double[][] data = new double[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (data[j] == null) {
          data[j] = _transformFunctions[j].transformToDoubleValuesSV(valueBlock);
        }
        hasNonNullValue = true;
        _doubleValuesSV[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        _doubleValuesSV[i] = NullValuePlaceHolder.DOUBLE;
      }
    }
    return _doubleValuesSV;
  }

  /// Get transform BigDecimal results based on store type.
  private BigDecimal[] getBigDecimalTransformResults(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initBigDecimalValuesSV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, _transformFunctions);
    BigDecimal[][] data = new BigDecimal[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (data[j] == null) {
          data[j] = _transformFunctions[j].transformToBigDecimalValuesSV(valueBlock);
        }
        hasNonNullValue = true;
        _bigDecimalValuesSV[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        _bigDecimalValuesSV[i] = NullValuePlaceHolder.BIG_DECIMAL;
      }
    }
    return _bigDecimalValuesSV;
  }

  /// Get transform String results based on store type.
  private String[] getStringTransformResults(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initStringValuesSV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, _transformFunctions);
    String[][] data = new String[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (data[j] == null) {
          data[j] = _transformFunctions[j].transformToStringValuesSV(valueBlock);
        }
        hasNonNullValue = true;
        _stringValuesSV[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        _stringValuesSV[i] = NullValuePlaceHolder.STRING;
      }
    }
    return _stringValuesSV;
  }

  /// Get transform int MV results based on store type.
  private int[][] getIntTransformResultsMV(ValueBlock valueBlock) {
    RoaringBitmap firstNullBitmap = _transformFunctions[0].getNullBitmap(valueBlock);
    if ((firstNullBitmap == null || firstNullBitmap.isEmpty()) && !_argIsSingleValue[0]) {
      return _transformFunctions[0].transformToIntValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initIntValuesMV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, firstNullBitmap);
    int[][][] mvData = new int[width][][];
    int[][] svData = new int[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!_argIsSingleValue[j]) {
          if (mvData[j] == null) {
            mvData[j] = _transformFunctions[j].transformToIntValuesMV(valueBlock);
          }
          _intValuesMV[i] = mvData[j][i];
        } else if (_argIsLiteral[j]) {
          int[] literalValue = (int[]) _literalMVValues[j];
          if (literalValue == null) {
            literalValue = new int[]{((LiteralTransformFunction) _transformFunctions[j]).getIntLiteral()};
            _literalMVValues[j] = literalValue;
          }
          _intValuesMV[i] = literalValue;
        } else {
          if (svData[j] == null) {
            svData[j] = _transformFunctions[j].transformToIntValuesSV(valueBlock);
          }
          _intValuesMV[i] = new int[]{svData[j][i]};
        }
        hasNonNullValue = true;
        break;
      }
      if (!hasNonNullValue) {
        _intValuesMV[i] = NullValuePlaceHolder.INT_ARRAY;
      }
    }
    return _intValuesMV;
  }

  /// Get transform long MV results based on store type.
  private long[][] getLongTransformResultsMV(ValueBlock valueBlock) {
    RoaringBitmap firstNullBitmap = _transformFunctions[0].getNullBitmap(valueBlock);
    if ((firstNullBitmap == null || firstNullBitmap.isEmpty()) && !_argIsSingleValue[0]) {
      return _transformFunctions[0].transformToLongValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initLongValuesMV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, firstNullBitmap);
    long[][][] mvData = new long[width][][];
    long[][] svData = new long[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!_argIsSingleValue[j]) {
          if (mvData[j] == null) {
            mvData[j] = _transformFunctions[j].transformToLongValuesMV(valueBlock);
          }
          _longValuesMV[i] = mvData[j][i];
        } else if (_argIsLiteral[j]) {
          long[] literalValue = (long[]) _literalMVValues[j];
          if (literalValue == null) {
            literalValue = new long[]{((LiteralTransformFunction) _transformFunctions[j]).getLongLiteral()};
            _literalMVValues[j] = literalValue;
          }
          _longValuesMV[i] = literalValue;
        } else {
          if (svData[j] == null) {
            svData[j] = _transformFunctions[j].transformToLongValuesSV(valueBlock);
          }
          _longValuesMV[i] = new long[]{svData[j][i]};
        }
        hasNonNullValue = true;
        break;
      }
      if (!hasNonNullValue) {
        _longValuesMV[i] = NullValuePlaceHolder.LONG_ARRAY;
      }
    }
    return _longValuesMV;
  }

  /// Get transform float MV results based on store type.
  private float[][] getFloatTransformResultsMV(ValueBlock valueBlock) {
    RoaringBitmap firstNullBitmap = _transformFunctions[0].getNullBitmap(valueBlock);
    if ((firstNullBitmap == null || firstNullBitmap.isEmpty()) && !_argIsSingleValue[0]) {
      return _transformFunctions[0].transformToFloatValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initFloatValuesMV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, firstNullBitmap);
    float[][][] mvData = new float[width][][];
    float[][] svData = new float[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!_argIsSingleValue[j]) {
          if (mvData[j] == null) {
            mvData[j] = _transformFunctions[j].transformToFloatValuesMV(valueBlock);
          }
          _floatValuesMV[i] = mvData[j][i];
        } else if (_argIsLiteral[j]) {
          float[] literalValue = (float[]) _literalMVValues[j];
          if (literalValue == null) {
            literalValue = new float[]{((LiteralTransformFunction) _transformFunctions[j]).getFloatLiteral()};
            _literalMVValues[j] = literalValue;
          }
          _floatValuesMV[i] = literalValue;
        } else {
          if (svData[j] == null) {
            svData[j] = _transformFunctions[j].transformToFloatValuesSV(valueBlock);
          }
          _floatValuesMV[i] = new float[]{svData[j][i]};
        }
        hasNonNullValue = true;
        break;
      }
      if (!hasNonNullValue) {
        _floatValuesMV[i] = NullValuePlaceHolder.FLOAT_ARRAY;
      }
    }
    return _floatValuesMV;
  }

  /// Get transform double MV results based on store type.
  private double[][] getDoubleTransformResultsMV(ValueBlock valueBlock) {
    RoaringBitmap firstNullBitmap = _transformFunctions[0].getNullBitmap(valueBlock);
    if ((firstNullBitmap == null || firstNullBitmap.isEmpty()) && !_argIsSingleValue[0]) {
      return _transformFunctions[0].transformToDoubleValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initDoubleValuesMV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, firstNullBitmap);
    double[][][] mvData = new double[width][][];
    double[][] svData = new double[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!_argIsSingleValue[j]) {
          if (mvData[j] == null) {
            mvData[j] = _transformFunctions[j].transformToDoubleValuesMV(valueBlock);
          }
          _doubleValuesMV[i] = mvData[j][i];
        } else if (_argIsLiteral[j]) {
          double[] literalValue = (double[]) _literalMVValues[j];
          if (literalValue == null) {
            literalValue = new double[]{((LiteralTransformFunction) _transformFunctions[j]).getDoubleLiteral()};
            _literalMVValues[j] = literalValue;
          }
          _doubleValuesMV[i] = literalValue;
        } else {
          if (svData[j] == null) {
            svData[j] = _transformFunctions[j].transformToDoubleValuesSV(valueBlock);
          }
          _doubleValuesMV[i] = new double[]{svData[j][i]};
        }
        hasNonNullValue = true;
        break;
      }
      if (!hasNonNullValue) {
        _doubleValuesMV[i] = NullValuePlaceHolder.DOUBLE_ARRAY;
      }
    }
    return _doubleValuesMV;
  }

  /// Get transform BigDecimal MV results based on store type.
  private BigDecimal[][] getBigDecimalTransformResultsMV(ValueBlock valueBlock) {
    RoaringBitmap firstNullBitmap = _transformFunctions[0].getNullBitmap(valueBlock);
    if ((firstNullBitmap == null || firstNullBitmap.isEmpty()) && !_argIsSingleValue[0]) {
      return _transformFunctions[0].transformToBigDecimalValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initBigDecimalValuesMV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, firstNullBitmap);
    BigDecimal[][][] mvData = new BigDecimal[width][][];
    BigDecimal[][] svData = new BigDecimal[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!_argIsSingleValue[j]) {
          if (mvData[j] == null) {
            mvData[j] = _transformFunctions[j].transformToBigDecimalValuesMV(valueBlock);
          }
          _bigDecimalValuesMV[i] = mvData[j][i];
        } else if (_argIsLiteral[j]) {
          BigDecimal[] literalValue = (BigDecimal[]) _literalMVValues[j];
          if (literalValue == null) {
            literalValue =
                new BigDecimal[]{((LiteralTransformFunction) _transformFunctions[j]).getBigDecimalLiteral()};
            _literalMVValues[j] = literalValue;
          }
          _bigDecimalValuesMV[i] = literalValue;
        } else {
          if (svData[j] == null) {
            svData[j] = _transformFunctions[j].transformToBigDecimalValuesSV(valueBlock);
          }
          _bigDecimalValuesMV[i] = new BigDecimal[]{svData[j][i]};
        }
        hasNonNullValue = true;
        break;
      }
      if (!hasNonNullValue) {
        _bigDecimalValuesMV[i] = NullValuePlaceHolder.BIG_DECIMAL_ARRAY;
      }
    }
    return _bigDecimalValuesMV;
  }

  /// Get transform String MV results based on store type.
  private String[][] getStringTransformResultsMV(ValueBlock valueBlock) {
    RoaringBitmap firstNullBitmap = _transformFunctions[0].getNullBitmap(valueBlock);
    if ((firstNullBitmap == null || firstNullBitmap.isEmpty()) && !_argIsSingleValue[0]) {
      return _transformFunctions[0].transformToStringValuesMV(valueBlock);
    }
    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, firstNullBitmap);
    String[][][] mvData = new String[width][][];
    String[][] svData = new String[width][];
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!_argIsSingleValue[j]) {
          if (mvData[j] == null) {
            mvData[j] = _transformFunctions[j].transformToStringValuesMV(valueBlock);
          }
          _stringValuesMV[i] = mvData[j][i];
        } else if (_argIsLiteral[j]) {
          String[] literalValue = (String[]) _literalMVValues[j];
          if (literalValue == null) {
            literalValue = new String[]{((LiteralTransformFunction) _transformFunctions[j]).getStringLiteral()};
            _literalMVValues[j] = literalValue;
          }
          _stringValuesMV[i] = literalValue;
        } else {
          if (svData[j] == null) {
            svData[j] = _transformFunctions[j].transformToStringValuesSV(valueBlock);
          }
          _stringValuesMV[i] = new String[]{svData[j][i]};
        }
        hasNonNullValue = true;
        break;
      }
      if (!hasNonNullValue) {
        _stringValuesMV[i] = NullValuePlaceHolder.STRING_ARRAY;
      }
    }
    return _stringValuesMV;
  }

  private RoaringBitmap[] getNullBitMaps(ValueBlock valueBlock, @Nullable RoaringBitmap firstNullBitmap) {
    RoaringBitmap[] nullBitMaps = new RoaringBitmap[_transformFunctions.length];
    if (firstNullBitmap == null || firstNullBitmap.isEmpty()) {
      return nullBitMaps;
    }
    nullBitMaps[0] = firstNullBitmap;
    for (int i = 1; i < nullBitMaps.length; i++) {
      RoaringBitmap nullBitmap = _transformFunctions[i].getNullBitmap(valueBlock);
      if (nullBitmap == null || nullBitmap.isEmpty()) {
        break;
      }
      nullBitMaps[i] = nullBitmap;
    }
    return nullBitMaps;
  }

  @Override
  public String getName() {
    return TransformFunctionType.COALESCE.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    int argSize = arguments.size();
    Preconditions.checkArgument(argSize > 0, "COALESCE needs to have at least one argument.");
    _transformFunctions = new TransformFunction[argSize];
    _argIsSingleValue = new boolean[argSize];
    _argIsLiteral = new boolean[argSize];
    _isSingleValue = true;
    for (int i = 0; i < argSize; i++) {
      TransformFunction func = arguments.get(i);
      TransformResultMetadata metadata = func.getResultMetadata();
      boolean isSingleValue = metadata.isSingleValue();
      _argIsSingleValue[i] = isSingleValue;
      _argIsLiteral[i] = func instanceof LiteralTransformFunction;
      if (!isSingleValue) {
        _isSingleValue = false;
      }
      DataType dataType = metadata.getDataType();
      // A whole OPEN_STRUCT argument supplies its JSON document through transformToStringValuesSV.
      if (dataType == DataType.OPEN_STRUCT) {
        dataType = DataType.STRING;
      }
      if (_dataType != null) {
        _dataType = getCompatibleType(_dataType, dataType);
      } else {
        _dataType = dataType;
      }
      _transformFunctions[i] = func;
    }
    if (!_isSingleValue) {
      _literalMVValues = new Object[argSize];
    }
    // TODO: Support BOOLEAN, TIMESTAMP, BYTES
    switch (_dataType) {
      case INT:
        _resultMetadata = _isSingleValue ? INT_SV_NO_DICTIONARY_METADATA : INT_MV_NO_DICTIONARY_METADATA;
        break;
      case LONG:
        _resultMetadata = _isSingleValue ? LONG_SV_NO_DICTIONARY_METADATA : LONG_MV_NO_DICTIONARY_METADATA;
        break;
      case FLOAT:
        _resultMetadata = _isSingleValue ? FLOAT_SV_NO_DICTIONARY_METADATA : FLOAT_MV_NO_DICTIONARY_METADATA;
        break;
      case DOUBLE:
        _resultMetadata = _isSingleValue ? DOUBLE_SV_NO_DICTIONARY_METADATA : DOUBLE_MV_NO_DICTIONARY_METADATA;
        break;
      case BIG_DECIMAL:
        _resultMetadata = _isSingleValue
            ? BIG_DECIMAL_SV_NO_DICTIONARY_METADATA
            : BIG_DECIMAL_MV_NO_DICTIONARY_METADATA;
        break;
      case STRING:
        _resultMetadata = _isSingleValue ? STRING_SV_NO_DICTIONARY_METADATA : STRING_MV_NO_DICTIONARY_METADATA;
        break;
      case UNKNOWN:
        _resultMetadata = _isSingleValue ? UNKNOWN_METADATA : UNKNOWN_MV_METADATA;
        break;
      default:
        throw new UnsupportedOperationException("Coalesce only supports numerical and string data type");
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    if (!_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.INT) {
      return super.transformToIntValuesSV(valueBlock);
    }
    return getIntTransformResults(valueBlock);
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (!_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    return getLongTransformResults(valueBlock);
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (!_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    return getFloatTransformResults(valueBlock);
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (!_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    return getDoubleTransformResults(valueBlock);
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (!_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
    return getBigDecimalTransformResults(valueBlock);
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (!_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    return getStringTransformResults(valueBlock);
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    if (_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.INT) {
      return super.transformToIntValuesMV(valueBlock);
    }
    return getIntTransformResultsMV(valueBlock);
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    if (_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.LONG) {
      return super.transformToLongValuesMV(valueBlock);
    }
    return getLongTransformResultsMV(valueBlock);
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    if (_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.FLOAT) {
      return super.transformToFloatValuesMV(valueBlock);
    }
    return getFloatTransformResultsMV(valueBlock);
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    if (_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.DOUBLE) {
      return super.transformToDoubleValuesMV(valueBlock);
    }
    return getDoubleTransformResultsMV(valueBlock);
  }

  @Override
  public BigDecimal[][] transformToBigDecimalValuesMV(ValueBlock valueBlock) {
    if (_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesMV(valueBlock);
    }
    return getBigDecimalTransformResultsMV(valueBlock);
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    if (_isSingleValue) {
      throw new UnsupportedOperationException();
    }
    if (_dataType != DataType.STRING) {
      return super.transformToStringValuesMV(valueBlock);
    }
    return getStringTransformResultsMV(valueBlock);
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    RoaringBitmap firstBitmap = _transformFunctions[0].getNullBitmap(valueBlock);
    if (firstBitmap == null || firstBitmap.isEmpty()) {
      return null;
    }
    if (_transformFunctions.length == 1) {
      return firstBitmap.clone();
    }
    RoaringBitmap result = null;
    for (int i = 1; i < _transformFunctions.length; i++) {
      RoaringBitmap curBitmap = _transformFunctions[i].getNullBitmap(valueBlock);
      if (curBitmap == null || curBitmap.isEmpty()) {
        return null;
      }
      if (result == null) {
        result = RoaringBitmap.and(firstBitmap, curBitmap);
      } else {
        result.and(curBitmap);
      }
      if (result.isEmpty()) {
        return null;
      }
    }
    return result;
  }
}
