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
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>CoalesceTransformFunction</code> implements the Coalesce operator.
 *
 * The result is first non-null value in the argument list.
 * If all arguments are null, return null.
 *
 * Note: arguments have to be compatible type.
 * Number of arguments has to be greater than 0.
 *
 * Expected result:
 * Coalesce(nullColumn, columnA): columnA
 * Coalesce(columnA, nullColumn): columnA
 * Coalesce(nullColumnA, nullColumnB): null
 *
 */
public class CoalesceTransformFunction extends BaseTransformFunction {
  private TransformFunction[] _transformFunctions;
  private DataType _dataType;
  private TransformResultMetadata _resultMetadata;

  /**
   * Returns a bit map of corresponding column. Returns an empty bitmap by default if null option is disabled.
   */
  private static RoaringBitmap[] getNullBitMaps(ValueBlock valueBlock, TransformFunction[] transformFunctions) {
    RoaringBitmap[] roaringBitmaps = new RoaringBitmap[transformFunctions.length];
    for (int i = 0; i < roaringBitmaps.length; i++) {
      TransformFunction func = transformFunctions[i];
      roaringBitmaps[i] = func.getNullBitmap(valueBlock);
    }
    return roaringBitmaps;
  }

  /**
   * Get compatible data type of left and right.
   * When left or right is numerical, we check both data types are numerical and widen the type.
   * Otherwise, return string type.
   *
   * @param left  data type
   * @param right data type
   * @return compatible data type.
   */
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

  /**
   * Get transform int results based on store type.
   */
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
        _intValuesSV[i] = (int) DataSchema.ColumnDataType.INT.getNullPlaceholder();
      }
    }
    return _intValuesSV;
  }

  /**
   * Get transform long results based on store type.
   */
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
        _longValuesSV[i] = (long) DataSchema.ColumnDataType.LONG.getNullPlaceholder();
      }
    }
    return _longValuesSV;
  }

  /**
   * Get transform float results based on store type.
   */
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
        _floatValuesSV[i] = (float) DataSchema.ColumnDataType.FLOAT.getNullPlaceholder();
      }
    }
    return _floatValuesSV;
  }

  /**
   * Get transform double results based on store type.
   */
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
        _doubleValuesSV[i] = (double) DataSchema.ColumnDataType.DOUBLE.getNullPlaceholder();
      }
    }
    return _doubleValuesSV;
  }

  /**
   * Get transform BigDecimal results based on store type.
   */
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
        _bigDecimalValuesSV[i] = (BigDecimal) DataSchema.ColumnDataType.BIG_DECIMAL.getNullPlaceholder();
      }
    }
    return _bigDecimalValuesSV;
  }

  /**
   * Get transform String results based on store type.
   */
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
        _stringValuesSV[i] = (String) DataSchema.ColumnDataType.STRING.getNullPlaceholder();
      }
    }
    return _stringValuesSV;
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
    for (int i = 0; i < argSize; i++) {
      TransformFunction func = arguments.get(i);
      DataType dataType = func.getResultMetadata().getDataType();
      if (_dataType != null) {
        _dataType = getCompatibleType(_dataType, dataType);
      } else {
        _dataType = dataType;
      }
      _transformFunctions[i] = func;
    }
    // TODO: Support BOOLEAN, TIMESTAMP, BYTES
    switch (_dataType) {
      case INT:
        _resultMetadata = INT_SV_NO_DICTIONARY_METADATA;
        break;
      case LONG:
        _resultMetadata = LONG_SV_NO_DICTIONARY_METADATA;
        break;
      case FLOAT:
        _resultMetadata = FLOAT_SV_NO_DICTIONARY_METADATA;
        break;
      case DOUBLE:
        _resultMetadata = DOUBLE_SV_NO_DICTIONARY_METADATA;
        break;
      case BIG_DECIMAL:
        _resultMetadata = BIG_DECIMAL_SV_NO_DICTIONARY_METADATA;
        break;
      case STRING:
        _resultMetadata = STRING_SV_NO_DICTIONARY_METADATA;
        break;
      case UNKNOWN:
        _resultMetadata = UNKNOWN_METADATA;
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
    if (_dataType != DataType.INT) {
      return super.transformToIntValuesSV(valueBlock);
    }
    return getIntTransformResults(valueBlock);
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    if (_dataType != DataType.LONG) {
      return super.transformToLongValuesSV(valueBlock);
    }
    return getLongTransformResults(valueBlock);
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_dataType != DataType.FLOAT) {
      return super.transformToFloatValuesSV(valueBlock);
    }
    return getFloatTransformResults(valueBlock);
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_dataType != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(valueBlock);
    }
    return getDoubleTransformResults(valueBlock);
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (_dataType != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
    return getBigDecimalTransformResults(valueBlock);
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    if (_dataType != DataType.STRING) {
      return super.transformToStringValuesSV(valueBlock);
    }
    return getStringTransformResults(valueBlock);
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    RoaringBitmap[] nullBitMaps = getNullBitMaps(valueBlock, _transformFunctions);
    RoaringBitmap bitmap = nullBitMaps[0];
    if (bitmap == null || bitmap.isEmpty()) {
      return null;
    }
    for (int i = 1; i < nullBitMaps.length; i++) {
      RoaringBitmap curBitmap = nullBitMaps[i];
      if (curBitmap == null || curBitmap.isEmpty()) {
        return null;
      }
      bitmap.and(curBitmap);
    }
    if (bitmap == null || bitmap.isEmpty()) {
      return null;
    }
    return bitmap;
  }
}
