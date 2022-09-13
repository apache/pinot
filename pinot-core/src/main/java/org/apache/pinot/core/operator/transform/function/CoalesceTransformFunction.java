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
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.NullValueUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * The <code>CoalesceTransformFunction</code> implements the Coalesce operator.
 *
 * The results are in String format for first non-null value in the argument list.
 * If all arguments are null, return a 'null' string.
 * Note: arguments have to be column names and single type. The type can be either numeric or string.
 * Number of arguments has to be greater than 0.
 *
 * Expected result:
 * Coalesce(nullColumn, columnA): columnA
 * Coalesce(columnA, nullColumn): nullColumn
 * Coalesce(nullColumnA, nullColumnB): "null"
 *
 * Note this operator only takes column names for now.
 * SQL Syntax:
 *    Coalesce(columnA, columnB)
 */
public class CoalesceTransformFunction extends BaseTransformFunction {
  private TransformFunction[] _transformFunctions;
  private FieldSpec.DataType _dataType;

  /**
   * Returns a bit map of corresponding column.
   * Returns an empty bitmap by default if null option is disabled.
   */
  private static RoaringBitmap[] getNullBitMaps(ProjectionBlock projectionBlock,
      TransformFunction[] transformFunctions) {
    RoaringBitmap[] roaringBitmaps = new RoaringBitmap[transformFunctions.length];
    for (int i = 0; i < roaringBitmaps.length; i++) {
      TransformFunction func = transformFunctions[i];
      String columnName = ((IdentifierTransformFunction) func).getColumnName();
      RoaringBitmap nullBitmap = projectionBlock.getBlockValueSet(columnName).getNullBitmap();
      roaringBitmaps[i] = nullBitmap;
    }
    return roaringBitmaps;
  }

  /**
   * Get transform int results based on store type.
   * @param projectionBlock
   */
  private int[] getIntTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    int[] results = new int[length];
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(projectionBlock, _transformFunctions);
    int[][] data = new int[width][length];
    RoaringBitmap filledData = new RoaringBitmap();
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!filledData.contains(j)) {
          filledData.add(j);
          data[j] = _transformFunctions[j].transformToIntValuesSV(projectionBlock);
        }
        hasNonNullValue = true;
        results[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        results[i] = (int) NullValueUtils.getDefaultNullValue(_dataType);
      }
    }
    return results;
  }

  /**
   * Get transform long results based on store type.
   * @param projectionBlock
   */
  private long[] getLongTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    long[] results = new long[length];
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(projectionBlock, _transformFunctions);
    long[][] data = new long[width][length];
    RoaringBitmap filledData = new RoaringBitmap(); // indicates whether certain column has be filled in data.
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!filledData.contains(j)) {
          filledData.add(j);
          data[j] = _transformFunctions[j].transformToLongValuesSV(projectionBlock);
        }
        hasNonNullValue = true;
        results[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        results[i] = (long) NullValueUtils.getDefaultNullValue(_dataType);
      }
    }
    return results;
  }

  /**
   * Get transform double results based on store type.
   * @param projectionBlock
   */
  private double[] getDoublelTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    double[] results = new double[length];
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(projectionBlock, _transformFunctions);
    double[][] data = new double[width][length];
    RoaringBitmap filledData = new RoaringBitmap(); // indicates whether certain column has be filled in data.
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!filledData.contains(j)) {
          filledData.add(j);
          data[j] = _transformFunctions[j].transformToDoubleValuesSV(projectionBlock);
        }
        hasNonNullValue = true;
        results[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        results[i] = (double) NullValueUtils.getDefaultNullValue(_dataType);
      }
    }
    return results;
  }

  /**
   * Get transform float results based on store type.
   * @param projectionBlock
   */
  private float[] getFloatTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    float[] results = new float[length];
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(projectionBlock, _transformFunctions);
    float[][] data = new float[width][length];
    RoaringBitmap filledData = new RoaringBitmap(); // indicates whether certain column has be filled in data.
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!filledData.contains(j)) {
          filledData.add(j);
          data[j] = _transformFunctions[j].transformToFloatValuesSV(projectionBlock);
        }
        hasNonNullValue = true;
        results[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        results[i] = (float) NullValueUtils.getDefaultNullValue(_dataType);
      }
    }
    return results;
  }

  /**
   * Get transform BigDecimal results based on store type.
   * @param projectionBlock
   */
  private BigDecimal[] getBigDecimalTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    BigDecimal[] results = new BigDecimal[length];
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(projectionBlock, _transformFunctions);
    BigDecimal[][] data = new BigDecimal[width][length];
    RoaringBitmap filledData = new RoaringBitmap(); // indicates whether certain column has be filled in data.
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!filledData.contains(j)) {
          filledData.add(j);
          data[j] = _transformFunctions[j].transformToBigDecimalValuesSV(projectionBlock);
        }
        hasNonNullValue = true;
        results[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        results[i] = (BigDecimal) NullValueUtils.getDefaultNullValue(_dataType);
      }
    }
    return results;
  }


  /**
   * Get transform String results based on store type.
   * @param projectionBlock
   */
  private String[] getStringTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    String[] results = new String[length];
    int width = _transformFunctions.length;
    RoaringBitmap[] nullBitMaps = getNullBitMaps(projectionBlock, _transformFunctions);
    String[][] data = new String[width][length];
    RoaringBitmap filledData = new RoaringBitmap(); // indicates whether certain column has be filled in data.
    for (int i = 0; i < length; i++) {
      boolean hasNonNullValue = false;
      for (int j = 0; j < width; j++) {
        // Consider value as null only when null option is enabled.
        if (nullBitMaps[j] != null && nullBitMaps[j].contains(i)) {
          continue;
        }
        if (!filledData.contains(j)) {
          filledData.add(j);
          data[j] = _transformFunctions[j].transformToStringValuesSV(projectionBlock);
        }
        hasNonNullValue = true;
        results[i] = data[j][i];
        break;
      }
      if (!hasNonNullValue) {
        results[i] = (String) NullValueUtils.getDefaultNullValue(_dataType);
      }
    }
    return results;
  }

  @Override
  public String getName() {
    return TransformFunctionType.COALESCE.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    int argSize = arguments.size();
    Preconditions.checkArgument(argSize > 0, "COALESCE needs to have at least one argument.");
    _transformFunctions = new TransformFunction[argSize];
    for (int i = 0; i < argSize; i++) {
      TransformFunction func = arguments.get(i);
      Preconditions.checkArgument(func instanceof IdentifierTransformFunction,
          "Only column names are supported in COALESCE.");
      FieldSpec.DataType dataType = func.getResultMetadata().getDataType().getStoredType();
      if (_dataType == null) {
        _dataType = dataType;
      } else {
        Preconditions.checkArgument(dataType.equals(_dataType), "Argument types have to be the same.");
      }
      _transformFunctions[i] = func;
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    switch (_dataType) {
      case STRING:
        return STRING_SV_NO_DICTIONARY_METADATA;
      case INT:
        return INT_SV_NO_DICTIONARY_METADATA;
      case LONG:
        return LONG_SV_NO_DICTIONARY_METADATA;
      case BIG_DECIMAL:
        return BIG_DECIMAL_SV_NO_DICTIONARY_METADATA;
      case FLOAT:
        return FLOAT_SV_NO_DICTIONARY_METADATA;
      case DOUBLE:
        return DOUBLE_SV_NO_DICTIONARY_METADATA;
      default:
        throw new RuntimeException("Coalesce only supports numerical and string data type");
    }
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != FieldSpec.DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    return getStringTransformResults(projectionBlock);
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != FieldSpec.DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    return getIntTransformResults(projectionBlock);
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != FieldSpec.DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    return getLongTransformResults(projectionBlock);
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != FieldSpec.DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(projectionBlock);
    }
    return getBigDecimalTransformResults(projectionBlock);
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != FieldSpec.DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    return getDoublelTransformResults(projectionBlock);
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != FieldSpec.DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    return getFloatTransformResults(projectionBlock);
  }
}
