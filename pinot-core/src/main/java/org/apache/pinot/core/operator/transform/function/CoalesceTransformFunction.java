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
import org.apache.pinot.spi.data.FieldSpec.DataType;
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
  public static final int NULL_INT = Integer.MIN_VALUE;
  public static final long NULL_LONG = Long.MIN_VALUE;
  public static final float NULL_FLOAT = Float.NEGATIVE_INFINITY;
  public static final double NULL_DOUBLE = Double.NEGATIVE_INFINITY;
  public static final BigDecimal NULL_BIG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);
  public static final String NULL_STRING = "null";

  private TransformFunction[] _transformFunctions;
  private DataType _dataType;
  private TransformResultMetadata _resultMetadata;

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
        results[i] = NULL_INT;
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
        results[i] = NULL_LONG;
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
        results[i] = NULL_FLOAT;
      }
    }
    return results;
  }

  /**
   * Get transform double results based on store type.
   * @param projectionBlock
   */
  private double[] getDoubleTransformResults(ProjectionBlock projectionBlock) {
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
        results[i] = NULL_DOUBLE;
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
        results[i] = NULL_BIG_DECIMAL;
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
        results[i] = NULL_STRING;
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
      DataType dataType = func.getResultMetadata().getDataType();
      if (_dataType == null) {
        _dataType = dataType;
      } else {
        Preconditions.checkArgument(dataType == _dataType, "Argument types have to be the same.");
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
      default:
        throw new UnsupportedOperationException("Coalesce only supports numerical and string data type");
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.INT) {
      return super.transformToIntValuesSV(projectionBlock);
    }
    return getIntTransformResults(projectionBlock);
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    return getLongTransformResults(projectionBlock);
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    return getFloatTransformResults(projectionBlock);
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    return getDoubleTransformResults(projectionBlock);
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(projectionBlock);
    }
    return getBigDecimalTransformResults(projectionBlock);
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    return getStringTransformResults(projectionBlock);
  }

  public static void main(String[] args) {
    System.out.println(BigDecimal.valueOf(Long.MIN_VALUE));
  }
}
