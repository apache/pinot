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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.IntConsumer;
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
   * Get transform int results based on store type.
   * @param projectionBlock
   */
  private Pair<RoaringBitmap, int[]> getIntTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_intValuesSV == null) {
      _intValuesSV = new int[length];
    }
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0L, length);
    for (TransformFunction func : _transformFunctions) {
      if (nullBitmap.isEmpty()) {
        return ImmutablePair.of(null, _intValuesSV);
      }
      Pair<RoaringBitmap, int[]> curResult = func.transformToIntValuesSVWithNull(projectionBlock);
      RoaringBitmap curBitmap = curResult.getLeft();
      int[] curValues = curResult.getRight();
      nullBitmap.forEach((IntConsumer) (i) -> {
        if (!curBitmap.contains(i)) {
          nullBitmap.remove(i);
          _intValuesSV[i] = curValues[i];
        }
      });
    }
    return ImmutablePair.of(nullBitmap, _intValuesSV);
  }

  /**
   * Get transform long results based on store type.
   * @param projectionBlock
   */
  private Pair<RoaringBitmap, long[]> getLongTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_longValuesSV == null) {
      _longValuesSV = new long[length];
    }
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0L, length);
    for (TransformFunction func : _transformFunctions) {
      if (nullBitmap.isEmpty()) {
        return ImmutablePair.of(null, _longValuesSV);
      }
      Pair<RoaringBitmap, long[]> curResult = func.transformToLongValuesSVWithNull(projectionBlock);
      RoaringBitmap curBitmap = curResult.getLeft();
      long[] curValues = curResult.getRight();
      nullBitmap.forEach((IntConsumer) (i) -> {
        if (!curBitmap.contains(i)) {
          nullBitmap.remove(i);
          _longValuesSV[i] = curValues[i];
        }
      });
    }
    return ImmutablePair.of(nullBitmap, _longValuesSV);
  }

  /**
   * Get transform float results based on store type.
   * @param projectionBlock
   */
  private Pair<RoaringBitmap, float[]> getFloatTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_floatValuesSV == null) {
      _floatValuesSV = new float[length];
    }
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0L, length);
    for (TransformFunction func : _transformFunctions) {
      if (nullBitmap.isEmpty()) {
        return ImmutablePair.of(null, _floatValuesSV);
      }
      Pair<RoaringBitmap, float[]> curResult = func.transformToFloatValuesSVWithNull(projectionBlock);
      RoaringBitmap curBitmap = curResult.getLeft();
      float[] curValues = curResult.getRight();
      nullBitmap.forEach((IntConsumer) (i) -> {
        if (!curBitmap.contains(i)) {
          nullBitmap.remove(i);
          _floatValuesSV[i] = curValues[i];
        }
      });
    }
    return ImmutablePair.of(nullBitmap, _floatValuesSV);
  }

  /**
   * Get transform double results based on store type.
   * @param projectionBlock
   */
  private Pair<RoaringBitmap, double[]> getDoubleTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_doubleValuesSV == null) {
      _doubleValuesSV = new double[length];
    }
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0L, length);
    for (TransformFunction func : _transformFunctions) {
      if (nullBitmap.isEmpty()) {
        return ImmutablePair.of(null, _doubleValuesSV);
      }
      Pair<RoaringBitmap, double[]> curResult = func.transformToDoubleValuesSVWithNull(projectionBlock);
      RoaringBitmap curBitmap = curResult.getLeft();
      double[] curValues = curResult.getRight();
      nullBitmap.forEach((IntConsumer) (i) -> {
        if (!curBitmap.contains(i)) {
          nullBitmap.remove(i);
          _doubleValuesSV[i] = curValues[i];
        }
      });
    }
    return ImmutablePair.of(nullBitmap, _doubleValuesSV);
  }

  /**
   * Get transform BigDecimal results based on store type.
   * @param projectionBlock
   */
  private Pair<RoaringBitmap, BigDecimal[]> getBigDecimalTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_bigDecimalValuesSV == null) {
      _bigDecimalValuesSV = new BigDecimal[length];
    }
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0L, length);
    for (TransformFunction func : _transformFunctions) {
      if (nullBitmap.isEmpty()) {
        return ImmutablePair.of(null, _bigDecimalValuesSV);
      }
      Pair<RoaringBitmap, BigDecimal[]> curResult = func.transformToBigDecimalValuesSVWithNull(projectionBlock);
      RoaringBitmap curBitmap = curResult.getLeft();
      BigDecimal[] curValues = curResult.getRight();
      nullBitmap.forEach((IntConsumer) (i) -> {
        if (!curBitmap.contains(i)) {
          nullBitmap.remove(i);
          _bigDecimalValuesSV[i] = curValues[i];
        }
      });
    }
    return ImmutablePair.of(nullBitmap, _bigDecimalValuesSV);
  }

  /**
   * Get transform String results based on store type.
   * @param projectionBlock
   */
  private Pair<RoaringBitmap, String[]> getStringTransformResults(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_stringValuesSV == null) {
      _stringValuesSV = new String[length];
    }
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0L, length);
    for (TransformFunction func : _transformFunctions) {
      if (nullBitmap.isEmpty()) {
        return ImmutablePair.of(null, _stringValuesSV);
      }
      Pair<RoaringBitmap, String[]> curResult = func.transformToStringValuesSVWithNull(projectionBlock);
      RoaringBitmap curBitmap = curResult.getLeft();
      String[] curValues = curResult.getRight();
      nullBitmap.forEach((IntConsumer) (i) -> {
        if (!curBitmap.contains(i)) {
          nullBitmap.remove(i);
          _stringValuesSV[i] = curValues[i];
        }
      });
    }
    return ImmutablePair.of(nullBitmap, _stringValuesSV);
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
      DataType dataType = func.getResultMetadata().getDataType();
      if (_dataType == null) {
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
      default:
        throw new UnsupportedOperationException("Coalesce only supports numerical and string data type");
    }
    super.init(arguments, dataSourceMap);
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
    return getIntTransformResults(projectionBlock).getRight();
  }

  @Override
  public Pair<RoaringBitmap, int[]> transformToIntValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.INT) {
      return super.transformToIntValuesSVWithNull(projectionBlock);
    }
    return getIntTransformResults(projectionBlock);
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.LONG) {
      return super.transformToLongValuesSV(projectionBlock);
    }
    return getLongTransformResults(projectionBlock).getRight();
  }

  @Override
  public Pair<RoaringBitmap, long[]> transformToLongValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.LONG) {
      return super.transformToLongValuesSVWithNull(projectionBlock);
    }
    return getLongTransformResults(projectionBlock);
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.FLOAT) {
      return super.transformToFloatValuesSV(projectionBlock);
    }
    return getFloatTransformResults(projectionBlock).getRight();
  }

  @Override
  public Pair<RoaringBitmap, float[]> transformToFloatValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.FLOAT) {
      return super.transformToFloatValuesSVWithNull(projectionBlock);
    }
    return getFloatTransformResults(projectionBlock);
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.DOUBLE) {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
    return getDoubleTransformResults(projectionBlock).getRight();
  }

  @Override
  public Pair<RoaringBitmap, double[]> transformToDoubleValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.DOUBLE) {
      return super.transformToDoubleValuesSVWithNull(projectionBlock);
    }
    return getDoubleTransformResults(projectionBlock);
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSV(projectionBlock);
    }
    return getBigDecimalTransformResults(projectionBlock).getRight();
  }

  @Override
  public Pair<RoaringBitmap, BigDecimal[]> transformToBigDecimalValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.BIG_DECIMAL) {
      return super.transformToBigDecimalValuesSVWithNull(projectionBlock);
    }
    return getBigDecimalTransformResults(projectionBlock);
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.STRING) {
      return super.transformToStringValuesSV(projectionBlock);
    }
    return getStringTransformResults(projectionBlock).getRight();
  }

  @Override
  public Pair<RoaringBitmap, String[]> transformToStringValuesSVWithNull(ProjectionBlock projectionBlock) {
    if (_dataType != DataType.STRING) {
      return super.transformToStringValuesSVWithNull(projectionBlock);
    }
    return getStringTransformResults(projectionBlock);
  }
}
