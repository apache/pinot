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
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class CastTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "cast";

  private TransformFunction _transformFunction;
  private TransformResultMetadata _resultMetadata;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are more than 1 arguments
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for CAST transform function");
    }

    _transformFunction = arguments.get(0);
    TransformFunction castFormatTransformFunction = arguments.get(1);
    boolean isSVCol = _transformFunction.getResultMetadata().isSingleValue();

    if (castFormatTransformFunction instanceof LiteralTransformFunction) {
      String targetType = ((LiteralTransformFunction) castFormatTransformFunction).getLiteral().toUpperCase();
      switch (targetType) {
        case "INT":
        case "INTEGER":
          _resultMetadata = isSVCol ? INT_SV_NO_DICTIONARY_METADATA : INT_MV_NO_DICTIONARY_METADATA;
          break;
        case "LONG":
          _resultMetadata = isSVCol ? LONG_SV_NO_DICTIONARY_METADATA : LONG_MV_NO_DICTIONARY_METADATA;
          break;
        case "FLOAT":
          _resultMetadata = isSVCol ? FLOAT_SV_NO_DICTIONARY_METADATA : FLOAT_MV_NO_DICTIONARY_METADATA;
          break;
        case "DOUBLE":
          _resultMetadata = isSVCol ? DOUBLE_SV_NO_DICTIONARY_METADATA : DOUBLE_MV_NO_DICTIONARY_METADATA;
          break;
        case "DECIMAL":
        case "BIGDECIMAL":
        case "BIG_DECIMAL":
          if (!isSVCol) {
            // TODO: MV cast to BIG_DECIMAL type
            throw new IllegalArgumentException(
                "Cast is not supported on multi-value column to target type: " + targetType);
          }
          _resultMetadata = BIG_DECIMAL_SV_NO_DICTIONARY_METADATA;
          break;
        case "BOOL":
        case "BOOLEAN":
          if (!isSVCol) {
            throw new IllegalArgumentException(
                "Cast is not supported on multi-value column to target type: " + targetType);
          }
          _resultMetadata = BOOLEAN_SV_NO_DICTIONARY_METADATA;
          break;
        case "TIMESTAMP":
          if (!isSVCol) {
            throw new IllegalArgumentException(
                "Cast is not supported on multi-value column to target type: " + targetType);
          }
          _resultMetadata = TIMESTAMP_SV_NO_DICTIONARY_METADATA;
          break;
        case "STRING":
        case "VARCHAR":
          _resultMetadata = isSVCol ? STRING_SV_NO_DICTIONARY_METADATA : STRING_MV_NO_DICTIONARY_METADATA;
          break;
        case "JSON":
          if (!isSVCol) {
            throw new IllegalArgumentException(
                "Cast is not supported on multi-value column to target type: " + targetType);
          }
          _resultMetadata = JSON_SV_NO_DICTIONARY_METADATA;
          break;
        default:
          throw new IllegalArgumentException("Unable to cast expression to type - " + targetType);
      }
    } else {
      throw new IllegalArgumentException("Invalid cast to type - " + castFormatTransformFunction.getName());
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return _resultMetadata;
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    DataType resultStoredType = _resultMetadata.getDataType().getStoredType();
    if (resultStoredType == DataType.DOUBLE) {
      return _transformFunction.transformToDoubleValuesMV(projectionBlock);
    } else {
      return super.transformToDoubleValuesMV(projectionBlock);
    }
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.STRING) {
      return _transformFunction.transformToStringValuesMV(projectionBlock);
    } else {
      return super.transformToStringValuesMV(projectionBlock);
    }
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    DataType resultStoredType = _resultMetadata.getDataType().getStoredType();
    if (resultStoredType == DataType.INT) {
      return _transformFunction.transformToIntValuesMV(projectionBlock);
    } else {
      return super.transformToIntValuesMV(projectionBlock);
    }
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    DataType resultStoredType = _resultMetadata.getDataType().getStoredType();
    if (resultStoredType == DataType.FLOAT) {
      return _transformFunction.transformToFloatValuesMV(projectionBlock);
    } else {
      return super.transformToFloatValuesMV(projectionBlock);
    }
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    DataType resultStoredType = _resultMetadata.getDataType().getStoredType();
    if (resultStoredType == DataType.LONG) {
      return _transformFunction.transformToLongValuesMV(projectionBlock);
    } else {
      return super.transformToLongValuesMV(projectionBlock);
    }
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.INT) {
      return _transformFunction.transformToIntValuesSV(projectionBlock);
    } else {
      return super.transformToIntValuesSV(projectionBlock);
    }
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.LONG) {
      return _transformFunction.transformToLongValuesSV(projectionBlock);
    } else {
      return super.transformToLongValuesSV(projectionBlock);
    }
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.FLOAT) {
      return _transformFunction.transformToFloatValuesSV(projectionBlock);
    } else {
      return super.transformToFloatValuesSV(projectionBlock);
    }
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.DOUBLE) {
      return _transformFunction.transformToDoubleValuesSV(projectionBlock);
    } else {
      return super.transformToDoubleValuesSV(projectionBlock);
    }
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.BIG_DECIMAL) {
      return _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
    } else {
      return super.transformToBigDecimalValuesSV(projectionBlock);
    }
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.STRING) {
      return _transformFunction.transformToStringValuesSV(projectionBlock);
    } else {
      return super.transformToStringValuesSV(projectionBlock);
    }
  }
}
