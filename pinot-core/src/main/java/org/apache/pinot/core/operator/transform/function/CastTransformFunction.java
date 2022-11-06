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
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ArrayCopyUtils;


public class CastTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "cast";

  private TransformFunction _transformFunction;
  private DataType _sourceDataType;
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
    TransformResultMetadata sourceMetadata = _transformFunction.getResultMetadata();
    _sourceDataType = sourceMetadata.getDataType();
    boolean sourceSV = sourceMetadata.isSingleValue();
    TransformFunction castFormatTransformFunction = arguments.get(1);
    if (castFormatTransformFunction instanceof LiteralTransformFunction) {
      String targetType = ((LiteralTransformFunction) castFormatTransformFunction).getLiteral().toUpperCase();
      switch (targetType) {
        case "INT":
        case "INTEGER":
          _resultMetadata = sourceSV ? INT_SV_NO_DICTIONARY_METADATA : INT_MV_NO_DICTIONARY_METADATA;
          break;
        case "LONG":
          _resultMetadata = sourceSV ? LONG_SV_NO_DICTIONARY_METADATA : LONG_MV_NO_DICTIONARY_METADATA;
          break;
        case "FLOAT":
          _resultMetadata = sourceSV ? FLOAT_SV_NO_DICTIONARY_METADATA : FLOAT_MV_NO_DICTIONARY_METADATA;
          break;
        case "DOUBLE":
          _resultMetadata = sourceSV ? DOUBLE_SV_NO_DICTIONARY_METADATA : DOUBLE_MV_NO_DICTIONARY_METADATA;
          break;
        case "DECIMAL":
        case "BIGDECIMAL":
        case "BIG_DECIMAL":
          // TODO: Support MV BIG_DECIMAL
          Preconditions.checkState(sourceSV, "Cannot cast from MV to BIG_DECIMAL");
          _resultMetadata = BIG_DECIMAL_SV_NO_DICTIONARY_METADATA;
          break;
        case "BOOL":
        case "BOOLEAN":
          _resultMetadata = sourceSV ? BOOLEAN_SV_NO_DICTIONARY_METADATA : BOOLEAN_MV_NO_DICTIONARY_METADATA;
          break;
        case "TIMESTAMP":
          _resultMetadata = sourceSV ? TIMESTAMP_SV_NO_DICTIONARY_METADATA : TIMESTAMP_MV_NO_DICTIONARY_METADATA;
          break;
        case "STRING":
        case "VARCHAR":
          _resultMetadata = sourceSV ? STRING_SV_NO_DICTIONARY_METADATA : STRING_MV_NO_DICTIONARY_METADATA;
          break;
        case "JSON":
          _resultMetadata = sourceSV ? JSON_SV_NO_DICTIONARY_METADATA : JSON_MV_NO_DICTIONARY_METADATA;
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
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    switch (_resultMetadata.getDataType()) {
      case INT:
        return _transformFunction.transformToIntValuesSV(projectionBlock);
      case BOOLEAN:
        return transformToBooleanValuesSV(projectionBlock);
      default:
        return super.transformToIntValuesSV(projectionBlock);
    }
  }

  // TODO: Add it to the interface
  private int[] transformToBooleanValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_intValuesSV == null) {
      _intValuesSV = new int[length];
    }
    switch (_sourceDataType.getStoredType()) {
      case INT:
        int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(intValues, _intValuesSV, length);
        break;
      case LONG:
        long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(longValues, _intValuesSV, length);
        break;
      case FLOAT:
        float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(floatValues, _intValuesSV, length);
        break;
      case DOUBLE:
        double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(doubleValues, _intValuesSV, length);
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(bigDecimalValues, _intValuesSV, length);
        break;
      case STRING:
        String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(stringValues, _intValuesSV, length);
        break;
      default:
        throw new IllegalStateException(String.format("Cannot cast from SV %s to BOOLEAN", _sourceDataType));
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    switch (_resultMetadata.getDataType()) {
      case LONG:
        return _transformFunction.transformToLongValuesSV(projectionBlock);
      case TIMESTAMP:
        return transformToTimestampValuesSV(projectionBlock);
      default:
        return super.transformToLongValuesSV(projectionBlock);
    }
  }

  // TODO: Add it to the interface
  private long[] transformToTimestampValuesSV(ProjectionBlock projectionBlock) {
    if (_sourceDataType.getStoredType() == DataType.STRING) {
      int length = projectionBlock.getNumDocs();
      if (_longValuesSV == null) {
        _longValuesSV = new long[length];
      }
      String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
      ArrayCopyUtils.copyToTimestamp(stringValues, _longValuesSV, length);
      return _longValuesSV;
    } else {
      return _transformFunction.transformToLongValuesSV(projectionBlock);
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
    DataType resultDataType = _resultMetadata.getDataType();
    if (resultDataType.getStoredType() == DataType.STRING) {
      switch (_sourceDataType) {
        case BOOLEAN:
          int length = projectionBlock.getNumDocs();
          if (_stringValuesSV == null) {
            _stringValuesSV = new String[length];
          }
          int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copyFromBoolean(intValues, _stringValuesSV, length);
          return _stringValuesSV;
        case TIMESTAMP:
          length = projectionBlock.getNumDocs();
          if (_stringValuesSV == null) {
            _stringValuesSV = new String[length];
          }
          long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copyFromTimestamp(longValues, _stringValuesSV, length);
          return _stringValuesSV;
        default:
          return _transformFunction.transformToStringValuesSV(projectionBlock);
      }
    } else {
      int length = projectionBlock.getNumDocs();
      if (_stringValuesSV == null) {
        _stringValuesSV = new String[length];
      }
      switch (resultDataType) {
        case INT:
          int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _stringValuesSV, length);
          break;
        case LONG:
          long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _stringValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _stringValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _stringValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _stringValuesSV, length);
          break;
        case BOOLEAN:
          intValues = transformToBooleanValuesSV(projectionBlock);
          ArrayCopyUtils.copyFromBoolean(intValues, _stringValuesSV, length);
          break;
        case TIMESTAMP:
          longValues = transformToTimestampValuesSV(projectionBlock);
          ArrayCopyUtils.copyFromTimestamp(longValues, _stringValuesSV, length);
          break;
        case BYTES:
          byte[][] bytesValues = transformToBytesValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bytesValues, _stringValuesSV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot cast from SV %s to STRING", resultDataType));
      }
    }
    return _stringValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ProjectionBlock projectionBlock) {
    switch (_resultMetadata.getDataType()) {
      case INT:
        return _transformFunction.transformToIntValuesMV(projectionBlock);
      case BOOLEAN:
        return transformToBooleanValuesMV(projectionBlock);
      default:
        return super.transformToIntValuesMV(projectionBlock);
    }
  }

  // TODO: Add it to the interface
  private int[][] transformToBooleanValuesMV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();
    if (_intValuesMV == null) {
      _intValuesMV = new int[length][];
    }
    switch (_sourceDataType.getStoredType()) {
      case INT:
        int[][] intValuesMV = _transformFunction.transformToIntValuesMV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(intValuesMV, _intValuesMV, length);
        break;
      case LONG:
        long[][] longValuesMV = _transformFunction.transformToLongValuesMV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(longValuesMV, _intValuesMV, length);
        break;
      case FLOAT:
        float[][] floatValuesMV = _transformFunction.transformToFloatValuesMV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(floatValuesMV, _intValuesMV, length);
        break;
      case DOUBLE:
        double[][] doubleValuesMV = _transformFunction.transformToDoubleValuesMV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(doubleValuesMV, _intValuesMV, length);
        break;
      case STRING:
        String[][] stringValuesMV = _transformFunction.transformToStringValuesMV(projectionBlock);
        ArrayCopyUtils.copyToBoolean(stringValuesMV, _intValuesMV, length);
        break;
      default:
        throw new IllegalStateException(String.format("Cannot cast from MV %s to BOOLEAN", _sourceDataType));
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ProjectionBlock projectionBlock) {
    switch (_resultMetadata.getDataType()) {
      case LONG:
        return _transformFunction.transformToLongValuesMV(projectionBlock);
      case TIMESTAMP:
        return transformToTimestampValuesMV(projectionBlock);
      default:
        return super.transformToLongValuesMV(projectionBlock);
    }
  }

  // TODO: Add it to the interface
  private long[][] transformToTimestampValuesMV(ProjectionBlock projectionBlock) {
    if (_sourceDataType.getStoredType() == DataType.STRING) {
      int length = projectionBlock.getNumDocs();
      if (_longValuesMV == null) {
        _longValuesMV = new long[length][];
      }
      String[][] stringValuesMV = _transformFunction.transformToStringValuesMV(projectionBlock);
      ArrayCopyUtils.copyToTimestamp(stringValuesMV, _longValuesMV, length);
      return _longValuesMV;
    } else {
      return _transformFunction.transformToLongValuesMV(projectionBlock);
    }
  }

  @Override
  public float[][] transformToFloatValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.FLOAT) {
      return _transformFunction.transformToFloatValuesMV(projectionBlock);
    } else {
      return super.transformToFloatValuesMV(projectionBlock);
    }
  }

  @Override
  public double[][] transformToDoubleValuesMV(ProjectionBlock projectionBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.DOUBLE) {
      return _transformFunction.transformToDoubleValuesMV(projectionBlock);
    } else {
      return super.transformToDoubleValuesMV(projectionBlock);
    }
  }

  @Override
  public String[][] transformToStringValuesMV(ProjectionBlock projectionBlock) {
    DataType resultDataType = _resultMetadata.getDataType();
    if (resultDataType.getStoredType() == DataType.STRING) {
      switch (_sourceDataType) {
        case BOOLEAN:
          int length = projectionBlock.getNumDocs();
          if (_stringValuesMV == null) {
            _stringValuesMV = new String[length][];
          }
          int[][] intValuesMV = _transformFunction.transformToIntValuesMV(projectionBlock);
          ArrayCopyUtils.copyFromBoolean(intValuesMV, _stringValuesMV, length);
          return _stringValuesMV;
        case TIMESTAMP:
          length = projectionBlock.getNumDocs();
          if (_stringValuesMV == null) {
            _stringValuesMV = new String[length][];
          }
          long[][] longValuesMV = _transformFunction.transformToLongValuesMV(projectionBlock);
          ArrayCopyUtils.copyFromTimestamp(longValuesMV, _stringValuesMV, length);
          return _stringValuesMV;
        default:
          return _transformFunction.transformToStringValuesMV(projectionBlock);
      }
    } else {
      int length = projectionBlock.getNumDocs();
      if (_stringValuesMV == null) {
        _stringValuesMV = new String[length][];
      }
      switch (resultDataType) {
        case INT:
          int[][] intValuesMV = _transformFunction.transformToIntValuesMV(projectionBlock);
          ArrayCopyUtils.copy(intValuesMV, _stringValuesMV, length);
          break;
        case LONG:
          long[][] longValuesMV = _transformFunction.transformToLongValuesMV(projectionBlock);
          ArrayCopyUtils.copy(longValuesMV, _stringValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = _transformFunction.transformToFloatValuesMV(projectionBlock);
          ArrayCopyUtils.copy(floatValuesMV, _stringValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = _transformFunction.transformToDoubleValuesMV(projectionBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _stringValuesMV, length);
          break;
        case BOOLEAN:
          intValuesMV = transformToBooleanValuesMV(projectionBlock);
          ArrayCopyUtils.copyFromBoolean(intValuesMV, _stringValuesMV, length);
          break;
        case TIMESTAMP:
          longValuesMV = transformToTimestampValuesMV(projectionBlock);
          ArrayCopyUtils.copyFromTimestamp(longValuesMV, _stringValuesMV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot cast from MV %s to STRING", resultDataType));
      }
    }
    return _stringValuesMV;
  }
}
