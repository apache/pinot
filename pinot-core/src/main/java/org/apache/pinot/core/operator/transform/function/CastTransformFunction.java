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
import java.sql.Timestamp;
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

    if (castFormatTransformFunction instanceof LiteralTransformFunction) {
      String targetType = ((LiteralTransformFunction) castFormatTransformFunction).getLiteral().toUpperCase();
      switch (targetType) {
        case "INT":
        case "INTEGER":
          _resultMetadata = INT_SV_NO_DICTIONARY_METADATA;
          break;
        case "LONG":
          _resultMetadata = LONG_SV_NO_DICTIONARY_METADATA;
          break;
        case "FLOAT":
          _resultMetadata = FLOAT_SV_NO_DICTIONARY_METADATA;
          break;
        case "DOUBLE":
          _resultMetadata = DOUBLE_SV_NO_DICTIONARY_METADATA;
          break;
        case "DECIMAL":
        case "BIGDECIMAL":
        case "BIG_DECIMAL":
          _resultMetadata = BIG_DECIMAL_SV_NO_DICTIONARY_METADATA;
          break;
        case "BOOLEAN":
          _resultMetadata = BOOLEAN_SV_NO_DICTIONARY_METADATA;
          break;
        case "TIMESTAMP":
          _resultMetadata = TIMESTAMP_SV_NO_DICTIONARY_METADATA;
          break;
        case "STRING":
        case "VARCHAR":
          _resultMetadata = STRING_SV_NO_DICTIONARY_METADATA;
          break;
        case "JSON":
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
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    // When casting to types other than INT, need to first read as the result type then convert to int values
    DataType resultStoredType = _resultMetadata.getDataType().getStoredType();
    if (resultStoredType == DataType.INT) {
      return _transformFunction.transformToIntValuesSV(projectionBlock);
    } else {
      int length = projectionBlock.getNumDocs();
      if (_intValuesSV == null || _intValuesSV.length < length) {
        _intValuesSV = new int[length];
      }
      switch (resultStoredType) {
        case LONG:
          long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _intValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _intValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _intValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _intValuesSV, length);
          break;
        case STRING:
          String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _intValuesSV, length);
          break;
        default:
          throw new IllegalStateException();
      }
      return _intValuesSV;
    }
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    // When casting to types other than LONG, need to first read as the result type then convert to long values
    DataType resultStoredType = _resultMetadata.getDataType().getStoredType();
    if (resultStoredType == DataType.LONG) {
      return _transformFunction.transformToLongValuesSV(projectionBlock);
    } else {
      int length = projectionBlock.getNumDocs();

      if (_longValuesSV == null || _longValuesSV.length < length) {
        _longValuesSV = new long[length];
      }
      switch (resultStoredType) {
        case INT:
          int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _longValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _longValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _longValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _longValuesSV, length);
          break;
        case STRING:
          String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _longValuesSV, length);
          break;
        default:
          throw new IllegalStateException();
      }
      return _longValuesSV;
    }
  }

  @Override
  public float[] transformToFloatValuesSV(ProjectionBlock projectionBlock) {
    // When casting to types other than FLOAT, need to first read as the result type then convert to float values
    DataType resultStoredType = _resultMetadata.getDataType().getStoredType();
    if (resultStoredType == DataType.FLOAT) {
      return _transformFunction.transformToFloatValuesSV(projectionBlock);
    } else {
      int length = projectionBlock.getNumDocs();

      if (_floatValuesSV == null || _floatValuesSV.length < length) {
        _floatValuesSV = new float[length];
      }
      switch (resultStoredType) {
        case INT:
          int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _floatValuesSV, length);
          break;
        case LONG:
          long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _floatValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _floatValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _floatValuesSV, length);
          break;
        case STRING:
          String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _floatValuesSV, length);
          break;
        default:
          throw new IllegalStateException();
      }
      return _floatValuesSV;
    }
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    // When casting to types other than DOUBLE, need to first read as the result type then convert to double values
    DataType resultStoredType = _resultMetadata.getDataType().getStoredType();
    if (resultStoredType == DataType.DOUBLE) {
      return _transformFunction.transformToDoubleValuesSV(projectionBlock);
    } else {
      int length = projectionBlock.getNumDocs();

      if (_doubleValuesSV == null || _doubleValuesSV.length < length) {
        _doubleValuesSV = new double[length];
      }
      switch (resultStoredType) {
        case INT:
          int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _doubleValuesSV, length);
          break;
        case LONG:
          long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _doubleValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _doubleValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _doubleValuesSV, length);
          break;
        case STRING:
          String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _doubleValuesSV, length);
          break;
        default:
          throw new IllegalStateException();
      }
      return _doubleValuesSV;
    }
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ProjectionBlock projectionBlock) {
    // When casting to types other than BIG_DECIMAL, need to first read as the result type then convert to
    // BigDecimal values
    DataType dataType = _resultMetadata.getDataType();
    DataType resultStoredType = dataType.getStoredType();
    if (dataType == DataType.BIG_DECIMAL) {
      return _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
    } else {
      int length = projectionBlock.getNumDocs();
      if (_bigDecimalValuesSV == null || _bigDecimalValuesSV.length < length) {
        _bigDecimalValuesSV = new BigDecimal[length];
      }
      int numDocs = projectionBlock.getNumDocs();
      switch (resultStoredType) {
        case INT:
          int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
          ArrayCopyUtils.copy(intValues, _bigDecimalValuesSV, numDocs);
          break;
        case LONG:
          long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
          ArrayCopyUtils.copy(longValues, _bigDecimalValuesSV, numDocs);
          break;
        case FLOAT:
          float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
          ArrayCopyUtils.copy(floatValues, _bigDecimalValuesSV, numDocs);
          break;
        case DOUBLE:
          double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
          ArrayCopyUtils.copy(doubleValues, _bigDecimalValuesSV, numDocs);
          break;
        case STRING:
          String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
          ArrayCopyUtils.copy(stringValues, _bigDecimalValuesSV, numDocs);
          break;
        default:
          throw new IllegalStateException();
      }
      return _bigDecimalValuesSV;
    }
  }

  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    // When casting to types other than STRING, need to first read as the result type then convert to string values
    DataType resultDataType = _resultMetadata.getDataType();
    DataType resultStoredType = resultDataType.getStoredType();
    int length = projectionBlock.getNumDocs();
    if (resultStoredType == DataType.STRING) {
      // Specialize BOOlEAN and TIMESTAMP when casting to STRING
      DataType inputDataType = _transformFunction.getResultMetadata().getDataType();
      if (inputDataType.getStoredType() != inputDataType) {
        if (_stringValuesSV == null || _stringValuesSV.length < length) {
          _stringValuesSV = new String[length];
        }
        if (inputDataType == DataType.BOOLEAN) {
          int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _stringValuesSV[i] = Boolean.toString(intValues[i] == 1);
          }
        } else {
          assert inputDataType == DataType.TIMESTAMP;
          long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _stringValuesSV[i] = new Timestamp(longValues[i]).toString();
          }
        }
        return _stringValuesSV;
      } else {
        return _transformFunction.transformToStringValuesSV(projectionBlock);
      }
    } else {
      if (_stringValuesSV == null || _stringValuesSV.length < length) {
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
          intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _stringValuesSV[i] = Boolean.toString(intValues[i] == 1);
          }
          break;
        case TIMESTAMP:
          longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
          for (int i = 0; i < length; i++) {
            _stringValuesSV[i] = new Timestamp(longValues[i]).toString();
          }
          break;
        default:
          throw new IllegalStateException();
      }
      return _stringValuesSV;
    }
  }
}
