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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
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
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
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
      String targetType = ((LiteralTransformFunction) castFormatTransformFunction).getStringLiteral().toUpperCase();
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
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    switch (_resultMetadata.getDataType()) {
      case INT:
        return _transformFunction.transformToIntValuesSV(valueBlock);
      case BOOLEAN:
        return transformToBooleanValuesSV(valueBlock);
      default:
        return super.transformToIntValuesSV(valueBlock);
    }
  }

  // TODO: Add it to the interface
  private int[] transformToBooleanValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    switch (_sourceDataType.getStoredType()) {
      case INT:
        int[] intValues = _transformFunction.transformToIntValuesSV(valueBlock);
        ArrayCopyUtils.copyToBoolean(intValues, _intValuesSV, length);
        break;
      case LONG:
        long[] longValues = _transformFunction.transformToLongValuesSV(valueBlock);
        ArrayCopyUtils.copyToBoolean(longValues, _intValuesSV, length);
        break;
      case FLOAT:
        float[] floatValues = _transformFunction.transformToFloatValuesSV(valueBlock);
        ArrayCopyUtils.copyToBoolean(floatValues, _intValuesSV, length);
        break;
      case DOUBLE:
        double[] doubleValues = _transformFunction.transformToDoubleValuesSV(valueBlock);
        ArrayCopyUtils.copyToBoolean(doubleValues, _intValuesSV, length);
        break;
      case BIG_DECIMAL:
        BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(valueBlock);
        ArrayCopyUtils.copyToBoolean(bigDecimalValues, _intValuesSV, length);
        break;
      case STRING:
        String[] stringValues = _transformFunction.transformToStringValuesSV(valueBlock);
        ArrayCopyUtils.copyToBoolean(stringValues, _intValuesSV, length);
        break;
      default:
        throw new IllegalStateException(String.format("Cannot cast from SV %s to BOOLEAN", _sourceDataType));
    }
    return _intValuesSV;
  }

  @Override
  public long[] transformToLongValuesSV(ValueBlock valueBlock) {
    switch (_resultMetadata.getDataType()) {
      case LONG:
        return _transformFunction.transformToLongValuesSV(valueBlock);
      case TIMESTAMP:
        return transformToTimestampValuesSV(valueBlock);
      default:
        return super.transformToLongValuesSV(valueBlock);
    }
  }

  // TODO: Add it to the interface
  private long[] transformToTimestampValuesSV(ValueBlock valueBlock) {
    if (_sourceDataType.getStoredType() == DataType.STRING) {
      int length = valueBlock.getNumDocs();
      initLongValuesSV(length);
      String[] stringValues = _transformFunction.transformToStringValuesSV(valueBlock);
      ArrayCopyUtils.copyToTimestamp(stringValues, _longValuesSV, length);
      return _longValuesSV;
    } else {
      return _transformFunction.transformToLongValuesSV(valueBlock);
    }
  }

  @Override
  public float[] transformToFloatValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.FLOAT) {
      return _transformFunction.transformToFloatValuesSV(valueBlock);
    } else {
      return super.transformToFloatValuesSV(valueBlock);
    }
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.DOUBLE) {
      return _transformFunction.transformToDoubleValuesSV(valueBlock);
    } else {
      return super.transformToDoubleValuesSV(valueBlock);
    }
  }

  @Override
  public BigDecimal[] transformToBigDecimalValuesSV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.BIG_DECIMAL) {
      return _transformFunction.transformToBigDecimalValuesSV(valueBlock);
    } else {
      return super.transformToBigDecimalValuesSV(valueBlock);
    }
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    DataType resultDataType = _resultMetadata.getDataType();
    if (resultDataType.getStoredType() == DataType.STRING) {
      switch (_sourceDataType) {
        case BOOLEAN:
          int length = valueBlock.getNumDocs();
          initStringValuesSV(length);
          int[] intValues = _transformFunction.transformToIntValuesSV(valueBlock);
          ArrayCopyUtils.copyFromBoolean(intValues, _stringValuesSV, length);
          return _stringValuesSV;
        case TIMESTAMP:
          length = valueBlock.getNumDocs();
          initStringValuesSV(length);
          long[] longValues = _transformFunction.transformToLongValuesSV(valueBlock);
          ArrayCopyUtils.copyFromTimestamp(longValues, _stringValuesSV, length);
          return _stringValuesSV;
        default:
          return _transformFunction.transformToStringValuesSV(valueBlock);
      }
    } else {
      int length = valueBlock.getNumDocs();
      initStringValuesSV(length);
      switch (resultDataType) {
        case INT:
          int[] intValues = _transformFunction.transformToIntValuesSV(valueBlock);
          ArrayCopyUtils.copy(intValues, _stringValuesSV, length);
          break;
        case LONG:
          long[] longValues = _transformFunction.transformToLongValuesSV(valueBlock);
          ArrayCopyUtils.copy(longValues, _stringValuesSV, length);
          break;
        case FLOAT:
          float[] floatValues = _transformFunction.transformToFloatValuesSV(valueBlock);
          ArrayCopyUtils.copy(floatValues, _stringValuesSV, length);
          break;
        case DOUBLE:
          double[] doubleValues = _transformFunction.transformToDoubleValuesSV(valueBlock);
          ArrayCopyUtils.copy(doubleValues, _stringValuesSV, length);
          break;
        case BIG_DECIMAL:
          BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(valueBlock);
          ArrayCopyUtils.copy(bigDecimalValues, _stringValuesSV, length);
          break;
        case BOOLEAN:
          intValues = transformToBooleanValuesSV(valueBlock);
          ArrayCopyUtils.copyFromBoolean(intValues, _stringValuesSV, length);
          break;
        case TIMESTAMP:
          longValues = transformToTimestampValuesSV(valueBlock);
          ArrayCopyUtils.copyFromTimestamp(longValues, _stringValuesSV, length);
          break;
        case BYTES:
          byte[][] bytesValues = transformToBytesValuesSV(valueBlock);
          ArrayCopyUtils.copy(bytesValues, _stringValuesSV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot cast from SV %s to STRING", resultDataType));
      }
    }
    return _stringValuesSV;
  }

  @Override
  public int[][] transformToIntValuesMV(ValueBlock valueBlock) {
    switch (_resultMetadata.getDataType()) {
      case INT:
        return _transformFunction.transformToIntValuesMV(valueBlock);
      case BOOLEAN:
        return transformToBooleanValuesMV(valueBlock);
      default:
        return super.transformToIntValuesMV(valueBlock);
    }
  }

  // TODO: Add it to the interface
  private int[][] transformToBooleanValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesMV(length);
    switch (_sourceDataType.getStoredType()) {
      case INT:
        int[][] intValuesMV = _transformFunction.transformToIntValuesMV(valueBlock);
        ArrayCopyUtils.copyToBoolean(intValuesMV, _intValuesMV, length);
        break;
      case LONG:
        long[][] longValuesMV = _transformFunction.transformToLongValuesMV(valueBlock);
        ArrayCopyUtils.copyToBoolean(longValuesMV, _intValuesMV, length);
        break;
      case FLOAT:
        float[][] floatValuesMV = _transformFunction.transformToFloatValuesMV(valueBlock);
        ArrayCopyUtils.copyToBoolean(floatValuesMV, _intValuesMV, length);
        break;
      case DOUBLE:
        double[][] doubleValuesMV = _transformFunction.transformToDoubleValuesMV(valueBlock);
        ArrayCopyUtils.copyToBoolean(doubleValuesMV, _intValuesMV, length);
        break;
      case STRING:
        String[][] stringValuesMV = _transformFunction.transformToStringValuesMV(valueBlock);
        ArrayCopyUtils.copyToBoolean(stringValuesMV, _intValuesMV, length);
        break;
      default:
        throw new IllegalStateException(String.format("Cannot cast from MV %s to BOOLEAN", _sourceDataType));
    }
    return _intValuesMV;
  }

  @Override
  public long[][] transformToLongValuesMV(ValueBlock valueBlock) {
    switch (_resultMetadata.getDataType()) {
      case LONG:
        return _transformFunction.transformToLongValuesMV(valueBlock);
      case TIMESTAMP:
        return transformToTimestampValuesMV(valueBlock);
      default:
        return super.transformToLongValuesMV(valueBlock);
    }
  }

  // TODO: Add it to the interface
  private long[][] transformToTimestampValuesMV(ValueBlock valueBlock) {
    if (_sourceDataType.getStoredType() == DataType.STRING) {
      int length = valueBlock.getNumDocs();
      initLongValuesMV(length);
      String[][] stringValuesMV = _transformFunction.transformToStringValuesMV(valueBlock);
      ArrayCopyUtils.copyToTimestamp(stringValuesMV, _longValuesMV, length);
      return _longValuesMV;
    } else {
      return _transformFunction.transformToLongValuesMV(valueBlock);
    }
  }

  @Override
  public float[][] transformToFloatValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.FLOAT) {
      return _transformFunction.transformToFloatValuesMV(valueBlock);
    } else {
      return super.transformToFloatValuesMV(valueBlock);
    }
  }

  @Override
  public double[][] transformToDoubleValuesMV(ValueBlock valueBlock) {
    if (_resultMetadata.getDataType().getStoredType() == DataType.DOUBLE) {
      return _transformFunction.transformToDoubleValuesMV(valueBlock);
    } else {
      return super.transformToDoubleValuesMV(valueBlock);
    }
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    DataType resultDataType = _resultMetadata.getDataType();
    if (resultDataType.getStoredType() == DataType.STRING) {
      switch (_sourceDataType) {
        case BOOLEAN:
          int length = valueBlock.getNumDocs();
          initStringValuesMV(length);
          int[][] intValuesMV = _transformFunction.transformToIntValuesMV(valueBlock);
          ArrayCopyUtils.copyFromBoolean(intValuesMV, _stringValuesMV, length);
          return _stringValuesMV;
        case TIMESTAMP:
          length = valueBlock.getNumDocs();
          initStringValuesMV(length);
          long[][] longValuesMV = _transformFunction.transformToLongValuesMV(valueBlock);
          ArrayCopyUtils.copyFromTimestamp(longValuesMV, _stringValuesMV, length);
          return _stringValuesMV;
        default:
          return _transformFunction.transformToStringValuesMV(valueBlock);
      }
    } else {
      int length = valueBlock.getNumDocs();
      initStringValuesMV(length);
      switch (resultDataType) {
        case INT:
          int[][] intValuesMV = _transformFunction.transformToIntValuesMV(valueBlock);
          ArrayCopyUtils.copy(intValuesMV, _stringValuesMV, length);
          break;
        case LONG:
          long[][] longValuesMV = _transformFunction.transformToLongValuesMV(valueBlock);
          ArrayCopyUtils.copy(longValuesMV, _stringValuesMV, length);
          break;
        case FLOAT:
          float[][] floatValuesMV = _transformFunction.transformToFloatValuesMV(valueBlock);
          ArrayCopyUtils.copy(floatValuesMV, _stringValuesMV, length);
          break;
        case DOUBLE:
          double[][] doubleValuesMV = _transformFunction.transformToDoubleValuesMV(valueBlock);
          ArrayCopyUtils.copy(doubleValuesMV, _stringValuesMV, length);
          break;
        case BOOLEAN:
          intValuesMV = transformToBooleanValuesMV(valueBlock);
          ArrayCopyUtils.copyFromBoolean(intValuesMV, _stringValuesMV, length);
          break;
        case TIMESTAMP:
          longValuesMV = transformToTimestampValuesMV(valueBlock);
          ArrayCopyUtils.copyFromTimestamp(longValuesMV, _stringValuesMV, length);
          break;
        default:
          throw new IllegalStateException(String.format("Cannot cast from MV %s to STRING", resultDataType));
      }
    }
    return _stringValuesMV;
  }
}
