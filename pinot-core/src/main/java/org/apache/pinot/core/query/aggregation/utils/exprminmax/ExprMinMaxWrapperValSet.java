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
package org.apache.pinot.core.query.aggregation.utils.exprminmax;

import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Wrapper class for the value sets of the column to do exprmin/max on.
 * This class is used for type-generic implementation of exprmin/max.
 */
@SuppressWarnings("rawtypes")
public class ExprMinMaxWrapperValSet {
  final ColumnDataType _storedType;
  int[] _intValues;
  long[] _longValues;
  float[] _floatValues;
  double[] _doublesValues;
  Comparable[] _objectsValues;
  int[][] _intValuesMV;
  long[][] _longValuesMV;
  float[][] _floatValuesMV;
  double[][] _doublesValuesMV;
  Comparable[][] _objectsValuesMV;

  public ExprMinMaxWrapperValSet(ColumnDataType storedType) {
    _storedType = storedType;
  }

  public ColumnDataType getStoredType() {
    return _storedType;
  }

  public void setNewBlock(BlockValSet blockValSet) {
    switch (_storedType) {
      case INT:
        _intValues = blockValSet.getIntValuesSV();
        break;
      case LONG:
        _longValues = blockValSet.getLongValuesSV();
        break;
      case FLOAT:
        _floatValues = blockValSet.getFloatValuesSV();
        break;
      case DOUBLE:
        _doublesValues = blockValSet.getDoubleValuesSV();
        break;
      case BIG_DECIMAL:
        _objectsValues = blockValSet.getBigDecimalValuesSV();
        break;
      case STRING:
        _objectsValues = blockValSet.getStringValuesSV();
        break;
      case BYTES: {
        // Wrap to ByteArray so values stay Comparable and match the DataTable BYTES serialization convention.
        byte[][] bytesSV = blockValSet.getBytesValuesSV();
        ByteArray[] byteArrays = new ByteArray[bytesSV.length];
        for (int i = 0; i < bytesSV.length; i++) {
          byteArrays[i] = new ByteArray(bytesSV[i]);
        }
        _objectsValues = byteArrays;
        break;
      }
      case INT_ARRAY:
        _intValuesMV = blockValSet.getIntValuesMV();
        break;
      case LONG_ARRAY:
        _longValuesMV = blockValSet.getLongValuesMV();
        break;
      case FLOAT_ARRAY:
        _floatValuesMV = blockValSet.getFloatValuesMV();
        break;
      case DOUBLE_ARRAY:
        _doublesValuesMV = blockValSet.getDoubleValuesMV();
        break;
      case BIG_DECIMAL_ARRAY:
        _objectsValuesMV = blockValSet.getBigDecimalValuesMV();
        break;
      case STRING_ARRAY:
        _objectsValuesMV = blockValSet.getStringValuesMV();
        break;
      case BYTES_ARRAY: {
        // Wrap to ByteArray[] so values match the DataTable BYTES_ARRAY serialization convention.
        byte[][][] bytesMV = blockValSet.getBytesValuesMV();
        ByteArray[][] byteArraysMV = new ByteArray[bytesMV.length][];
        for (int i = 0; i < bytesMV.length; i++) {
          byte[][] value = bytesMV[i];
          ByteArray[] byteArrays = new ByteArray[value.length];
          for (int j = 0; j < value.length; j++) {
            byteArrays[j] = new ByteArray(value[j]);
          }
          byteArraysMV[i] = byteArrays;
        }
        _objectsValuesMV = byteArraysMV;
        break;
      }
      default:
        throw new IllegalStateException("Unsupported stored type: " + _storedType);
    }
  }
}
