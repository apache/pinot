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
package org.apache.pinot.core.query.aggregation.utils.argminmax;

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;


public class ArgMinMaxWrapperValSet {
  private final DataSchema.ColumnDataType _dataType;
  boolean _isSingleValue;

  int[] _intValues;
  long[] _longValues;
  float[] _floatValues;
  double[] _doublesValues;
  Object[] _objectsValues;

  int[][] _intValuesMV;
  long[][] _longValuesMV;
  float[][] _floatValuesMV;
  double[][] _doublesValuesMV;
  Object[][] _objectsValuesMV;

  public ArgMinMaxWrapperValSet(boolean isSingleValue, DataSchema.ColumnDataType dataType, BlockValSet blockValSet) {
    _dataType = dataType;
    _isSingleValue = isSingleValue;
    setNewBlock(blockValSet);
  }

  public void setNewBlock(BlockValSet blockValSet) {
    if (_isSingleValue) {
      switch (_dataType) {
        case INT:
        case BOOLEAN:
          _intValues = blockValSet.getIntValuesSV();
          break;
        case LONG:
        case TIMESTAMP:
          _longValues = blockValSet.getLongValuesSV();
          break;
        case FLOAT:
          _floatValues = blockValSet.getFloatValuesSV();
          break;
        case DOUBLE:
          _doublesValues = blockValSet.getDoubleValuesSV();
          break;
        case STRING:
        case JSON:
          _objectsValues = blockValSet.getStringValuesSV();
          break;
        case BIG_DECIMAL:
          _objectsValues = blockValSet.getBigDecimalValuesSV();
          break;
        case BYTES:
          _objectsValues = blockValSet.getBytesValuesSV();
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + _dataType);
      }
    } else {
      switch (_dataType) {
        case INT_ARRAY:
        case BOOLEAN_ARRAY:
          _intValuesMV = blockValSet.getIntValuesMV();
          break;
        case LONG_ARRAY:
        case TIMESTAMP_ARRAY:
          _longValuesMV = blockValSet.getLongValuesMV();
          break;
        case FLOAT_ARRAY:
          _floatValuesMV = blockValSet.getFloatValuesMV();
          break;
        case DOUBLE_ARRAY:
          _doublesValuesMV = blockValSet.getDoubleValuesMV();
          break;
        case STRING_ARRAY:
          _objectsValuesMV = blockValSet.getStringValuesMV();
          break;
        case BYTES_ARRAY:
          _objectsValuesMV = blockValSet.getBytesValuesMV();
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + _dataType);
      }
    }
  }

  public Comparable getComparable(int i) {
    switch (_dataType) {
      case INT:
      case BOOLEAN:
        return _intValues[i];
      case LONG:
      case TIMESTAMP:
        return _longValues[i];
      case FLOAT:
        return _floatValues[i];
      case DOUBLE:
        return _doublesValues[i];
      case STRING:
      case BIG_DECIMAL:
        return (Comparable) _objectsValues[i];
      default:
        throw new IllegalStateException("Unsupported data type: " + _dataType);
    }
  }

  public Object getValue(int i) {
    switch (_dataType) {
      case INT:
      case BOOLEAN:
        return _intValues[i];
      case LONG:
      case TIMESTAMP:
        return _longValues[i];
      case FLOAT:
        return _floatValues[i];
      case DOUBLE:
        return _doublesValues[i];
      case STRING:
      case BIG_DECIMAL:
      case BYTES:
      case JSON:
          return _objectsValues[i];
      case INT_ARRAY:
        return _intValuesMV[i];
      case LONG_ARRAY:
      case TIMESTAMP_ARRAY:
        return _longValuesMV[i];
      case FLOAT_ARRAY:
        return _floatValuesMV[i];
      case DOUBLE_ARRAY:
        return _doublesValuesMV[i];
      case STRING_ARRAY:
      case BYTES_ARRAY:
        return _objectsValuesMV[i];
      default:
        throw new IllegalStateException("Unsupported data type: " + _dataType);
    }
  }

  public int compare(int i, Object o) {
      switch (_dataType) {
        case INT:
        case BOOLEAN:
          return Integer.compare((Integer) o, _intValues[i]);
        case LONG:
        case TIMESTAMP:
          return Long.compare((Long) o, _longValues[i]);
        case FLOAT:
          return Float.compare((Float) o, _floatValues[i]);
        case DOUBLE:
          return Double.compare((Double) o, _doublesValues[i]);
        case STRING:
          return ((String) o).compareTo((String) _objectsValues[i]);
        case BIG_DECIMAL:
          return ((java.math.BigDecimal) o).compareTo((java.math.BigDecimal) _objectsValues[i]);
        default:
          throw new IllegalStateException("Unsupported data type in comparison" + _dataType);
      }
  }
}
