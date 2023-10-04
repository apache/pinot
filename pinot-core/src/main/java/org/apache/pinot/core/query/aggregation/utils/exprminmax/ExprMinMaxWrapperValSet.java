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

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;


/**
 * Wrapper class for the value sets of the column to do exprmin/max on.
 * This class is used for type-generic implementation of exprmin/max.
 */
public class ExprMinMaxWrapperValSet {
  protected final DataSchema.ColumnDataType _dataType;
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

  public ExprMinMaxWrapperValSet(
      DataSchema.ColumnDataType dataType, boolean isSingleValue) {
    _dataType = dataType;
    _isSingleValue = isSingleValue;
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
}
