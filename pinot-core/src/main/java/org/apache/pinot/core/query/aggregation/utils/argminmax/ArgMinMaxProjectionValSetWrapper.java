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


/**
 * Wrapper class for projection block value set for argmin/max aggregation function.
 * Used to get the value from val set of different data types.
 */
public class ArgMinMaxProjectionValSetWrapper extends ArgMinMaxWrapperValSet {

  public ArgMinMaxProjectionValSetWrapper(boolean isSingleValue, DataSchema.ColumnDataType dataType,
      BlockValSet blockValSet) {
    super(dataType, isSingleValue);
    setNewBlock(blockValSet);
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
        return _intValuesMV[i].length == 0 ? null : _intValuesMV[i];
      case LONG_ARRAY:
      case TIMESTAMP_ARRAY:
        return _longValuesMV[i].length == 0 ? null : _longValuesMV[i];
      case FLOAT_ARRAY:
        return _floatValuesMV[i].length == 0 ? null : _floatValuesMV[i];
      case DOUBLE_ARRAY:
        return _doublesValuesMV[i].length == 0 ? null : _doublesValuesMV[i];
      case STRING_ARRAY:
      case BYTES_ARRAY:
        return _objectsValuesMV[i].length == 0 ? null : _objectsValuesMV[i];
      default:
        throw new IllegalStateException("Unsupported data type: " + _dataType);
    }
  }
}
