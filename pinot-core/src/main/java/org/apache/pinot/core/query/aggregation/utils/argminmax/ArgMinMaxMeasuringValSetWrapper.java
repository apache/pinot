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
 * Wrapper class for measuring columns in argmin/max aggregation function.
 * Meanly used to do comparison without boxing primitive types.
 */
public class ArgMinMaxMeasuringValSetWrapper extends ArgMinMaxWrapperValSet {

  public ArgMinMaxMeasuringValSetWrapper(boolean isSingleValue, DataSchema.ColumnDataType dataType,
      BlockValSet blockValSet) {
    super(dataType, isSingleValue);
    setNewBlock(blockValSet);
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
