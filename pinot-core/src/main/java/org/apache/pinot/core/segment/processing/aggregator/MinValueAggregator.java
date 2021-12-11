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
package org.apache.pinot.core.segment.processing.aggregator;

import org.apache.pinot.spi.data.FieldSpec;


/**
 * Min value aggregator
 */
public class MinValueAggregator implements ValueAggregator {

  private final FieldSpec.DataType _dataType;

  public MinValueAggregator(FieldSpec.DataType dataType) {
    _dataType = dataType;
  }

  @Override
  public Object aggregate(Object value1, Object value2) {
    Object result;
    switch (_dataType) {
      case INT:
        result = Math.min(((Number) value1).intValue(), ((Number) value2).intValue());
        break;
      case LONG:
        result = Math.min(((Number) value1).longValue(), ((Number) value2).longValue());
        break;
      case FLOAT:
        result = Math.min(((Number) value1).floatValue(), ((Number) value2).floatValue());
        break;
      case DOUBLE:
        result = Math.min(((Number) value1).doubleValue(), ((Number) value2).doubleValue());
        break;
      default:
        throw new IllegalArgumentException("Unsupported metric type : " + _dataType);
    }
    return result;
  }
}
