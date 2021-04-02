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
package org.apache.pinot.core.operator.filter.predicate;

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


public class PredicateUtils {

  /**
   * Converts the given predicate value to the stored value based on the data type.
   */
  public static String getStoredValue(String value, DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return getStoredBooleanValue(value);
      case TIMESTAMP:
        return getStoredTimestampValue(value);
      default:
        return value;
    }
  }

  /**
   * Converts the given boolean predicate value to the inner representation (int).
   */
  public static String getStoredBooleanValue(String booleanValue) {
    return Integer.toString(BooleanUtils.toInt(booleanValue));
  }

  /**
   * Converts the given timestamp predicate value to the inner representation (millis since epoch).
   */
  public static String getStoredTimestampValue(String timestampValue) {
    return Long.toString(TimestampUtils.toMillisSinceEpoch(timestampValue));
  }
}
