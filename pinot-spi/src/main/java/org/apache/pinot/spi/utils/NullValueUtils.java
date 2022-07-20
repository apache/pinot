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
package org.apache.pinot.spi.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class NullValueUtils {
  private NullValueUtils() {
  }

  private static final Map<DataType, Object> FIELD_TYPE_DEFAULT_NULL_VALUE_MAP = new HashMap<>();

  static {
    DataType[] types = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.BIG_DECIMAL,
        DataType.BOOLEAN, DataType.TIMESTAMP, DataType.STRING, DataType.JSON, DataType.BYTES};
    for (DataType type : types) {
      FieldSpec.FieldType fieldType = type.equals(FieldSpec.DataType.BIG_DECIMAL)
          ? FieldSpec.FieldType.METRIC : FieldSpec.FieldType.DIMENSION;
      FIELD_TYPE_DEFAULT_NULL_VALUE_MAP.put(type, FieldSpec.getDefaultNullValue(fieldType, type, null));
    }
  }

  public static Object getDefaultNullValue(FieldSpec.DataType dataType) {
    return FIELD_TYPE_DEFAULT_NULL_VALUE_MAP.get(dataType);
  }
}
