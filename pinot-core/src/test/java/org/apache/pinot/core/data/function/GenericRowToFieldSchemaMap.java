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
package org.apache.pinot.core.data.function;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * This class helps test various classes that require a schema to function,
 * but just produce {@code GenericRow}. It makes a "best effort" attempt and
 * users of this should take care to tailor it to their test use case.
 */
public class GenericRowToFieldSchemaMap {

  private GenericRowToFieldSchemaMap() {
  }

  public static Map<String, FieldSpec> inferFieldMap(GenericRow row) {
    return inferFieldMapWithDefaultType(row, null, false);
  }

  public static Map<String, FieldSpec> inferFieldMapWithDefaultType(
      GenericRow row, FieldSpec.DataType defaultType, boolean defaultMV
  ) {
    ImmutableMap.Builder<String, FieldSpec> builder = ImmutableMap.<String, FieldSpec>builder();

    for (Map.Entry<String, Object> entry : row.getFieldToValueMap().entrySet()) {
      if (entry.getValue() != null) {
        // simply ignore null values for now, have the calling test add them
        builder.put(entry.getKey(), extractFieldSpec(entry.getKey(), entry.getValue()));
      } else if (defaultType != null) {
        builder.put(entry.getKey(), new DimensionFieldSpec(entry.getKey(), defaultType, defaultMV));
      }
    }

    return builder.build();
  }

  private static FieldSpec extractFieldSpec(String key, Object value) {
    if (value == null) {
      throw new IllegalArgumentException("Unexpected NULL value");
    } else if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
      return new DimensionFieldSpec(key, FieldSpec.DataType.INT, true);
    } else if (value instanceof Long) {
      return new DimensionFieldSpec(key, FieldSpec.DataType.LONG, true);
    } else if (value instanceof Float) {
      return new DimensionFieldSpec(key, FieldSpec.DataType.FLOAT, true);
    } else if (value instanceof Double) {
      return new DimensionFieldSpec(key, FieldSpec.DataType.DOUBLE, true);
    } else if (value instanceof Timestamp) {
      return new DimensionFieldSpec(key, FieldSpec.DataType.TIMESTAMP, true);
    } else if (value instanceof byte[]) {
      return new DimensionFieldSpec(key, FieldSpec.DataType.BYTES, true);
    } else if (value instanceof String) {
      return new DimensionFieldSpec(key, FieldSpec.DataType.STRING, true);
    } else if (value instanceof Object[]) {
      if (((Object[]) value).length == 0) {
        throw new IllegalStateException("cannot infer type for empty array");
      }
      Object o = ((Object[]) value)[0];
      if (o == null) {
        throw new IllegalStateException("cannot infer type for array with null values");
      }

      FieldSpec singleFieldSpec = extractFieldSpec(key, o);
      return new DimensionFieldSpec(key, singleFieldSpec.getDataType(), false);
    } else if (value.getClass().isArray()) {
      // this is the case for primitive arrays, which need some special handling
      if (Array.getLength(value) == 0) {
        throw new IllegalStateException("cannot infer type for empty array");
      }
      Object o = Array.get(value, 0);
      FieldSpec fieldSpec = extractFieldSpec(key, o);
      return new DimensionFieldSpec(key, fieldSpec.getDataType(), false);
    }

    // assume string otherwise
    return new DimensionFieldSpec(key, FieldSpec.DataType.STRING, false);
  }
}
