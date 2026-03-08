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
package org.apache.pinot.segment.local.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.PinotDataType;


/**
 * Utility class for data type transformation operations shared between
 * DataTypeTransformer (record-level) and DataTypeColumnTransformer (column-level).
 */
@SuppressWarnings("rawtypes")
public class DataTypeTransformerUtils {

  private DataTypeTransformerUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Transforms a value to the destination data type.
   * This method standardizes the value, determines the source type, converts to the destination type,
   * and converts to internal representation.
   *
   * @param column The column name (used for error messages)
   * @param value The value to transform
   * @param destDataType The destination PinotDataType
   * @return The transformed value, or null if the standardized value is null
   */
  @Nullable
  public static Object transformValue(String column, @Nullable Object value, PinotDataType destDataType) {
    if (value == null) {
      return null;
    }

    // Standardize the value (except for JSON and MAP types)
    if (destDataType != PinotDataType.JSON && destDataType != PinotDataType.MAP) {
      value = standardize(column, value, destDataType.isSingleValue());
    }

    // NOTE: The standardized value could be null for empty Collection/Map/Object[].
    if (value == null) {
      return null;
    }

    // Determine the source data type
    PinotDataType sourceDataType;
    if (value instanceof Object[]) {
      // Multi-value column
      Object[] values = (Object[]) value;
      // JSON is not standardised for empty json array
      if (destDataType == PinotDataType.JSON && values.length == 0) {
        sourceDataType = PinotDataType.JSON;
      } else {
        sourceDataType = PinotDataType.getMultiValueType(values[0].getClass());
      }
    } else {
      // Single-value column
      sourceDataType = PinotDataType.getSingleValueType(value.getClass());
    }

    // Convert from source to destination type
    // Skipping conversion when srcType!=destType is speculative, and can be unsafe when
    // the array for MV column contains values of mixing types. Mixing types can lead
    // to ClassCastException during conversion, often aborting data ingestion jobs.
    //
    // So now, calling convert() unconditionally for safety. Perf impact is negligible:
    // 1. for SV column, when srcType=destType, the conversion is simply pass through.
    // 2. for MV column, when srcType=destType, the conversion is simply pass through
    // if the source type is not Object[] (but sth like Integer[], Double[]). For Object[],
    // the conversion loops through values in the array like before, but can catch the
    // ClassCastException if it happens and continue the conversion now.
    value = destDataType.convert(value, sourceDataType);
    value = destDataType.toInternal(value);

    return value;
  }

  /**
   * Standardize the value into supported types.
   * <ul>
   *   <li>Empty Collection/Map/Object[] will be standardized to null</li>
   *   <li>Single-entry Collection/Map/Object[] will be standardized to single value (map key is ignored)</li>
   *   <li>Multi-entries Collection/Map/Object[] will be standardized to Object[] (map key is ignored)</li>
   * </ul>
   */
  @VisibleForTesting
  @Nullable
  public static Object standardize(String column, @Nullable Object value, boolean isSingleValue) {
    if (value == null) {
      return null;
    }
    if (value instanceof Collection) {
      return standardizeCollection(column, (Collection) value, isSingleValue);
    }
    if (value instanceof Map) {
      return standardizeCollection(column, ((Map) value).values(), isSingleValue);
    }
    if (value instanceof Object[]) {
      Object[] values = (Object[]) value;
      int numValues = values.length;
      if (numValues == 0) {
        return null;
      }
      if (numValues == 1) {
        return standardize(column, values[0], isSingleValue);
      }
      List<Object> standardizedValues = new ArrayList<>(numValues);
      for (Object singleValue : values) {
        Object standardizedValue = standardize(column, singleValue, true);
        if (standardizedValue != null) {
          standardizedValues.add(standardizedValue);
        }
      }
      int numStandardizedValues = standardizedValues.size();
      if (numStandardizedValues == 0) {
        return null;
      }
      if (numStandardizedValues == 1) {
        return standardizedValues.get(0);
      }
      if (isSingleValue) {
        throw new IllegalArgumentException(
            "Cannot read single-value from Object[]: " + Arrays.toString(values) + " for column: " + column);
      }
      return standardizedValues.toArray();
    }
    return value;
  }

  private static Object standardizeCollection(String column, Collection collection, boolean isSingleValue) {
    int numValues = collection.size();
    if (numValues == 0) {
      return null;
    }
    if (numValues == 1) {
      return standardize(column, collection.iterator().next(), isSingleValue);
    }
    List<Object> standardizedValues = new ArrayList<>(numValues);
    for (Object singleValue : collection) {
      Object standardizedValue = standardize(column, singleValue, true);
      if (standardizedValue != null) {
        standardizedValues.add(standardizedValue);
      }
    }
    int numStandardizedValues = standardizedValues.size();
    if (numStandardizedValues == 0) {
      return null;
    }
    if (numStandardizedValues == 1) {
      return standardizedValues.get(0);
    }
    Preconditions.checkState(!isSingleValue, "Cannot read single-value from Collection: %s for column: %s", collection,
        column);
    return standardizedValues.toArray();
  }
}
