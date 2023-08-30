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

import javax.annotation.Nullable;


public class BooleanUtils {
  private BooleanUtils() {
  }

  public static final int INTERNAL_TRUE = 1;
  public static final int INTERNAL_FALSE = 0;

  /**
   * Returns the boolean value for the given boolean string.
   * <ul>
   *   <li>'true', '1' (internal representation) -> true</li>
   *   <li>Others -> false</li>
   * </ul>
   */
  public static boolean toBoolean(String booleanString) {
    return booleanString.equalsIgnoreCase("true") || booleanString.equals("1");
  }

  public static boolean toBoolean(Object booleanObject) {
    if (booleanObject == null) {
      return false;
    } else if (booleanObject instanceof String) {
      return BooleanUtils.toBoolean((String) booleanObject);
    } else if (booleanObject instanceof Number) {
      return ((Number) booleanObject).intValue() == INTERNAL_TRUE;
    } else if (booleanObject instanceof Boolean) {
      return (boolean) booleanObject;
    } else {
      throw new IllegalArgumentException("Illegal type for boolean conversion: " + booleanObject.getClass());
    }
  }

  /**
   * Returns the int value (1 for true, 0 for false) for the given boolean string.
   * <ul>
   *   <li>'true', '1' (internal representation) -> '1'</li>
   *   <li>Others -> '0'</li>
   * </ul>
   */
  public static int toInt(String booleanString) {
    return toBoolean(booleanString) ? INTERNAL_TRUE : INTERNAL_FALSE;
  }

  /**
   * Returns the boolean value for the given non-null Integer object (internal value for BOOLEAN).
   */
  public static boolean fromNonNullInternalValue(Object value) {
    return (int) value == INTERNAL_TRUE;
  }

  /**
   * Returns whether the given nullable Integer object (internal value for BOOLEAN) represents true.
   */
  public static boolean isTrueInternalValue(@Nullable Object value) {
    return value != null && (int) value == INTERNAL_TRUE;
  }

  /**
   * Returns whether the given nullable Integer object (internal value for BOOLEAN) represents false.
   */
  public static boolean isFalseInternalValue(@Nullable Object value) {
    return value != null && (int) value == INTERNAL_FALSE;
  }
}
