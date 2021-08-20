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

public class BooleanUtils {
  private BooleanUtils() {
  }

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

  /**
   * Returns the int value (1 for true, 0 for false) for the given boolean string.
   * <ul>
   *   <li>'true', '1' (internal representation) -> '1'</li>
   *   <li>Others -> '0'</li>
   * </ul>
   */
  public static int toInt(String booleanString) {
    return toBoolean(booleanString) ? 1 : 0;
  }
}
