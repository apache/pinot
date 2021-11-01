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
package org.apache.pinot.core.query.aggregation.groupby.utils;

import java.math.BigDecimal;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Interface for mapping primitive values to contiguous id's.
 */
public interface ValueToIdMap {
  int INVALID_KEY = -1;

  int put(int value);

  int put(long value);

  int put(float value);

  int put(double value);

  int put(String value);

  int put(ByteArray value);

  int put(BigDecimal value);

  int getInt(int id);

  long getLong(int id);

  float getFloat(int id);

  double getDouble(int id);

  String getString(int id);

  ByteArray getBytes(int id);

  BigDecimal getBigDecimal(int id);

  /**
   * Returns the value for the given id.
   * <p>The Object type returned for each value type:
   * <ul>
   *   <li>INT -> Integer</li>
   *   <li>LONG -> Long</li>
   *   <li>FLOAT -> Float</li>
   *   <li>DOUBLE -> Double</li>
   *   <li>STRING -> String</li>
   *   <li>BYTES -> ByteArray</li>
   * </ul>
   */
  Object get(int id);
}
