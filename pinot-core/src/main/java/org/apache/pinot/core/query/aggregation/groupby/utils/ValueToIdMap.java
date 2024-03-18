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

/**
 * Interface for mapping primitive values to contiguous id's.
 */
public interface ValueToIdMap {
  int INVALID_KEY = -1;

  default int put(int value) {
    throw new UnsupportedOperationException();
  }

  default int put(long value) {
    throw new UnsupportedOperationException();
  }

  default int put(float value) {
    throw new UnsupportedOperationException();
  }

  default int put(double value) {
    throw new UnsupportedOperationException();
  }

  int put(Object value);

  default int getId(int value) {
    throw new UnsupportedOperationException();
  }

  default int getId(long value) {
    throw new UnsupportedOperationException();
  }

  default int getId(float value) {
    throw new UnsupportedOperationException();
  }

  default int getId(double value) {
    throw new UnsupportedOperationException();
  }

  int getId(Object value);

  /**
   * Returns the value for the given id.
   * <p>The Object type returned for each value type:
   * <ul>
   *   <li>INT -> Integer</li>
   *   <li>LONG -> Long</li>
   *   <li>FLOAT -> Float</li>
   *   <li>DOUBLE -> Double</li>
   *   <li>BIG_DECIMAL -> BigDecimal</li>
   *   <li>STRING -> String</li>
   *   <li>BYTES -> ByteArray</li>
   * </ul>
   */
  Object get(int id);
}
