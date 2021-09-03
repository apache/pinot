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
package org.apache.pinot.core.data.table;

import java.util.Arrays;


/**
 * Defines the key component of the record.
 * <p>Key can be used as the key in a map, and may only contain single-value columns.
 * <p>For each data type, the value should be stored as:
 * <ul>
 *   <li>INT: Integer</li>
 *   <li>LONG: Long</li>
 *   <li>FLOAT: Float</li>
 *   <li>DOUBLE: Double</li>
 *   <li>STRING: String</li>
 *   <li>BYTES: ByteArray</li>
 * </ul>
 *
 * TODO: Consider replacing Key with Record as the concept is very close and the implementation is the same
 */
public class Key {
  private final Object[] _values;

  public Key(Object[] values) {
    _values = values;
  }

  public Object[] getValues() {
    return _values;
  }

  // NOTE: Not check class for performance concern
  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    return Arrays.equals(_values, ((Key) o)._values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_values);
  }
}
