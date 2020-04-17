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
 * Defines a single record in Pinot.
 * <p>Record may contain both single-value and multi-value columns. In order to use the record as the key in a map, it
 * can only contain single-value columns (to avoid using Arrays.deepEquals() and Arrays.deepHashCode() for performance
 * concern).
 * <p>For each data type, the value should be stored as:
 * <ul>
 *   <li>INT: Integer</li>
 *   <li>LONG: Long</li>
 *   <li>FLOAT: Float</li>
 *   <li>DOUBLE: Double</li>
 *   <li>STRING: String</li>
 *   <li>BYTES: ByteArray</li>
 *   <li>OBJECT (intermediate aggregation result): Object</li>
 *   <li>INT_ARRAY: int[]</li>
 *   <li>LONG_ARRAY: long[]</li>
 *   <li>FLOAT_ARRAY: float[]</li>
 *   <li>DOUBLE_ARRAY: double[]</li>
 *   <li>STRING_ARRAY: String[]</li>
 * </ul>
 */
public class Record {
  private final Object[] _values;

  public Record(Object[] values) {
    _values = values;
  }

  public Object[] getValues() {
    return _values;
  }

  // NOTE: Not check class for performance concern
  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    return Arrays.equals(_values, ((Record) o)._values);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_values);
  }
}
