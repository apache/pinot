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
package org.apache.pinot.spi.data.readers;

import java.util.Arrays;
import org.apache.commons.lang3.SerializationUtils;


/**
 * The primary key of a record. Note that the value used in the primary key must be single-value.
 */
public class PrimaryKey {
  private final Object[] _values;

  public PrimaryKey(Object[] values) {
    _values = values;
  }

  public Object[] getValues() {
    return _values;
  }

  public byte[] asBytes() {
    return SerializationUtils.serialize(_values);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof PrimaryKey) {
      PrimaryKey that = (PrimaryKey) obj;
      return Arrays.equals(_values, that._values);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_values);
  }

  @Override
  public String toString() {
    return Arrays.toString(_values);
  }
}
