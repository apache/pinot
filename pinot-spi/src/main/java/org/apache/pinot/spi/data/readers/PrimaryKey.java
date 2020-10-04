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


/**
 * The primary key of a record.
 */
public class PrimaryKey {
  private final Object[] _fields;

  public PrimaryKey(Object[] fields) {
    _fields = fields;
  }

  public Object[] getFields() {
    return _fields;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof PrimaryKey) {
      PrimaryKey that = (PrimaryKey) obj;
      return Arrays.equals(_fields, that._fields);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_fields);
  }

  @Override
  public String toString() {
    return Arrays.toString(_fields);
  }
}
