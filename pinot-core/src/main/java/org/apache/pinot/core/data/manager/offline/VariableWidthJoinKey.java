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
package org.apache.pinot.core.data.manager.offline;

import java.util.Arrays;
import org.apache.pinot.spi.utils.ByteArray;


public final class VariableWidthJoinKey implements JoinKey {
  private final Object[] _key;
  private int _index;

  public VariableWidthJoinKey(int size) {
    _key = new Object[size];
  }

  @Override
  public void reset() {
    Arrays.fill(_key, null);
    _index = 0;
  }

  @Override
  public void set(int value) {
    _key[_index++] = value;
  }

  @Override
  public void set(long value) {
    _key[_index++] = value;
  }

  @Override
  public void set(float value) {
    _key[_index++] = value;
  }

  @Override
  public void set(double value) {
    _key[_index++] = value;
  }

  @Override
  public void set(String value) {
    _key[_index++] = value;
  }

  @Override
  public void set(byte[] value) {
    _key[_index++] = new ByteArray(value);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof VariableWidthJoinKey) {
      VariableWidthJoinKey that = (VariableWidthJoinKey) o;
      return Arrays.equals(_key, that._key);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_key);
  }

  @Override
  public String toString() {
    return Arrays.toString(_key);
  }
}
