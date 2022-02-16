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

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.pinot.spi.data.FieldSpec;


public final class FixedWidthJoinKey implements JoinKey {

  private final ByteBuffer _keys;

  public FixedWidthJoinKey(FieldSpec.DataType... types) {
    int size = 0;
    for (FieldSpec.DataType type : types) {
      size += type.getStoredType().size();
    }
    _keys = ByteBuffer.allocate(size);
  }

  @Override
  public void reset() {
    _keys.position(0);
  }

  @Override
  public void set(int value) {
    _keys.putInt(value);
  }

  @Override
  public void set(long value) {
    _keys.putLong(value);
  }

  @Override
  public void set(float value) {
    _keys.putFloat(value);
  }

  @Override
  public void set(double value) {
    _keys.putDouble(value);
  }

  @Override
  public void set(String value) {
    throw new UnsupportedOperationException("cannot set String");
  }

  @Override
  public void set(byte[] value) {
    throw new UnsupportedOperationException("cannot set byte[]");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof FixedWidthJoinKey) {
      FixedWidthJoinKey that = (FixedWidthJoinKey) o;
      return Arrays.equals(_keys.array(), that._keys.array());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    for (int i = 0; i < _keys.capacity(); i += Integer.BYTES) {
      hash = 241 * hash + _keys.getInt(i);
    }
    return hash;
  }
}
