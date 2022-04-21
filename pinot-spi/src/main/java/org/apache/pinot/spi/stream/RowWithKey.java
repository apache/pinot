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

package org.apache.pinot.spi.stream;

import java.util.Arrays;
import javax.annotation.Nullable;


/**
 * Helper class so the key and payload can be easily tied together instead of using pair
 */
public class RowWithKey {
  private final byte[] _key;
  private final byte[] _payload;

  public RowWithKey(@Nullable byte[] key, byte[] payload) {
    _key = key;
    _payload = payload;
  }

  public byte[] getKey() {
    return _key;
  }

  public byte[] getPayload() {
    return _payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowWithKey that = (RowWithKey) o;
    return Arrays.equals(_key, that._key) && Arrays.equals(_payload, that._payload);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(_key);
    result = 31 * result + Arrays.hashCode(_payload);
    return result;
  }
}
