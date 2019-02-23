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
package com.linkedin.pinot.opal.common.keyValueStore;

import java.util.Arrays;

public class ByteArrayWrapper {
  private final byte[] _data;

  public ByteArrayWrapper(byte[] data) {
    if (data == null) {
      throw new NullPointerException();
    }
    _data = data;
  }

  public byte[] getData() {
    return _data;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ByteArrayWrapper)) {
      return false;
    }
    return Arrays.equals(this._data, ((ByteArrayWrapper) other)._data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_data);
  }
}
