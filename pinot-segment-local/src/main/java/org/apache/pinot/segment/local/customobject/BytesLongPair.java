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
package org.apache.pinot.segment.local.customobject;

import java.nio.ByteBuffer;
import org.apache.pinot.spi.utils.ByteArray;


/// A BYTES value with its time, for FIRST_WITH_TIME / LAST_WITH_TIME. UUID values use the same pair.
public class BytesLongPair extends ValueLongPair<ByteArray> {

  public BytesLongPair(ByteArray value, long time) {
    super(value, time);
  }

  public static BytesLongPair fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static BytesLongPair fromByteBuffer(ByteBuffer byteBuffer) {
    byte[] valueBytes = new byte[byteBuffer.getInt()];
    byteBuffer.get(valueBytes);
    return new BytesLongPair(new ByteArray(valueBytes), byteBuffer.getLong());
  }

  @Override
  public byte[] toBytes() {
    byte[] valueBytes = _value.getBytes();
    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + valueBytes.length + Long.BYTES);
    byteBuffer.putInt(valueBytes.length);
    byteBuffer.put(valueBytes);
    byteBuffer.putLong(_time);
    return byteBuffer.array();
  }
}
