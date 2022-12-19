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

import static java.nio.charset.StandardCharsets.UTF_8;


public class StringLongPair extends ValueLongPair<String> {

  public StringLongPair(String value, long time) {
    super(value, time);
  }

  public static StringLongPair fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static StringLongPair fromByteBuffer(ByteBuffer byteBuffer) {
    byte[] stringBytes = new byte[byteBuffer.getInt()];
    byteBuffer.get(stringBytes);
    return new StringLongPair(new String(stringBytes, UTF_8), byteBuffer.getLong());
  }

  @Override
  public byte[] toBytes() {
    byte[] stringBytes = _value.getBytes(UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + stringBytes.length + Long.BYTES);
    byteBuffer.putInt(stringBytes.length);
    byteBuffer.put(stringBytes);
    byteBuffer.putLong(_time);
    return byteBuffer.array();
  }
}
