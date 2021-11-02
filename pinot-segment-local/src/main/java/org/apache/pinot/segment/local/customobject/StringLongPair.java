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


public class StringLongPair extends ValueLongPair<String> {

  public StringLongPair(String value, long time) {
    super(value, time);
  }

  public static StringLongPair fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

  public static StringLongPair fromByteBuffer(ByteBuffer byteBuffer) {
    int len = byteBuffer.getInt();
    byte[] bytes = new byte[len];
    byteBuffer.get(bytes);
    return new StringLongPair(new String(bytes), byteBuffer.getLong());
  }

  @Override
  public byte[] toBytes() {
    int len = _value.length();
    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + len + Long.BYTES);
    byteBuffer.putInt(len);
    byteBuffer.put(_value.getBytes());
    byteBuffer.putLong(_time);
    return byteBuffer.array();
  }
}
