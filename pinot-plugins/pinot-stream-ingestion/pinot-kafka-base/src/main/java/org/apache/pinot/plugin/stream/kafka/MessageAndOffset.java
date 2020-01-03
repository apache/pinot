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
package org.apache.pinot.plugin.stream.kafka;

import java.nio.ByteBuffer;


public class MessageAndOffset {

  private ByteBuffer _message;
  private long _offset;

  public MessageAndOffset(byte[] message, long offset) {
    this(ByteBuffer.wrap(message), offset);
  }

  public MessageAndOffset(ByteBuffer message, long offset) {
    _message = message;
    _offset = offset;
  }

  public ByteBuffer getMessage() {
    return _message;
  }

  public long getOffset() {
    return _offset;
  }

  public long getNextOffset() {
    return getOffset() + 1;
  }

  public int payloadSize() {
    return getMessage().array().length;
  }
}
