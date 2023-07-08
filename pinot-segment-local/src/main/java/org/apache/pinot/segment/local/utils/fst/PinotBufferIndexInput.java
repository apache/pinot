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
package org.apache.pinot.segment.local.utils.fst;

import java.io.IOException;
import org.apache.lucene.store.IndexInput;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * PinotBufferIndexInput is a wrapper around PinotDataBuffer implementing IndexInput apis.
 * It acts as a bridge between lucene FST reader library and PinotDataBuffer, it lets FST index
 * load into PinotDataBuffer and helps in utilizing FST reader (org.apache.lucene.util.fst.FST).
 *
 */
public class PinotBufferIndexInput extends IndexInput {
  private final PinotDataBuffer _pinotDataBuffer;
  private final long _sliceOffset;
  private final long _length;
  private long _readPointerOffset;

  public PinotBufferIndexInput(PinotDataBuffer pinotDataBuffer, long offset, long length) {
    super("");
    _pinotDataBuffer = pinotDataBuffer;
    _sliceOffset = offset;
    _readPointerOffset = offset;
    _length = length;
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public long getFilePointer() {
    return _readPointerOffset;
  }

  @Override
  public void seek(long l)
      throws IOException {
    _readPointerOffset = _sliceOffset + l;
  }

  @Override
  public long length() {
    return _length;
  }

  @Override
  public IndexInput slice(String s, long l, long l1)
      throws IOException {
    return new PinotBufferIndexInput(_pinotDataBuffer, _sliceOffset + l, l1);
  }

  @Override
  public byte readByte()
      throws IOException {
    byte b = _pinotDataBuffer.getByte(_readPointerOffset);
    _readPointerOffset += 1;
    return b;
  }

  @Override
  public void readBytes(byte[] bytes, int destOffset, int length)
      throws IOException {
    for (int i = 0; i < length; i++) {
      bytes[destOffset] = readByte();
      destOffset += 1;
    }
  }
}
