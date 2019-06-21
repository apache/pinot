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
package org.apache.pinot.core.io.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.core.io.writer.impl.OffHeapByteArrayStore;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


/**
 * TODO Fill out this class with methods as needed
 */
public class VarLengthBytesValueReaderWriter implements Closeable, ValueReader {

  private OffHeapByteArrayStore _buffer;

  // Used for memory-mapping while loading a segment
  public VarLengthBytesValueReaderWriter(PinotDataBuffer pinotDataBuffer) {
    PinotDataBuffer valuesBuffer = readHeader(pinotDataBuffer);
    _buffer = new OffHeapByteArrayStore(valuesBuffer);
  }

  // Used to create one while building a segment
  public VarLengthBytesValueReaderWriter(byte[][] arrays, File dictionaryFile, long size) throws IOException {
    long sizeWithHeader = size + getHeaderSize(arrays);
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer
        .mapFile(dictionaryFile, false, 0, size, ByteOrder.BIG_ENDIAN,
            getClass().getSimpleName());
    writeHeader(pinotDataBuffer, arrays);
    _buffer = new OffHeapByteArrayStore(pinotDataBuffer);
    for (byte[] array : arrays) {
      _buffer.add(array);
    }
  }

  private int getHeaderSize(byte[][] arrays) {
    return 11;  // or whatever computed value for fixed/var sized headers, version etc..
  }

  private void writeHeader(PinotDataBuffer pinotDataBuffer, byte[][] arrays) {
    // write headers here, maybe header has some metadata about values.
  }

  private PinotDataBuffer readHeader(PinotDataBuffer pinotDataBuffer) {
    // slice the pinot databuffer afer reading headers, so that we just have the pinotdatabuffer
    // with the values.
    return null;
  }

  @Override
  public int getInt(int index) {
    return 0;
  }

  @Override
  public long getLong(int index) {
    return 0;
  }

  @Override
  public float getFloat(int index) {
    return 0;
  }

  @Override
  public double getDouble(int index) {
    return 0;
  }

  @Override
  public String getUnpaddedString(int index, int numBytesPerValue, byte paddingByte, byte[] buffer) {
    return null;
  }

  @Override
  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    return null;
  }

  @Override
  public byte[] getBytes(int index, int numBytesPerValue, byte[] buffer) {
    return new byte[0];
  }

  @Override
  public void close() throws IOException {

  }
}
