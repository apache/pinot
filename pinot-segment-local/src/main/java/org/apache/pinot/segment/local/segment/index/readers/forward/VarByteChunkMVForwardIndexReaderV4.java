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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;


public class VarByteChunkMVForwardIndexReaderV4 extends VarByteChunkSVForwardIndexReaderV4 {
  public VarByteChunkMVForwardIndexReaderV4(PinotDataBuffer dataBuffer, FieldSpec.DataType storedType) {
    super(dataBuffer, storedType);
  }

  @Override
  public BigDecimal getBigDecimal(int docId, ReaderContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(int docId, ReaderContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes(int docId, ReaderContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSingleValue() {
    return false;
  }

  @Override
  public int getStringMV(int docId, String[] valueBuffer, VarByteChunkSVForwardIndexReaderV4.ReaderContext context) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(context.getValue(docId));
    int numValues = byteBuffer.getInt();
    int contentOffset = (numValues + 1) * Integer.BYTES;
    for (int i = 0; i < numValues; i++) {
      int length = byteBuffer.getInt((i + 1) * Integer.BYTES);
      byte[] bytes = new byte[length];
      byteBuffer.position(contentOffset);
      byteBuffer.get(bytes, 0, length);
      valueBuffer[i] = new String(bytes, StandardCharsets.UTF_8);
      contentOffset += length;
    }
    return numValues;
  }

  @Override
  public String[] getStringMV(int docId, VarByteChunkSVForwardIndexReaderV4.ReaderContext context) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(context.getValue(docId));
    int numValues = byteBuffer.getInt();
    String[] valueBuffer = new String[numValues];
    int contentOffset = (numValues + 1) * Integer.BYTES;
    for (int i = 0; i < numValues; i++) {
      int length = byteBuffer.getInt((i + 1) * Integer.BYTES);
      byte[] bytes = new byte[length];
      byteBuffer.position(contentOffset);
      byteBuffer.get(bytes, 0, length);
      valueBuffer[i] = new String(bytes, StandardCharsets.UTF_8);
      contentOffset += length;
    }
    return valueBuffer;
  }

  @Override
  public int getBytesMV(int docId, byte[][] valueBuffer, VarByteChunkSVForwardIndexReaderV4.ReaderContext context) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(context.getValue(docId));
    int numValues = byteBuffer.getInt();
    int contentOffset = (numValues + 1) * Integer.BYTES;
    for (int i = 0; i < numValues; i++) {
      int length = byteBuffer.getInt((i + 1) * Integer.BYTES);
      byte[] bytes = new byte[length];
      byteBuffer.position(contentOffset);
      byteBuffer.get(bytes, 0, length);
      valueBuffer[i] = bytes;
      contentOffset += length;
    }
    return numValues;
  }

  @Override
  public byte[][] getBytesMV(int docId, VarByteChunkSVForwardIndexReaderV4.ReaderContext context) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(context.getValue(docId));
    int numValues = byteBuffer.getInt();
    byte[][] valueBuffer = new byte[numValues][];
    int contentOffset = (numValues + 1) * Integer.BYTES;
    for (int i = 0; i < numValues; i++) {
      int length = byteBuffer.getInt((i + 1) * Integer.BYTES);
      byte[] bytes = new byte[length];
      byteBuffer.position(contentOffset);
      byteBuffer.get(bytes, 0, length);
      valueBuffer[i] = bytes;
      contentOffset += length;
    }
    return valueBuffer;
  }

  @Override
  public int getNumValuesMV(int docId, VarByteChunkSVForwardIndexReaderV4.ReaderContext context) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(context.getValue(docId));
    return byteBuffer.getInt();
  }
}
