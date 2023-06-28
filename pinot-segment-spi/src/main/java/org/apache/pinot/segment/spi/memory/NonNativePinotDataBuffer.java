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
package org.apache.pinot.segment.spi.memory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class NonNativePinotDataBuffer extends PinotDataBuffer {
  private final PinotDataBuffer _nativeBuffer;

  public NonNativePinotDataBuffer(PinotDataBuffer nativeBuffer) {
    super(nativeBuffer.isCloseable());
    _nativeBuffer = nativeBuffer;
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    PinotDataBuffer nativeView = _nativeBuffer.view(start, end);
    if (byteOrder == ByteOrder.nativeOrder()) {
      return nativeView;
    }
    return new NonNativePinotDataBuffer(nativeView);
  }

  /*
  Methods that require special byte order treatment
  */

  @Override
  public char getChar(int offset) {
    return Character.reverseBytes(_nativeBuffer.getChar(offset));
  }

  @Override
  public char getChar(long offset) {
    return Character.reverseBytes(_nativeBuffer.getChar(offset));
  }

  @Override
  public void putChar(int offset, char value) {
    _nativeBuffer.putChar(offset, Character.reverseBytes(value));
  }

  @Override
  public void putChar(long offset, char value) {
    _nativeBuffer.putChar(offset, Character.reverseBytes(value));
  }

  @Override
  public short getShort(int offset) {
    return Short.reverseBytes(_nativeBuffer.getShort(offset));
  }

  @Override
  public short getShort(long offset) {
    return Short.reverseBytes(_nativeBuffer.getShort(offset));
  }

  @Override
  public void putShort(int offset, short value) {
    _nativeBuffer.putShort(offset, Short.reverseBytes(value));
  }

  @Override
  public void putShort(long offset, short value) {
    _nativeBuffer.putShort(offset, Short.reverseBytes(value));
  }

  @Override
  public int getInt(int offset) {
    return Integer.reverseBytes(_nativeBuffer.getInt(offset));
  }

  @Override
  public int getInt(long offset) {
    return Integer.reverseBytes(_nativeBuffer.getInt(offset));
  }

  @Override
  public void putInt(int offset, int value) {
    _nativeBuffer.putInt(offset, Integer.reverseBytes(value));
  }

  @Override
  public void putInt(long offset, int value) {
    _nativeBuffer.putInt(offset, Integer.reverseBytes(value));
  }

  @Override
  public long getLong(int offset) {
    return Long.reverseBytes(_nativeBuffer.getLong(offset));
  }

  @Override
  public long getLong(long offset) {
    return Long.reverseBytes(_nativeBuffer.getLong(offset));
  }

  @Override
  public void putLong(int offset, long value) {
    _nativeBuffer.putLong(offset, Long.reverseBytes(value));
  }

  @Override
  public void putLong(long offset, long value) {
    _nativeBuffer.putLong(offset, Long.reverseBytes(value));
  }

  @Override
  public float getFloat(int offset) {
    return Float.intBitsToFloat(Integer.reverseBytes(_nativeBuffer.getInt(offset)));
  }

  @Override
  public float getFloat(long offset) {
    return Float.intBitsToFloat(Integer.reverseBytes(_nativeBuffer.getInt(offset)));
  }

  @Override
  public void putFloat(int offset, float value) {
    _nativeBuffer.putInt(offset, Integer.reverseBytes(Float.floatToRawIntBits(value)));
  }

  @Override
  public void putFloat(long offset, float value) {
    _nativeBuffer.putInt(offset, Integer.reverseBytes(Float.floatToRawIntBits(value)));
  }

  @Override
  public double getDouble(int offset) {
    return Double.longBitsToDouble(Long.reverseBytes(_nativeBuffer.getLong(offset)));
  }

  @Override
  public double getDouble(long offset) {
    return Double.longBitsToDouble(Long.reverseBytes(_nativeBuffer.getLong(offset)));
  }

  @Override
  public void putDouble(int offset, double value) {
    _nativeBuffer.putLong(offset, Long.reverseBytes(Double.doubleToRawLongBits(value)));
  }

  @Override
  public void putDouble(long offset, double value) {
    _nativeBuffer.putLong(offset, Long.reverseBytes(Double.doubleToRawLongBits(value)));
  }

  @Override
  public ByteOrder order() {
    return NON_NATIVE_ORDER;
  }

  /*
   Methods that can be directly delegated on the native buffer
   */

  @Override
  public byte getByte(int offset) {
    return _nativeBuffer.getByte(offset);
  }

  @Override
  public byte getByte(long offset) {
    return _nativeBuffer.getByte(offset);
  }

  @Override
  public void putByte(int offset, byte value) {
    _nativeBuffer.putByte(offset, value);
  }

  @Override
  public void putByte(long offset, byte value) {
    _nativeBuffer.putByte(offset, value);
  }

  @Override
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    _nativeBuffer.copyTo(offset, buffer, destOffset, size);
  }

  @Override
  public void copyTo(long offset, byte[] buffer) {
    _nativeBuffer.copyTo(offset, buffer);
  }

  @Override
  public void copyTo(long offset, PinotDataBuffer buffer, long destOffset, long size) {
    _nativeBuffer.copyTo(offset, buffer, destOffset, size);
  }

  @Override
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    _nativeBuffer.readFrom(offset, buffer, srcOffset, size);
  }

  @Override
  public void readFrom(long offset, byte[] buffer) {
    _nativeBuffer.readFrom(offset, buffer);
  }

  @Override
  public void readFrom(long offset, ByteBuffer buffer) {
    _nativeBuffer.readFrom(offset, buffer);
  }

  @Override
  public void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException {
    _nativeBuffer.readFrom(offset, file, srcOffset, size);
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    return _nativeBuffer.toDirectByteBuffer(offset, size, byteOrder);
  }

  @Override
  public long size() {
    return _nativeBuffer.size();
  }

  @Override
  public void flush() {
    _nativeBuffer.flush();
  }

  @Override
  public void release()
      throws IOException {
    _nativeBuffer.release();
  }
}
