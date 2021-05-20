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
import java.nio.ByteOrder;
import javax.annotation.concurrent.ThreadSafe;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;


@ThreadSafe
public class PinotNativeOrderLBuffer extends BasePinotLBuffer {

  static PinotNativeOrderLBuffer allocateDirect(long size) {
    LBufferAPI buffer = new LBuffer(size);
    return new PinotNativeOrderLBuffer(buffer, true, false);
  }

  static PinotNativeOrderLBuffer loadFile(File file, long offset, long size)
      throws IOException {
    PinotNativeOrderLBuffer buffer = allocateDirect(size);
    buffer.readFrom(0, file, offset, size);
    return buffer;
  }

  public static PinotNativeOrderLBuffer mapFile(File file, boolean readOnly, long offset, long size)
      throws IOException {
    if (readOnly) {
      return new PinotNativeOrderLBuffer(new MMapBuffer(file, offset, size, MMapMode.READ_ONLY), true, false);
    } else {
      return new PinotNativeOrderLBuffer(new MMapBuffer(file, offset, size, MMapMode.READ_WRITE), true, true);
    }
  }

  PinotNativeOrderLBuffer(LBufferAPI buffer, boolean closeable, boolean flushable) {
    super(buffer, closeable, flushable);
  }

  @Override
  public char getChar(int offset) {
    return _buffer.getChar(offset);
  }

  @Override
  public char getChar(long offset) {
    return _buffer.getChar(offset);
  }

  @Override
  public void putChar(int offset, char value) {
    _buffer.putChar(offset, value);
  }

  @Override
  public void putChar(long offset, char value) {
    _buffer.putChar(offset, value);
  }

  @Override
  public short getShort(int offset) {
    return _buffer.getShort(offset);
  }

  @Override
  public short getShort(long offset) {
    return _buffer.getShort(offset);
  }

  @Override
  public void putShort(int offset, short value) {
    _buffer.putShort(offset, value);
  }

  @Override
  public void putShort(long offset, short value) {
    _buffer.putShort(offset, value);
  }

  @Override
  public int getInt(int offset) {
    return _buffer.getInt(offset);
  }

  @Override
  public int getInt(long offset) {
    return _buffer.getInt(offset);
  }

  @Override
  public void putInt(int offset, int value) {
    _buffer.putInt(offset, value);
  }

  @Override
  public void putInt(long offset, int value) {
    _buffer.putInt(offset, value);
  }

  @Override
  public long getLong(int offset) {
    return _buffer.getLong(offset);
  }

  @Override
  public long getLong(long offset) {
    return _buffer.getLong(offset);
  }

  @Override
  public void putLong(int offset, long value) {
    _buffer.putLong(offset, value);
  }

  @Override
  public void putLong(long offset, long value) {
    _buffer.putLong(offset, value);
  }

  @Override
  public float getFloat(int offset) {
    return _buffer.getFloat(offset);
  }

  @Override
  public float getFloat(long offset) {
    return _buffer.getFloat(offset);
  }

  @Override
  public void putFloat(int offset, float value) {
    _buffer.putFloat(offset, value);
  }

  @Override
  public void putFloat(long offset, float value) {
    _buffer.putFloat(offset, value);
  }

  @Override
  public double getDouble(int offset) {
    return _buffer.getDouble(offset);
  }

  @Override
  public double getDouble(long offset) {
    return _buffer.getDouble(offset);
  }

  @Override
  public void putDouble(int offset, double value) {
    _buffer.putDouble(offset, value);
  }

  @Override
  public void putDouble(long offset, double value) {
    _buffer.putDouble(offset, value);
  }

  @Override
  public ByteOrder order() {
    return NATIVE_ORDER;
  }
}
