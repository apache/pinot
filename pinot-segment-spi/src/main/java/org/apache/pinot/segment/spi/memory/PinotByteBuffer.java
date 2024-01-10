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
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import javax.annotation.concurrent.ThreadSafe;


@ThreadSafe
public class PinotByteBuffer extends PinotDataBuffer {
  private final ByteBuffer _buffer;
  private final boolean _flushable;

  static PinotByteBuffer allocateDirect(int size, ByteOrder byteOrder) {
    return new PinotByteBuffer(ByteBuffer.allocateDirect(size).order(byteOrder), true, false);
  }

  static PinotByteBuffer loadFile(File file, long offset, int size, ByteOrder byteOrder)
      throws IOException {
    PinotByteBuffer buffer = allocateDirect(size, byteOrder);
    buffer.readFrom(0, file, offset, size);
    return buffer;
  }

  static PinotByteBuffer mapFile(File file, boolean readOnly, long offset, int size, ByteOrder byteOrder)
      throws IOException {
    if (readOnly) {
      try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel()) {
        ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, size).order(byteOrder);
        return new PinotByteBuffer(buffer, true, false);
      }
    } else {
      try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
        ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, size).order(byteOrder);
        return new PinotByteBuffer(buffer, true, true);
      }
    }
  }

  public PinotByteBuffer(ByteBuffer buffer, boolean closeable, boolean flushable) {
    super(closeable);
    _buffer = buffer;
    _flushable = flushable;
  }

  @Override
  public byte getByte(int offset) {
    return _buffer.get(offset);
  }

  @Override
  public byte getByte(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return _buffer.get((int) offset);
  }

  @Override
  public void putByte(int offset, byte value) {
    _buffer.put(offset, value);
  }

  @Override
  public void putByte(long offset, byte value) {
    assert offset <= Integer.MAX_VALUE;
    _buffer.put((int) offset, value);
  }

  @Override
  public char getChar(int offset) {
    return _buffer.getChar(offset);
  }

  @Override
  public char getChar(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return _buffer.getChar((int) offset);
  }

  @Override
  public void putChar(int offset, char value) {
    _buffer.putChar(offset, value);
  }

  @Override
  public void putChar(long offset, char value) {
    assert offset <= Integer.MAX_VALUE;
    _buffer.putChar((int) offset, value);
  }

  @Override
  public short getShort(int offset) {
    return _buffer.getShort(offset);
  }

  @Override
  public short getShort(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return _buffer.getShort((int) offset);
  }

  @Override
  public void putShort(int offset, short value) {
    _buffer.putShort(offset, value);
  }

  @Override
  public void putShort(long offset, short value) {
    assert offset <= Integer.MAX_VALUE;
    _buffer.putShort((int) offset, value);
  }

  @Override
  public int getInt(int offset) {
    return _buffer.getInt(offset);
  }

  @Override
  public int getInt(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return _buffer.getInt((int) offset);
  }

  @Override
  public void putInt(int offset, int value) {
    _buffer.putInt(offset, value);
  }

  @Override
  public void putInt(long offset, int value) {
    assert offset <= Integer.MAX_VALUE;
    _buffer.putInt((int) offset, value);
  }

  @Override
  public long getLong(int offset) {
    return _buffer.getLong(offset);
  }

  @Override
  public long getLong(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return _buffer.getLong((int) offset);
  }

  @Override
  public void putLong(int offset, long value) {
    _buffer.putLong(offset, value);
  }

  @Override
  public void putLong(long offset, long value) {
    assert offset <= Integer.MAX_VALUE;
    _buffer.putLong((int) offset, value);
  }

  @Override
  public float getFloat(int offset) {
    return _buffer.getFloat(offset);
  }

  @Override
  public float getFloat(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return _buffer.getFloat((int) offset);
  }

  @Override
  public void putFloat(int offset, float value) {
    _buffer.putFloat(offset, value);
  }

  @Override
  public void putFloat(long offset, float value) {
    assert offset <= Integer.MAX_VALUE;
    _buffer.putFloat((int) offset, value);
  }

  @Override
  public double getDouble(int offset) {
    return _buffer.getDouble(offset);
  }

  @Override
  public double getDouble(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return _buffer.getDouble((int) offset);
  }

  @Override
  public void putDouble(int offset, double value) {
    _buffer.putDouble(offset, value);
  }

  @Override
  public void putDouble(long offset, double value) {
    assert offset <= Integer.MAX_VALUE;
    _buffer.putDouble((int) offset, value);
  }

  @Override
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    assert offset <= Integer.MAX_VALUE;
    int intOffset = (int) offset;
    if (size <= BULK_BYTES_PROCESSING_THRESHOLD) {
      int end = destOffset + size;
      for (int i = destOffset; i < end; i++) {
        buffer[i] = getByte(intOffset++);
      }
    } else {
      ByteBuffer duplicate = _buffer.duplicate();
      ((Buffer) duplicate).position(intOffset);
      duplicate.get(buffer, destOffset, size);
    }
  }

  @Override
  public void copyTo(long offset, PinotDataBuffer buffer, long destOffset, long size) {
    assert offset <= Integer.MAX_VALUE;
    assert size <= Integer.MAX_VALUE;
    int start = (int) offset;
    int end = start + (int) size;
    ByteBuffer duplicate = _buffer.duplicate();
    ((Buffer) duplicate).position(start).limit(end);
    buffer.readFrom(destOffset, duplicate);
  }

  @Override
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    assert offset <= Integer.MAX_VALUE;
    int intOffset = (int) offset;
    if (size <= BULK_BYTES_PROCESSING_THRESHOLD) {
      int end = srcOffset + size;
      for (int i = srcOffset; i < end; i++) {
        putByte(intOffset++, buffer[i]);
      }
    } else {
      ByteBuffer duplicate = _buffer.duplicate();
      ((Buffer) duplicate).position(intOffset);
      duplicate.put(buffer, srcOffset, size);
    }
  }

  @Override
  public void readFrom(long offset, ByteBuffer buffer) {
    assert offset <= Integer.MAX_VALUE;
    ByteBuffer duplicate = _buffer.duplicate();
    ((Buffer) duplicate).position((int) offset);
    duplicate.put(buffer);
  }

  @Override
  public void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException {
    assert offset <= Integer.MAX_VALUE;
    assert size <= Integer.MAX_VALUE;
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
        FileChannel channel = randomAccessFile.getChannel()) {

      ByteBuffer duplicate = _buffer.duplicate();
      int start = (int) offset;
      int end = start + (int) size;
      ((Buffer) duplicate).position(start).limit(end);
      channel.read(duplicate, srcOffset);
    }
  }

  @Override
  public long size() {
    return _buffer.limit();
  }

  @Override
  public ByteOrder order() {
    return _buffer.order();
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    assert start <= end;
    assert end <= Integer.MAX_VALUE;
    ByteBuffer duplicate = _buffer.duplicate();
    ((Buffer) duplicate).position((int) start).limit((int) end);
    ByteBuffer buffer = duplicate.slice();
    buffer.order(byteOrder);
    return new PinotByteBuffer(buffer, false, false);
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    assert offset <= Integer.MAX_VALUE;
    int start = (int) offset;
    int end = start + size;
    ByteBuffer duplicate = _buffer.duplicate();
    ((Buffer) duplicate).position(start).limit(end);
    ByteBuffer buffer = duplicate.slice();
    buffer.order(byteOrder);
    return buffer;
  }

  @Override
  public void flush() {
    if (_flushable) {
      ((MappedByteBuffer) _buffer).force();
    }
  }

  @Override
  public void release()
      throws IOException {
    if (CleanerUtil.UNMAP_SUPPORTED) {
      CleanerUtil.getCleaner().freeBuffer(_buffer);
    }
  }
}
