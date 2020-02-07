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
package org.apache.pinot.core.segment.memory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.core.util.CleanerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;


@ThreadSafe
public class PinotNonNativeUnsafeByteBuffer extends PinotDataBuffer {
  private final DirectBuffer _buffer;
  private final long _address;
  private final boolean _flushable;
  private static final Unsafe UNSAFE = getUnsafe();

  private static Unsafe getUnsafe() {
    try {
      Field var0 = Unsafe.class.getDeclaredField("theUnsafe");
      var0.setAccessible(true);
      return (Unsafe)(var0.get((Object)null));
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException("sun.misc.Unsafe is not available in this JVM", e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  public static PinotNonNativeUnsafeByteBuffer allocateDirect(int size) {
    return new PinotNonNativeUnsafeByteBuffer(ByteBuffer.allocateDirect(size), true, false);
  }

  static PinotNonNativeUnsafeByteBuffer loadFile(File file, long offset, int size)
      throws IOException {
    PinotNonNativeUnsafeByteBuffer buffer = allocateDirect(size);
    buffer.readFrom(0, file, offset, size);
    return buffer;
  }

  static PinotNonNativeUnsafeByteBuffer mapFile(File file, boolean readOnly, long offset, int size)
      throws IOException {
    if (readOnly) {
      try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel()) {
        ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, offset, size);
        return new PinotNonNativeUnsafeByteBuffer(buffer, true, false);
      }
    } else {
      try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
        ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, offset, size);
        return new PinotNonNativeUnsafeByteBuffer(buffer, true, true);
      }
    }
  }

  private PinotNonNativeUnsafeByteBuffer(ByteBuffer buffer, boolean closeable, boolean flushable) {
    super(closeable);
    _buffer = (DirectBuffer)buffer;
    _address = _buffer.address();
    _flushable = flushable;
  }

  @Override
  public byte getByte(int offset) {
    return UNSAFE.getByte(_address + offset);
  }

  @Override
  public byte getByte(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return UNSAFE.getByte(_address + (int) offset);
  }

  @Override
  public void putByte(int offset, byte value) {
    UNSAFE.putByte(_address + offset, value);
  }

  @Override
  public void putByte(long offset, byte value) {
    assert offset <= Integer.MAX_VALUE;
    UNSAFE.putByte(_address + (int) offset, value);
  }

  @Override
  public char getChar(int offset) {
    return Character.reverseBytes(UNSAFE.getChar(_address + offset));
  }

  @Override
  public char getChar(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return Character.reverseBytes(UNSAFE.getChar(_address + (int) offset));
  }

  @Override
  public void putChar(int offset, char value) {
    UNSAFE.putChar(_address + offset, Character.reverseBytes(value));
  }

  @Override
  public void putChar(long offset, char value) {
    assert offset <= Integer.MAX_VALUE;
    UNSAFE.putChar(_address + (int) offset, Character.reverseBytes(value));
  }

  @Override
  public short getShort(int offset) {
    return Short.reverseBytes(UNSAFE.getShort(_address + offset));
  }

  @Override
  public short getShort(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return Short.reverseBytes(UNSAFE.getShort(_address + (int) offset));
  }

  @Override
  public void putShort(int offset, short value) {
    UNSAFE.putShort(_address + offset, Short.reverseBytes(value));
  }

  @Override
  public void putShort(long offset, short value) {
    assert offset <= Integer.MAX_VALUE;
    UNSAFE.putShort(_address + (int) offset, Short.reverseBytes(value));
  }

  @Override
  public int getInt(int offset) {
    return Integer.reverseBytes(UNSAFE.getInt(_address + offset));
  }

  @Override
  public int getInt(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return Integer.reverseBytes(UNSAFE.getInt(_address + (int) offset));
  }

  @Override
  public void putInt(int offset, int value) {
    UNSAFE.putInt(_address + offset, Integer.reverseBytes(value));
  }

  @Override
  public void putInt(long offset, int value) {
    assert offset <= Integer.MAX_VALUE;
    UNSAFE.putInt(_address + (int) offset, Integer.reverseBytes(value));
  }

  @Override
  public long getLong(int offset) {
    return Long.reverseBytes(UNSAFE.getLong(_address + offset));
  }

  @Override
  public long getLong(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return Long.reverseBytes(UNSAFE.getLong(_address + (int) offset));
  }

  @Override
  public void putLong(int offset, long value) {
    UNSAFE.putLong(_address + offset, Long.reverseBytes(value));
  }

  @Override
  public void putLong(long offset, long value) {
    assert offset <= Integer.MAX_VALUE;
    UNSAFE.putLong(_address + (int) offset, Long.reverseBytes(value));
  }

  @Override
  public float getFloat(int offset) {
    return Float.intBitsToFloat(Integer.reverseBytes(UNSAFE.getInt(_address + offset)));
  }

  @Override
  public float getFloat(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return Float.intBitsToFloat(Integer.reverseBytes(UNSAFE.getInt(_address + (int) offset)));
  }

  @Override
  public void putFloat(int offset, float value) {
    UNSAFE.putInt(_address + offset, Integer.reverseBytes(Float.floatToRawIntBits(value)));
  }

  @Override
  public void putFloat(long offset, float value) {
    assert offset <= Integer.MAX_VALUE;
    UNSAFE.putInt(_address + (int) offset, Integer.reverseBytes(Float.floatToRawIntBits(value)));
  }

  @Override
  public double getDouble(int offset) {
    return Double.longBitsToDouble(Long.reverseBytes(UNSAFE.getLong(_address + offset)));
  }

  @Override
  public double getDouble(long offset) {
    assert offset <= Integer.MAX_VALUE;
    return Double.longBitsToDouble(Long.reverseBytes(UNSAFE.getLong(_address + (int) offset)));
  }

  @Override
  public void putDouble(int offset, double value) {
    UNSAFE.putLong(_address + offset, Long.reverseBytes(Double.doubleToRawLongBits(value)));
  }

  @Override
  public void putDouble(long offset, double value) {
    assert offset <= Integer.MAX_VALUE;
    UNSAFE.putLong(_address + (int) offset, Long.reverseBytes(Double.doubleToRawLongBits(value)));
  }

  @Override
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    assert offset <= Integer.MAX_VALUE;
    int intOffset = (int) offset;
    if (size <= PinotDataBuffer.BULK_BYTES_PROCESSING_THRESHOLD) {
      int end = destOffset + size;
      for (int i = destOffset; i < end; i++) {
        buffer[i] = getByte(intOffset++);
      }
    } else {
      ByteBuffer duplicate = ((ByteBuffer)_buffer).duplicate();
      duplicate.position(intOffset);
      duplicate.get(buffer, destOffset, size);
    }
  }

  @Override
  public void copyTo(long offset, PinotDataBuffer buffer, long destOffset, long size) {
    assert offset <= Integer.MAX_VALUE;
    assert size <= Integer.MAX_VALUE;
    int start = (int) offset;
    int end = start + (int) size;
    ByteBuffer duplicate = ((ByteBuffer)_buffer).duplicate();
    duplicate.position(start).limit(end);
    buffer.readFrom(destOffset, duplicate);
  }

  @Override
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    assert offset <= Integer.MAX_VALUE;
    int intOffset = (int) offset;
    if (size <= PinotDataBuffer.BULK_BYTES_PROCESSING_THRESHOLD) {
      int end = srcOffset + size;
      for (int i = srcOffset; i < end; i++) {
        putByte(intOffset++, buffer[i]);
      }
    } else {
      ByteBuffer duplicate = ((ByteBuffer)_buffer).duplicate();
      duplicate.position(intOffset);
      duplicate.put(buffer, srcOffset, size);
    }
  }

  @Override
  public void readFrom(long offset, ByteBuffer buffer) {
    assert offset <= Integer.MAX_VALUE;
    ByteBuffer duplicate = ((ByteBuffer)_buffer).duplicate();
    duplicate.position((int) offset);
    duplicate.put(buffer);
  }

  @Override
  public void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException {
    assert offset <= Integer.MAX_VALUE;
    assert size <= Integer.MAX_VALUE;
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      ByteBuffer duplicate = ((ByteBuffer)_buffer).duplicate();
      int start = (int) offset;
      int end = start + (int) size;
      duplicate.position(start).limit(end);
      randomAccessFile.getChannel().read(duplicate, srcOffset);
    }
  }

  @Override
  public long size() {
    return ((ByteBuffer)_buffer).limit();
  }

  @Override
  public ByteOrder order() {
    return ((ByteBuffer)_buffer).order();
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    assert start <= end;
    assert end <= Integer.MAX_VALUE;
    ByteBuffer duplicate = ((ByteBuffer)_buffer).duplicate();
    duplicate.position((int) start).limit((int) end);
    ByteBuffer buffer = duplicate.slice();
    buffer.order(byteOrder);
    return new PinotNonNativeUnsafeByteBuffer(buffer, false, false);
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    assert offset <= Integer.MAX_VALUE;
    int start = (int) offset;
    int end = start + size;
    ByteBuffer duplicate = ((ByteBuffer)_buffer).duplicate();
    duplicate.position(start).limit(end);
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
  protected void release()
      throws IOException {
    if (CleanerUtil.UNMAP_SUPPORTED) {
      CleanerUtil.getCleaner().freeBuffer((ByteBuffer)_buffer);
    }
  }
}
