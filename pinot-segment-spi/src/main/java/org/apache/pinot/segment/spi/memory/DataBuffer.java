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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public interface DataBuffer extends Closeable {

  default byte getByte(int offset) {
    return getByte((long) offset);
  }

  byte getByte(long offset);

  default void putByte(int offset, byte value) {
    putByte((long) offset, value);
  }

  void putByte(long offset, byte value);

  default char getChar(int offset) {
    return getChar((long) offset);
  }

  char getChar(long offset);

  default void putChar(int offset, char value) {
    putChar((long) offset, value);
  }

  void putChar(long offset, char value);

  default short getShort(int offset) {
    return getShort((long) offset);
  }

  short getShort(long offset);

  default void putShort(int offset, short value) {
    putShort((long) offset, value);
  }

  void putShort(long offset, short value);

  default int getInt(int offset) {
    return getInt((long) offset);
  }

  int getInt(long offset);

  default void putInt(int offset, int value) {
    putInt((long) offset, value);
  }

  void putInt(long offset, int value);

  default long getLong(int offset) {
    return getLong((long) offset);
  }

  long getLong(long offset);

  default void putLong(int offset, long value) {
    putLong((long) offset, value);
  }

  void putLong(long offset, long value);

  default float getFloat(int offset) {
    return getFloat((long) offset);
  }

  float getFloat(long offset);

  default void putFloat(int offset, float value) {
    putFloat((long) offset, value);
  }

  void putFloat(long offset, float value);

  default double getDouble(int offset) {
    return getDouble((long) offset);
  }

  double getDouble(long offset);

  default void putDouble(int offset, double value) {
    putDouble((long) offset, value);
  }

  void putDouble(long offset, double value);

  /**
   * Given an array of bytes, copies the content of this object into the array of bytes.
   * The first byte to be copied is the one that could be read with {@code this.getByte(offset)}
   */
  void copyTo(long offset, byte[] buffer, int destOffset, int size);

  /**
   * Given an array of bytes, copies the content of this object into the array of bytes.
   * The first byte to be copied is the one that could be read with {@code this.getByte(offset)}
   */
  default void copyTo(long offset, byte[] buffer) {
    copyTo(offset, buffer, 0, buffer.length);
  }

  /**
   * Note: It is the responsibility of the caller to make sure arguments are checked before the methods are called.
   * While some rudimentary checks are performed on the input, the checks are best effort and when performance is an
   * overriding priority, as when methods of this class are optimized by the runtime compiler, some or all checks
   * (if any) may be elided. Hence, the caller must not rely on the checks and corresponding exceptions!
   */
  void copyTo(long offset, DataBuffer buffer, long destOffset, long size);

  default void copyTo(long offset, ByteBuffer buffer, int destOffset, int size) {
    PinotByteBuffer wrap = PinotByteBuffer.wrap(buffer);
    copyTo(offset, wrap, destOffset, size);
  }

  /**
   * Given an array of bytes, writes the content in the specified position.
   */
  void readFrom(long offset, byte[] buffer, int srcOffset, int size);

  default void readFrom(long offset, byte[] buffer) {
    readFrom(offset, buffer, 0, buffer.length);
  }

  void readFrom(long offset, ByteBuffer buffer);

  void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException;

  long size();

  ByteOrder order();

  /**
   * Creates a view of the range [start, end) of this buffer with the given byte order. Calling {@link #flush()} or
   * {@link #close()} has no effect on view.
   */
  DataBuffer view(long start, long end, ByteOrder byteOrder);

  /**
   * Creates a view of the range [start, end) of this buffer with the current byte order. Calling {@link #flush()} or
   * {@link #close()} has no effect on view.
   */
  default DataBuffer view(long start, long end) {
    return view(start, end, order());
  }

  void flush();

  default PinotInputStream openInputStream() {
    return new DataBufferPinotInputStream(this);
  }

  default PinotInputStream openInputStream(long offset) {
    return openInputStream(offset, size() - offset);
  }

  default PinotInputStream openInputStream(long offset, long length) {
    return new DataBufferPinotInputStream(this, offset, offset + length);
  }

  /**
   * Reads the range [offset, offset + length) as a RoaringBitmap.
   * <p>
   * Implementations should do their best to do not allocate memory and instead return a view of the underlying data.
   */
  ImmutableRoaringBitmap viewAsRoaringBitmap(long offset, int length);

  /**
   * Returns a ByteBuffer whose content is the same as the range [offset, offset + size) of this buffer.
   * <p>
   * Implementations should do their best to do not allocate memory and instead return a view of the underlying data.
   * Callers cannot assume they have the ownership of the returned ByteBuffer, and therefore should not call
   * {@link CleanerUtil#cleanQuietly(ByteBuffer)} using the returned object.
   */
  ByteBuffer copyOrView(long offset, int size, ByteOrder byteOrder);

  void appendAsByteBuffers(List<ByteBuffer> appendTo);

  /**
   *
   * Returns a ByteBuffer whose content is the same as the range [offset, offset + size) of this buffer.
   * <p>
   * Implementations should do their best to do not allocate memory and instead return a view of the underlying data.
   * Callers cannot assume they have the ownership of the returned ByteBuffer, and therefore should not call
   * {@link CleanerUtil#cleanQuietly(ByteBuffer)} using the returned object.
   * <p>
   * The returned ByteBuffer will have the same byte order as this buffer.
   */
  default ByteBuffer copyOrView(long offset, int size) {
    return copyOrView(offset, size, order());
  }

  static boolean sameContent(DataBuffer buffer1, DataBuffer buffer2) {
    long size = buffer1.size();
    if (size != buffer2.size()) {
      return false;
    }
    DataBuffer nativeBuffer1 = buffer1.view(0, size, ByteOrder.nativeOrder());
    DataBuffer nativeBuffer2 = buffer2.view(0, size, ByteOrder.nativeOrder());

    long maxLong = size & ~7L;
    for (long i = 0; i < maxLong; i += 8) {
      if (nativeBuffer1.getLong(i) != nativeBuffer2.getLong(i)) {
        return false;
      }
    }
    for (long i = maxLong; i < size; i++) {
      if (buffer1.getByte(i) != buffer2.getByte(i)) {
        return false;
      }
    }
    return true;
  }

  static int commonHashCode(DataBuffer dataBuffer) {
    long firstLong;
    long lastLong;
    long size = dataBuffer.size();
    int intSize;
    if (size > Integer.MAX_VALUE) {
      intSize = Integer.MAX_VALUE;
    } else {
      intSize = (int) size;
    }
    switch (intSize) {
      case 0:
        firstLong = 0;
        lastLong = 0;
        break;
      case 1:
        firstLong = dataBuffer.getByte(0);
        lastLong = firstLong;
        break;
      case 2:
        firstLong = dataBuffer.getShort(0);
        lastLong = firstLong;
        break;
      case 3:
        firstLong = dataBuffer.getShort(0);
        lastLong = dataBuffer.getShort(1);
        break;
      case 4:
        firstLong = dataBuffer.getInt(0);
        lastLong = firstLong;
        break;
      case 5:
      case 6:
      case 7:
        firstLong = dataBuffer.getInt(0);
        lastLong = dataBuffer.getInt(intSize - 4);
        break;
      default:
        firstLong = dataBuffer.getLong(0);
        lastLong = dataBuffer.getLong(size - 8);
        break;
    }
    return Objects.hash(size, firstLong, lastLong);
  }
}
