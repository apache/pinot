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
package org.apache.pinot.foreign.memory;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public abstract class AbstractForeignMemoryPinotDataBuffer extends PinotDataBuffer {

  private final MemorySegment _memorySegment;
  @Nullable
  private final Arena _arena;

  public AbstractForeignMemoryPinotDataBuffer(MemorySegment memorySegment, @Nullable Arena arena) {
    super(arena != null);
    _memorySegment = memorySegment;
    _arena = arena;
  }

  protected abstract ValueLayout.OfChar getOfChar();

  protected abstract ValueLayout.OfShort getOfShort();

  protected abstract ValueLayout.OfInt getOfInt();

  protected abstract ValueLayout.OfFloat getOfFloat();

  protected abstract ValueLayout.OfLong getOfLong();

  protected abstract ValueLayout.OfDouble getOfDouble();

  @Override
  public byte getByte(long offset) {
    return _memorySegment.get(ValueLayout.JAVA_BYTE, offset);
  }

  @Override
  public void putByte(long offset, byte value) {
    _memorySegment.set(ValueLayout.JAVA_BYTE, offset, value);
  }

  @Override
  public char getChar(long offset) {
    return _memorySegment.get(getOfChar(), offset);
  }

  @Override
  public void putChar(long offset, char value) {
    _memorySegment.set(getOfChar(), offset, value);
  }

  @Override
  public short getShort(long offset) {
    return _memorySegment.get(getOfShort(), offset);
  }

  @Override
  public void putShort(long offset, short value) {
    _memorySegment.set(getOfShort(), offset, value);
  }

  @Override
  public int getInt(long offset) {
    return _memorySegment.get(getOfInt(), offset);
  }

  @Override
  public void putInt(long offset, int value) {
    _memorySegment.set(getOfInt(), offset, value);
  }

  @Override
  public long getLong(long offset) {
    return _memorySegment.get(getOfLong(), offset);
  }

  @Override
  public void putLong(long offset, long value) {
    _memorySegment.set(getOfLong(), offset, value);
  }

  @Override
  public float getFloat(long offset) {
    return _memorySegment.get(getOfFloat(), offset);
  }

  @Override
  public void putFloat(long offset, float value) {
    _memorySegment.set(getOfFloat(), offset, value);
  }

  @Override
  public double getDouble(long offset) {
    return _memorySegment.get(getOfDouble(), offset);
  }

  @Override
  public void putDouble(long offset, double value) {
    _memorySegment.set(getOfDouble(), offset, value);
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
      // This creates 1 small object, but should be faster with larger buffers
      MemorySegment.copy(_memorySegment, offset, MemorySegment.ofArray(buffer), destOffset, size);
    }
  }

  @Override
  public void copyTo(long offset, PinotDataBuffer buffer, long destOffset, long size) {
    long actualSize = Math.min(size, buffer.size() - destOffset);
    actualSize = Math.min(actualSize, _memorySegment.byteSize() - offset);

    if (buffer instanceof AbstractForeignMemoryPinotDataBuffer) {
      AbstractForeignMemoryPinotDataBuffer other = (AbstractForeignMemoryPinotDataBuffer) buffer;
      MemorySegment.copy(_memorySegment, offset, other._memorySegment, destOffset, actualSize);
    } else {
      long position = destOffset;
      long remaining = actualSize;

      int step = Integer.MAX_VALUE;

      while (remaining > step) {
        ByteBuffer destBb = buffer.toDirectByteBuffer(position, step);
        MemorySegment destSegment = MemorySegment.ofBuffer(destBb);
        MemorySegment.copy(_memorySegment, offset, destSegment, 0, step);
        position += step;
        remaining -= step;
      }
      if (remaining > 0) {
        assert remaining < step;
        ByteBuffer destBb = buffer.toDirectByteBuffer(position, (int) remaining);
        MemorySegment destSegment = MemorySegment.ofBuffer(destBb);
        MemorySegment.copy(_memorySegment, offset, destSegment, 0, remaining);
      }
    }
  }

  @Override
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    MemorySegment.copy(MemorySegment.ofArray(buffer), srcOffset, _memorySegment, offset, size);
  }

  @Override
  public void readFrom(long offset, ByteBuffer buffer) {
    MemorySegment.copy(MemorySegment.ofBuffer(buffer), 0, _memorySegment, offset, buffer.remaining());
  }

  @Override
  public long size() {
    return _memorySegment.byteSize();
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    Preconditions.checkArgument(start >= 0, "Start cannot be negative");
    Preconditions.checkArgument(end <= size(), "End cannot be higher than this buffer capacity");

    MemorySegment slice = _memorySegment.asSlice(start, end - start);

    return byteOrder == ByteOrder.BIG_ENDIAN ? new BigEndianForeignMemoryPinotDataBuffer(slice, null)
        : new LittleEndianForeignMemoryPinotDataBuffer(slice, null);
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    return _memorySegment.asSlice(offset, size)
        .asByteBuffer()
        .order(byteOrder);
  }

  @Override
  public void flush() {
    if (_memorySegment.isMapped() && !_memorySegment.isReadOnly()) {
      _memorySegment.force();
    }
  }

  @Override
  public void release() {
    _arena.close();
  }
}
