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
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * A {@link DataBuffer} that is composed of multiple {@link DataBuffer}s that define a single contiguous buffer.
 * <p>
 * While reads and writes can span multiple buffers, there may be a performance impact when doing so.
 * Therefore it is recommended to try to wrap independent buffers.
 * <p>
 * Once this class is built, buffers cannot be added or removed.
 */
public class CompoundDataBuffer implements DataBuffer {

  private final DataBuffer[] _buffers;
  private final long[] _bufferOffsets;
  private int _lastBufferIndex = 0;
  private final ByteOrder _order;
  private final long _size;
  private final boolean _owner;

  /**
   * Creates a compound buffer from the given buffers.
   *
   * @param buffers The buffers that will be concatenated to form the compound buffer.
   * @param order The byte order of the buffer. Buffers in the array that have a different byte order will be converted.
   * @param owner Whether this buffer owns the underlying buffers. If true, the underlying buffers will be released when
   *              this buffer is closed.
   */
  public CompoundDataBuffer(DataBuffer[] buffers, ByteOrder order, boolean owner) {
    _owner = owner;
    _buffers = buffers;
    _bufferOffsets = new long[buffers.length];
    _order = order;
    long offset = 0;
    for (int i = 0; i < buffers.length; i++) {
      if (buffers[i].size() == 0) {
        throw new IllegalArgumentException("Buffer at index " + i + " is empty");
      }
      if (buffers[i].order() != _order) {
        buffers[i] = buffers[i].view(0, buffers[i].size(), _order);
      }
    }
    for (int i = 0; i < buffers.length; i++) {
      _bufferOffsets[i] = offset;
      long size = buffers[i].size();
      offset += size;
    }
    _size = offset;
  }

  public CompoundDataBuffer(ByteBuffer[] buffers, ByteOrder order, boolean owner) {
    this(asDataBufferArray(buffers), order, owner);
  }

  /**
   * Creates a compound buffer from the given buffers.
   * @param buffers The buffers that will be concatenated to form the compound buffer.
   * @param order The byte order of the buffer. Buffers in the list that have a different byte order will be converted.
   * @param owner Whether this buffer owns the underlying buffers. If true, the underlying buffers will be released when
   *              this buffer is closed.
   */
  public CompoundDataBuffer(List<DataBuffer> buffers, ByteOrder order, boolean owner) {
    this(buffers.toArray(new DataBuffer[0]), order, owner);
  }

  private static DataBuffer[] asDataBufferArray(ByteBuffer[] buffers) {
    DataBuffer[] result = new DataBuffer[buffers.length];
    for (int i = 0; i < buffers.length; i++) {
      result[i] = PinotByteBuffer.wrap(buffers[i]);
    }
    return result;
  }

  private int getBufferIndex(long offset) {
    // this optimistically assumes that lookups are going to be in ascending order
    // we don't care about concurrency here given that this is only used to speed up the lookup
    int lastBufferIndex = _lastBufferIndex;
    if (_bufferOffsets[lastBufferIndex] > offset) {
      lastBufferIndex = 0;
    }

    for (int i = lastBufferIndex; i < _bufferOffsets.length; i++) {
      if (offset < _bufferOffsets[i]) {
        int result = i - 1;
        _lastBufferIndex = result;
        return result;
      }
    }
    int result = _bufferOffsets.length - 1;
    _lastBufferIndex = result;
    return result;
  }

  private ByteBuffer copy(long offset, int length) {
    if (offset + length > _size) {
      throw new BufferUnderflowException();
    }
    byte[] result = new byte[length];

    int bufferIndex = getBufferIndex(offset);
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];

    DataBuffer buffer = _buffers[bufferIndex];
    int toCopy = (int) Math.min(length, buffer.size() - inBufferIndex);
    buffer.copyTo(inBufferIndex, result, 0, toCopy);

    int remaining = length - toCopy;
    while (remaining > 0) {
      bufferIndex++;
      buffer = _buffers[bufferIndex];
      toCopy = (int) Math.min(remaining, buffer.size());
      buffer.copyTo(0, result, length - remaining, toCopy);

      remaining -= toCopy;
    }
    return ByteBuffer.wrap(result).order(_order);
  }

  @Override
  public void readFrom(long offset, byte[] input, int srcOffset, int size) {
    if (offset + size > _size) {
      throw new BufferOverflowException();
    }

    int bufferIndex = getBufferIndex(offset);
    int remaining = size;
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    while (remaining > 0) {
      DataBuffer buffer = _buffers[bufferIndex];
      int toRead = (int) Math.min(remaining, buffer.size() - inBufferIndex);
      buffer.readFrom(inBufferIndex, input, srcOffset, toRead);

      bufferIndex++;
      remaining -= toRead;
      srcOffset += toRead;
      inBufferIndex = 0; // from now on we always write in the first position of the buffer
    }
  }

  @Override
  public void readFrom(long offset, ByteBuffer input) {
    if (offset + input.remaining() > _size) {
      throw new BufferOverflowException();
    }

    int startLimit = input.limit();
    int bufferIndex = getBufferIndex(offset);
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];

    while (input.hasRemaining()) {
      DataBuffer buffer = _buffers[bufferIndex];
      int toRead = (int) Math.min(input.remaining(), buffer.size() - inBufferIndex);

      input.limit(input.position() + toRead);
      buffer.readFrom(inBufferIndex, input);

      input.position(input.limit());
      input.limit(startLimit);
      bufferIndex++;
      inBufferIndex = 0; // from now on we always write in the first position of the buffer
    }
  }

  @Override
  public void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException {
    if (offset + size > _size) {
      throw new BufferOverflowException();
    }

    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    long toRead = Math.min(size, buffer.size() - inBufferIndex);
    buffer.readFrom(inBufferIndex, file, srcOffset, toRead);

    long fileOffset = srcOffset + toRead;
    long remaining = size - toRead;
    while (remaining > 0) {
      bufferIndex++;
      buffer = _buffers[bufferIndex];
      toRead = Math.min(remaining, buffer.size());
      buffer.readFrom(0, file, fileOffset, toRead);

      remaining -= toRead;
      fileOffset += toRead;
    }
  }

  @Override
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    if (offset + size > _size) {
      throw new BufferUnderflowException();
    }

    int bufferIndex = getBufferIndex(offset);
    int remaining = size;

    DataBuffer bufferToCopy = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    int toCopy = (int) Math.min(remaining, bufferToCopy.size() - inBufferIndex);
    bufferToCopy.copyTo(inBufferIndex, buffer, destOffset, toCopy);

    remaining -= toCopy;
    destOffset += toCopy;

    while (remaining > 0) {
      bufferIndex++;
      bufferToCopy = _buffers[bufferIndex];
      toCopy = (int) Math.min(remaining, bufferToCopy.size());
      bufferToCopy.copyTo(0, buffer, destOffset, toCopy);

      remaining -= toCopy;
      destOffset += toCopy;
    }
  }

  @Override
  public void copyTo(long offset, DataBuffer buffer, long destOffset, long size) {
    if (offset + size > _size) {
      throw new BufferUnderflowException();
    }

    int bufferIndex = getBufferIndex(offset);
    long remaining = size;

    DataBuffer bufferToCopy = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    long toCopy = Math.min(remaining, bufferToCopy.size() - inBufferIndex);
    bufferToCopy.copyTo(inBufferIndex, buffer, destOffset, toCopy);

    bufferIndex++;
    remaining -= toCopy;
    destOffset += toCopy;

    while (remaining > 0) {
      bufferToCopy = _buffers[bufferIndex];
      toCopy = Math.min(remaining, bufferToCopy.size());
      bufferToCopy.copyTo(0, buffer, destOffset, toCopy);

      bufferIndex++;
      remaining -= toCopy;
      destOffset += toCopy;
    }
  }

  @Override
  public byte getByte(long offset) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    return buffer.getByte(inBufferIndex);
  }

  @Override
  public void putByte(long offset, byte value) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    buffer.putByte(inBufferIndex, value);
  }

  @Override
  public char getChar(long offset) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Character.BYTES <= buffer.size()) {
      // fast path
      return buffer.getChar(inBufferIndex);
    } else {
      // slow path
      ByteBuffer byteBuffer = copy(offset, Character.BYTES);
      return byteBuffer.getChar();
    }
  }

  @Override
  public void putChar(long offset, char value) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Character.BYTES <= buffer.size()) {
      // fast path
      buffer.putChar(inBufferIndex, value);
    } else {
      // slow path
      ByteBuffer byteBuffer = ByteBuffer.allocate(Character.BYTES)
          .order(_order)
          .putChar(value);
      byteBuffer.flip();
      readFrom(offset, byteBuffer);
    }
  }

  @Override
  public short getShort(long offset) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Short.BYTES <= buffer.size()) {
      // fast path
      return buffer.getShort(inBufferIndex);
    } else {
      // slow path
      ByteBuffer byteBuffer = copy(offset, Short.BYTES);
      return byteBuffer.getShort();
    }
  }

  @Override
  public void putShort(long offset, short value) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Short.BYTES <= buffer.size()) {
      // fast path
      buffer.putShort(inBufferIndex, value);
    } else {
      // slow path
      ByteBuffer byteBuffer = ByteBuffer.allocate(Short.BYTES)
          .order(_order)
          .putShort(value);
      byteBuffer.flip();
      readFrom(offset, byteBuffer);
    }
  }

  @Override
  public int getInt(long offset) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Integer.BYTES <= buffer.size()) {
      // fast path
      return buffer.getInt(inBufferIndex);
    } else {
      // slow path
      ByteBuffer byteBuffer = copy(offset, Integer.BYTES);
      return byteBuffer.getInt();
    }
  }

  @Override
  public void putInt(long offset, int value) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Integer.BYTES <= buffer.size()) {
      // fast path
      buffer.putInt(inBufferIndex, value);
    } else {
      // slow path
      ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES)
          .order(_order)
          .putInt(value);
      byteBuffer.flip();
      readFrom(offset, byteBuffer);
    }
  }

  @Override
  public long getLong(long offset) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Long.BYTES <= buffer.size()) {
      // fast path
      return buffer.getLong(inBufferIndex);
    } else {
      // slow path
      ByteBuffer byteBuffer = copy(offset, Long.BYTES);
      return byteBuffer.getLong();
    }
  }

  @Override
  public void putLong(long offset, long value) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Long.BYTES <= buffer.size()) {
      // fast path
      buffer.putLong(inBufferIndex, value);
    } else {
      // slow path
      ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES)
          .order(_order)
          .putLong(value);
      byteBuffer.flip();
      readFrom(offset, byteBuffer);
    }
  }

  @Override
  public float getFloat(long offset) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Float.BYTES <= buffer.size()) {
      // fast path
      return buffer.getFloat(inBufferIndex);
    } else {
      // slow path
      ByteBuffer byteBuffer = copy(offset, Float.BYTES);
      return byteBuffer.getFloat();
    }
  }

  @Override
  public void putFloat(long offset, float value) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Float.BYTES <= buffer.size()) {
      // fast path
      buffer.putFloat(inBufferIndex, value);
    } else {
      // slow path
      ByteBuffer byteBuffer = ByteBuffer.allocate(Float.BYTES)
          .order(_order)
          .putFloat(value);
      byteBuffer.flip();
      readFrom(offset, byteBuffer);
    }
  }

  @Override
  public double getDouble(long offset) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Double.BYTES <= buffer.size()) {
      // fast path
      return buffer.getDouble(inBufferIndex);
    } else {
      // slow path
      ByteBuffer byteBuffer = copy(offset, Double.BYTES);
      return byteBuffer.getDouble();
    }
  }

  @Override
  public void putDouble(long offset, double value) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + Double.BYTES <= buffer.size()) {
      // fast path
      buffer.putDouble(inBufferIndex, value);
    } else {
      // slow path
      ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES)
          .order(_order)
          .putDouble(value);
      byteBuffer.flip();
      readFrom(offset, byteBuffer);
    }
  }

  @Override
  public long size() {
    return _size;
  }

  @Override
  public ByteOrder order() {
    return _order;
  }

  @Override
  public DataBuffer view(long start, long end, ByteOrder byteOrder) {
    if (start < 0 || end > _size || start > end) {
      throw new IllegalArgumentException("Invalid start/end: " + start + "/" + end);
    }
    if (start == 0 && end == _size && byteOrder == _order) {
      return new CompoundDataBuffer(_buffers, _order, false);
    }
    int startBufferIndex = getBufferIndex(start);
    int endBufferIndex = getBufferIndex(end - 1);

    if (startBufferIndex == endBufferIndex) {
      long bufferOffset = _bufferOffsets[startBufferIndex];
      DataBuffer buffer = _buffers[startBufferIndex];
      return buffer.view(start - bufferOffset, end - bufferOffset, byteOrder);
    } else {
      DataBuffer firstBuffer = _buffers[startBufferIndex]
          .view(start - _bufferOffsets[startBufferIndex], _buffers[startBufferIndex].size(), byteOrder);
      DataBuffer lastBuffer = _buffers[endBufferIndex]
          .view(0, end - _bufferOffsets[endBufferIndex], byteOrder);

      DataBuffer[] buffers = new DataBuffer[endBufferIndex - startBufferIndex + 1];
      buffers[0] = firstBuffer;
      if (buffers.length > 2) {
        System.arraycopy(_buffers, startBufferIndex + 1, buffers, 1, buffers.length - 2);
      }
      buffers[buffers.length - 1] = lastBuffer;

      return new CompoundDataBuffer(buffers, byteOrder, false);
    }
  }

  @Override
  public ImmutableRoaringBitmap viewAsRoaringBitmap(long offset, int length) {
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + length < buffer.size()) {
      return buffer.viewAsRoaringBitmap(inBufferIndex, length);
    } else {
      ByteBuffer copy = copy(offset, length);
      return new ImmutableRoaringBitmap(copy);
    }
  }

  @Override
  public ByteBuffer copyOrView(long offset, int size, ByteOrder byteOrder) {
    if (offset + size > _size) {
      throw new BufferUnderflowException();
    }
    int bufferIndex = getBufferIndex(offset);
    DataBuffer buffer = _buffers[bufferIndex];
    long inBufferIndex = offset - _bufferOffsets[bufferIndex];
    if (inBufferIndex + size <= buffer.size()) {
      return buffer.copyOrView(inBufferIndex, size, byteOrder);
    } else {
      return copy(offset, size).order(byteOrder);
    }
  }

  @Override
  public void flush() {
    RuntimeException firstException = null;

    for (DataBuffer buffer : _buffers) {
      try {
        buffer.flush();
      } catch (RuntimeException ex) {
        if (firstException == null) {
          firstException = ex;
        }
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  @Override
  public void close()
      throws IOException {
    if (!_owner) {
      return;
    }
    IOException firstException = null;

    for (DataBuffer buffer : _buffers) {
      try {
        buffer.close();
      } catch (IOException ex) {
        if (firstException == null) {
          firstException = ex;
        }
      }
    }
    if (firstException != null) {
      throw firstException;
    }
  }

  public DataBuffer[] getBuffers() {
    return _buffers;
  }

  @Override
  public void appendAsByteBuffers(List<ByteBuffer> appendTo) {
    for (DataBuffer buffer : _buffers) {
      buffer.appendAsByteBuffers(appendTo);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DataBuffer)) {
      return false;
    }
    DataBuffer buffer = (DataBuffer) o;
    return DataBuffer.sameContent(this, buffer);
  }

  @Override
  public int hashCode() {
    return DataBuffer.commonHashCode(this);
  }

  public static class Builder {
    private final ByteOrder _order;
    private final boolean _owner;
    private final ArrayList<DataBuffer> _buffers = new ArrayList<>();
    private long _writtenBytes;

    public Builder() {
      this(ByteOrder.BIG_ENDIAN, false);
    }

    public Builder(ByteOrder order) {
      this(order, false);
    }

    public Builder(boolean owner) {
      this(ByteOrder.BIG_ENDIAN, owner);
    }

    public Builder(ByteOrder order, boolean owner) {
      _order = order;
      _owner = owner;
    }

    public long getWrittenBytes() {
      return _writtenBytes;
    }

    public Builder addBuffer(ByteBuffer buffer) {
      return addBuffer(PinotByteBuffer.wrap(buffer));
    }

    public Builder addBuffers(Iterable<ByteBuffer> buffers) {
      for (ByteBuffer buffer : buffers) {
        addBuffer(buffer);
      }
      return this;
    }

    public Builder addBuffer(DataBuffer buffer) {
      if (buffer instanceof CompoundDataBuffer) {
        CompoundDataBuffer compoundBuffer = (CompoundDataBuffer) buffer;
        for (DataBuffer childBuffer : compoundBuffer.getBuffers()) {
          addBuffer(childBuffer);
        }
        return this;
      }
      long size = buffer.size();
      if (size != 0) {
        _buffers.add(buffer);
        _writtenBytes += size;
      }
      return this;
    }

    public Builder addPagedOutputStream(PagedPinotOutputStream stream) {
      ByteBuffer[] pages = stream.getPages();
      _buffers.ensureCapacity(_buffers.size() + pages.length);
      for (ByteBuffer page : pages) {
        addBuffer(PinotByteBuffer.wrap(page));
      }
      return this;
    }

    public CompoundDataBuffer build() {
      return new CompoundDataBuffer(_buffers, _order, _owner);
    }
  }
}
