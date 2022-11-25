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
package org.apache.pinot.segment.spi.memory.chronicle;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytes;
import org.apache.pinot.segment.spi.memory.ByteBufferUtil;
import org.apache.pinot.segment.spi.memory.NonNativePinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public class ChronicleDataBuffer extends PinotDataBuffer {

  private final Bytes<?> _store;
  private final boolean _flushable;
  private boolean _closed = false;
  private final long _viewStart;
  private final long _viewEnd;

  public ChronicleDataBuffer(Bytes<?> store, boolean closeable, boolean flushable, long start, long end) {
    super(closeable);
    Preconditions.checkArgument(start >= 0, "Invalid start " + start + ". It must be > 0");
    Preconditions.checkArgument(start <= end, "Invalid start " + start + " and end " + end
        + ". Start cannot be greater than end");
    Preconditions.checkArgument(end + store.start() <= store.capacity(), "Invalid end " + end
        + ". It cannot be greater than capacity (" + (store.capacity() - store.start()) + ")");
    _store = store;
    _flushable = flushable;
    _viewStart = start + store.start();
    _viewEnd = end + store.start();

    _store.writePosition(_store.capacity());
  }

  public ChronicleDataBuffer(ChronicleDataBuffer other, long start, long end) {
    this(other._store, false, other._flushable, start, end);
  }

  @Override
  public byte getByte(long offset) {
    return _store.readByte(offset);
  }

  @Override
  public void putByte(long offset, byte value) {
    _store.writeByte(offset, value);
  }

  @Override
  public char getChar(long offset) {
    return (char) _store.readUnsignedShort(offset);
  }

  @Override
  public void putChar(long offset, char value) {
    _store.writeUnsignedShort(offset + _viewStart, value);
  }

  @Override
  public short getShort(long offset) {
    return _store.readShort(offset + _viewStart);
  }

  @Override
  public void putShort(long offset, short value) {
    _store.writeShort(offset + _viewStart, value);
  }

  @Override
  public int getInt(long offset) {
    return _store.readInt(offset + _viewStart);
  }

  @Override
  public void putInt(long offset, int value) {
    _store.writeInt(offset + _viewStart, value);
  }

  @Override
  public long getLong(long offset) {
    return _store.readLong(offset + _viewStart);
  }

  @Override
  public void putLong(long offset, long value) {
    _store.writeLong(offset + _viewStart, value);
  }

  @Override
  public float getFloat(long offset) {
    return _store.readFloat(offset + _viewStart);
  }

  @Override
  public void putFloat(long offset, float value) {
    _store.writeFloat(offset + _viewStart, value);
  }

  @Override
  public double getDouble(long offset) {
    return _store.readDouble(offset + _viewStart);
  }

  @Override
  public void putDouble(long offset, double value) {
    _store.writeDouble(offset + _viewStart, value);
  }

  @Override
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    Bytes<byte[]> dest = Bytes.wrapForWrite(buffer);
    dest.writePosition(destOffset);
    dest.writeLimit(destOffset + size);
    _store.readPosition(offset + _viewStart);
    _store.copyTo(dest);
  }

  @Override
  public void copyTo(long offset, PinotDataBuffer buffer, long destOffset, long size) {
    long actualSize = Math.min(size, buffer.size() - destOffset);
    actualSize = Math.min(actualSize, _store.capacity() - offset + _viewStart);

    if (buffer instanceof ChronicleDataBuffer) {
      ChronicleDataBuffer other = (ChronicleDataBuffer) buffer;
      if (_store == other._store) {
        _store.move(offset + _viewStart, destOffset + other._viewStart, size);
      } else {
        _store.readPosition(offset + _viewStart);
        other._store.writePosition(destOffset + other._viewStart);
        _store.copyTo(other._store);
      }
    } else {
      long copied = 0;
      while (copied < actualSize) {
        int subSize = Math.min((int) (actualSize - copied), Integer.MAX_VALUE);
        _store.readPosition(offset + _viewStart + copied);
        _store.readLimit(offset + _viewStart + copied + subSize);
        BytesStore<?, ?> into = BytesStore.wrap(buffer.toDirectByteBuffer(destOffset, subSize));
        copied += _store.copyTo(into);
      }
      _store.readLimit(_store.capacity());
    }
  }

  @Override
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    _store.write(offset + _viewStart, buffer, srcOffset, size);
  }

  @Override
  public void readFrom(long offset, ByteBuffer buffer) {
    ByteBuffer nativeBuf;
    if (buffer.order() == ByteOrder.nativeOrder()) {
      nativeBuf = buffer;
    } else {
      nativeBuf = buffer.duplicate().order(ByteOrder.nativeOrder());
    }
    _store.write(offset + _viewStart, nativeBuf, nativeBuf.position(), nativeBuf.remaining());
  }

  @Override
  public void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException {
    checkLimits(_viewEnd - _viewStart, offset, size);
    try (FileInputStream fos = new FileInputStream(file); FileChannel channel = fos.getChannel()) {
      if (srcOffset + size > file.length()) {
        throw new IllegalArgumentException("Final position cannot be larger than the file length");
      }
      channel.position(srcOffset);

      int len;
      byte[] buffer = new byte[4096];
      long offsetOnStore = offset + _viewStart;
      long pendingBytes = size - srcOffset;
      while (pendingBytes >= 0 && (len = fos.read(buffer, 0, Math.min((int) pendingBytes, 4096))) > 0) {
        _store.write(offsetOnStore, buffer, 0, len);
        offsetOnStore += len;
        pendingBytes -= len;
      }
    }
  }

  @Override
  public long size() {
    return _viewEnd - _viewStart;
  }

  @Override
  public ByteOrder order() {
    return _store.byteOrder();
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    Preconditions.checkArgument(start >= 0, "Start cannot be negative");
    Preconditions.checkArgument(start < _viewEnd - _viewStart,
        "Start cannot be larger than this buffer capacity");
    Preconditions.checkArgument(end <= _viewEnd, "End cannot be higher than this buffer capacity");
    ChronicleDataBuffer buffer = new ChronicleDataBuffer(this, start + _viewStart, end + _viewEnd);

    return byteOrder == ByteOrder.nativeOrder() ? buffer : new NonNativePinotDataBuffer(buffer);
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    Object underlying = _store.underlyingObject();
    if (underlying instanceof ByteBuffer) {
      return ((ByteBuffer) underlying)
          .duplicate()
          .position((int) offset)
          .limit((int) offset + size)
          .slice()
          .order(byteOrder);
    }
    return ByteBufferUtil.newDirectByteBuffer(_store.addressForRead(size) + offset + _viewStart, size, _store)
        .order(byteOrder);
  }

  @Override
  public void flush() {
    if (_flushable && _store instanceof MappedBytes) {
      _store.writePosition(_store.capacity());
      ((MappedBytes) _store).sync();
    }
  }

  @Override
  public synchronized void release()
      throws IOException {
    if (!_closed) {
      _closed = true;
      _store.releaseLast();
    }
  }
}
