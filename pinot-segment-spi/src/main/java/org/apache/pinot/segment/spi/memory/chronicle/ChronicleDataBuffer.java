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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytesStore;
import org.apache.pinot.segment.spi.memory.ByteBufferUtil;
import org.apache.pinot.segment.spi.memory.NonNativePinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public class ChronicleDataBuffer extends PinotDataBuffer {

  private final BytesStore<?, ?> _store;
  private final boolean _flushable;

  public ChronicleDataBuffer(BytesStore<?, ?> store, boolean closeable, boolean flushable) {
    super(closeable);
    _store = store;
    _flushable = flushable;
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
    _store.writeUnsignedShort(offset, value);
  }

  @Override
  public short getShort(long offset) {
    return _store.readShort(offset);
  }

  @Override
  public void putShort(long offset, short value) {
    _store.writeShort(offset, value);
  }

  @Override
  public int getInt(long offset) {
    return _store.readInt(offset);
  }

  @Override
  public void putInt(long offset, int value) {
    _store.writeInt(offset, value);
  }

  @Override
  public long getLong(long offset) {
    return _store.readLong(offset);
  }

  @Override
  public void putLong(long offset, long value) {
    _store.writeLong(offset, value);
  }

  @Override
  public float getFloat(long offset) {
    return _store.readFloat(offset);
  }

  @Override
  public void putFloat(long offset, float value) {
    _store.writeFloat(offset, value);
  }

  @Override
  public double getDouble(long offset) {
    return _store.readDouble(offset);
  }

  @Override
  public void putDouble(long offset, double value) {
    _store.writeDouble(offset, value);
  }

  @Override
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    Bytes<byte[]> dest = Bytes.wrapForWrite(buffer);
    dest.writePosition(destOffset);
    dest.writeLimit(destOffset + size);
    _store.copyTo(dest);
  }

  @Override
  public void copyTo(long offset, PinotDataBuffer buffer, long destOffset, long size) {
    long actualSize = Math.min(size, buffer.size() - destOffset);
    actualSize = Math.min(actualSize, _store.length() - offset);

    if (buffer instanceof ChronicleDataBuffer) {
      ChronicleDataBuffer other = (ChronicleDataBuffer) buffer;
      BytesStore<?, ?> dest = other._store.subBytes(destOffset, size);
      _store.copyTo(dest);
    } else {
      long read = 0;
      byte[] arr = new byte[4096];
      while (read < actualSize) {
        long offsetOnStore = offset + read;
        int bytesToRead = Math.min((int) (actualSize - read), 4096);
        long inc = _store.read(offsetOnStore, arr, 0, bytesToRead);
        assert inc == bytesToRead;
        buffer.readFrom(offsetOnStore, arr, 0, bytesToRead);
        read += inc;
      }
    }
  }

  @Override
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    _store.write(offset, buffer, srcOffset, size);
  }

  @Override
  public void readFrom(long offset, ByteBuffer buffer) {
    _store.write(offset, buffer, buffer.position(), buffer.remaining());
  }

  @Override
  public void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException {
    try (FileInputStream fos = new FileInputStream(file); FileChannel channel = fos.getChannel()) {
      channel.position(offset);

      int len;
      byte[] buffer = new byte[4096];
      long offsetOnStore = offset;
      while ((len = fos.read(buffer)) > 0) {
        _store.write(offsetOnStore, buffer, 0, len);
        offsetOnStore += len;
      }
    }
  }

  @Override
  public long size() {
    return _store.capacity();
  }

  @Override
  public ByteOrder order() {
    return _store.byteOrder();
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    ChronicleDataBuffer buffer = new ChronicleDataBuffer(_store.subBytes(start, end - start), false, false);

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
    return ByteBufferUtil.newDirectByteBuffer(_store.addressForRead(size), size, _store)
        .order(byteOrder);
  }

  @Override
  public void flush() {
    if (_flushable && _store instanceof MappedBytesStore) {
      ((MappedBytesStore) _store).syncUpTo(_store.capacity());
    }
  }

  @Override
  public void release()
      throws IOException {
    _store.releaseLast();
  }
}
