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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import javax.annotation.concurrent.ThreadSafe;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.mmap.MMapBuffer;


@ThreadSafe
public abstract class BasePinotLBuffer extends PinotDataBuffer {
  protected final LBufferAPI _buffer;
  private final boolean _flushable;

  protected BasePinotLBuffer(LBufferAPI buffer, boolean closeable, boolean flushable) {
    super(closeable);
    _buffer = buffer;
    _flushable = flushable;
  }

  @Override
  public byte getByte(int offset) {
    return _buffer.getByte(offset);
  }

  @Override
  public byte getByte(long offset) {
    return _buffer.getByte(offset);
  }

  @Override
  public void putByte(int offset, byte value) {
    _buffer.putByte(offset, value);
  }

  @Override
  public void putByte(long offset, byte value) {
    _buffer.putByte(offset, value);
  }

  @Override
  public void copyTo(long offset, PinotDataBuffer buffer, long destOffset, long size) {
    if (buffer instanceof BasePinotLBuffer) {
      _buffer.copyTo(offset, ((BasePinotLBuffer) buffer)._buffer, destOffset, size);
    } else {
      super.copyTo(offset, buffer, destOffset, size);
    }
  }

  @Override
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    if (size <= BULK_BYTES_PROCESSING_THRESHOLD) {
      int end = srcOffset + size;
      for (int i = srcOffset; i < end; i++) {
        _buffer.putByte(offset++, buffer[i]);
      }
    } else {
      _buffer.toDirectByteBuffer(offset, size).put(buffer, srcOffset, size);
    }
  }

  @Override
  public void readFrom(long offset, ByteBuffer buffer) {
    _buffer.toDirectByteBuffer(offset, buffer.remaining()).put(buffer);
  }

  @Override
  public void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException {
    try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel()) {
      if (size <= Integer.MAX_VALUE) {
        fileChannel.read(_buffer.toDirectByteBuffer(offset, (int) size), srcOffset);
      } else {
        while (size > Integer.MAX_VALUE) {
          fileChannel.read(_buffer.toDirectByteBuffer(offset, Integer.MAX_VALUE), srcOffset);
          offset += Integer.MAX_VALUE;
          srcOffset += Integer.MAX_VALUE;
          size -= Integer.MAX_VALUE;
        }
        fileChannel.read(_buffer.toDirectByteBuffer(offset, (int) size), srcOffset);
      }
    }
  }

  @Override
  public long size() {
    if (_buffer instanceof MMapBuffer) {
      // Workaround to handle cases where offset is not page-aligned
      return _buffer.m.address() + _buffer.m.size() - _buffer.address();
    } else {
      return _buffer.size();
    }
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    if (byteOrder == NATIVE_ORDER) {
      return new PinotNativeOrderLBuffer(_buffer.view(start, end), false, false);
    } else {
      return new PinotNonNativeOrderLBuffer(_buffer.view(start, end), false, false);
    }
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    return _buffer.toDirectByteBuffer(offset, size).order(byteOrder);
  }

  @Override
  public void flush() {
    if (_flushable) {
      ((MMapBuffer) _buffer).flush();
    }
  }

  @Override
  public void release()
      throws IOException {
    if (_buffer instanceof LBuffer) {
      _buffer.release();
    } else if (_buffer instanceof MMapBuffer) {
      ((MMapBuffer) _buffer).close();
    }
  }
}
