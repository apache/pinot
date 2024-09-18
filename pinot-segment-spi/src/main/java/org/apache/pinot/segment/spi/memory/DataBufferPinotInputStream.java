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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


/**
 * An adaptor that allows a {@link DataBuffer} to be read as a {@link PinotInputStream}.
 */
public class DataBufferPinotInputStream extends PinotInputStream {
  private final DataBuffer _dataBuffer;
  private long _currentOffset;

  public DataBufferPinotInputStream(DataBuffer dataBuffer) {
    this(dataBuffer, 0, dataBuffer.size());
  }

  public DataBufferPinotInputStream(DataBuffer dataBuffer, long startOffset, long endOffset) {
    _dataBuffer = dataBuffer.view(startOffset, endOffset, ByteOrder.BIG_ENDIAN);
    _currentOffset = 0;
  }

  @Override
  public long getCurrentOffset() {
    return _currentOffset;
  }

  @Override
  public void seek(long newPos) {
    if (newPos < 0 || newPos > _dataBuffer.size()) {
      throw new IllegalArgumentException("Invalid new position: " + newPos);
    }
    _currentOffset = newPos;
  }

  @Override
  public int read(ByteBuffer buf) {
    int remaining = available();
    if (remaining == 0) {
      return -1;
    }
    PinotByteBuffer wrap = PinotByteBuffer.wrap(buf);
    int toRead = Math.min(remaining, buf.remaining());
    if (toRead > 0) {
      _dataBuffer.copyTo(_currentOffset, wrap, 0, toRead);
      _currentOffset += toRead;
    }

    return toRead;
  }

  @Override
  public int read() {
    if (_currentOffset >= _dataBuffer.size()) {
      return -1;
    } else {
      return _dataBuffer.getByte(_currentOffset++) & 0xFF;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) {
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException("off=" + off + ", len=" + len + ", b.length=" + b.length);
    }
    int available = available();
    if (available == 0) {
      return -1;
    }
    int result = Math.min(available, len);
    _dataBuffer.copyTo(_currentOffset, b, off, result);
    _currentOffset += result;
    return result;
  }

  @Override
  public long skip(long n) {
    long increase = Math.min(n, availableLong());
    _currentOffset += increase;
    return increase;
  }

  public long availableLong() {
    return _dataBuffer.size() - _currentOffset;
  }

  @Override
  public int available() {
    long available = _dataBuffer.size() - _currentOffset;
    if (available > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) available;
    }
  }

  @Override
  public void readFully(byte[] b, int off, int len)
      throws EOFException {
    if (len < 0) {
      throw new IndexOutOfBoundsException("len is negative: " + len);
    }
    if (off < 0 || off + len > b.length) {
      throw new IndexOutOfBoundsException("off=" + off + ", len=" + len + ", b.length=" + b.length);
    }
    boolean eof = availableLong() < len;
    _dataBuffer.copyTo(_currentOffset, b, off, len);
    _currentOffset += len;
    if (eof) {
      throw new EOFException();
    }
  }

  @Override
  public boolean readBoolean()
      throws EOFException {
    if (_currentOffset >= _dataBuffer.size()) {
      throw new EOFException();
    }
    return _dataBuffer.getByte(_currentOffset++) != 0;
  }

  @Override
  public byte readByte()
      throws EOFException {
    if (availableLong() < 1) {
      throw new EOFException();
    }
    return _dataBuffer.getByte(_currentOffset++);
  }

  @Override
  public int readUnsignedByte()
      throws EOFException {
    if (availableLong() < 1) {
      throw new EOFException();
    }
    return _dataBuffer.getByte(_currentOffset++) & 0xFF;
  }

  @Override
  public short readShort()
      throws EOFException {
    if (availableLong() < 2) {
      throw new EOFException();
    }
    short result = _dataBuffer.getShort(_currentOffset);
    _currentOffset += 2;
    return result;
  }

  @Override
  public int readUnsignedShort()
      throws EOFException {
    return readShort() & 0xFFFF;
  }

  @Override
  public char readChar()
      throws EOFException {
    return (char) readUnsignedShort();
  }

  @Override
  public int readInt()
      throws EOFException {
    if (availableLong() < 4) {
      throw new EOFException();
    }
    int result = _dataBuffer.getInt(_currentOffset);
    _currentOffset += 4;
    return result;
  }

  @Override
  public long readLong()
      throws EOFException {
    if (availableLong() < 8) {
      throw new EOFException();
    }
    long result = _dataBuffer.getLong(_currentOffset);
    _currentOffset += 8;
    return result;
  }

  @Override
  public float readFloat()
      throws EOFException {
    if (availableLong() < 4) {
      throw new EOFException();
    }
    float result = _dataBuffer.getFloat(_currentOffset);
    _currentOffset += 4;
    return result;
  }

  @Override
  public double readDouble()
      throws EOFException {
    if (availableLong() < 8) {
      throw new EOFException();
    }
    double result = _dataBuffer.getDouble(_currentOffset);
    _currentOffset += 8;
    return result;
  }

  @Deprecated
  @Override
  public String readLine()
      throws IOException {
    return new DataInputStream(this).readLine();
  }

  @Override
  public String readUTF()
      throws IOException {
    return new DataInputStream(this).readUTF();
  }
}
