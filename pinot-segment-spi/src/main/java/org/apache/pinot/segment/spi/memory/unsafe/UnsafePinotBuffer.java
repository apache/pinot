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
package org.apache.pinot.segment.spi.memory.unsafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.pinot.segment.spi.memory.ByteBufferUtil;
import org.apache.pinot.segment.spi.memory.NonNativePinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import sun.misc.Unsafe;


/**
 * A {@link PinotDataBuffer} that uses {@link Unsafe} and can only read native byte order.
 */
public class UnsafePinotBuffer extends PinotDataBuffer {

  private final long _address;
  private final long _size;
  private final Memory _memory;

  public UnsafePinotBuffer(Memory memory, boolean ownsMemory) {
    this(memory, ownsMemory, memory.getAddress(), memory.getSize());
  }

  private UnsafePinotBuffer(Memory memory, boolean ownsMemory, long address, long size) {
    super(ownsMemory);
    _memory = memory;
    _size = size;
    _address = address;
  }

  void checkOffset(long offset, long size) {
    if (offset < 0) {
      throw new IllegalArgumentException("Offset is " + offset);
    }
    if (offset + size > _size) {
      throw new IllegalArgumentException("Cannot apply a " + size + " byte length operation in offset " + offset);
    }
  }

  @Override
  public byte getByte(long offset) {
    checkOffset(offset, 1);
    return Unsafer.UNSAFE.getByte(_address + offset);
  }

  @Override
  public void putByte(long offset, byte value) {
    checkOffset(offset, 1);
    Unsafer.UNSAFE.putByte(_address + offset, value);
  }

  @Override
  public char getChar(long offset) {
    checkOffset(offset, 2);
    return Unsafer.UNSAFE.getChar(_address + offset);
  }

  @Override
  public void putChar(long offset, char value) {
    checkOffset(offset, 2);
    Unsafer.UNSAFE.putChar(_address + offset, value);
  }

  @Override
  public short getShort(long offset) {
    checkOffset(offset, 2);
    return Unsafer.UNSAFE.getShort(_address + offset);
  }

  @Override
  public void putShort(long offset, short value) {
    checkOffset(offset, 2);
    Unsafer.UNSAFE.putShort(_address + offset, value);
  }

  @Override
  public int getInt(long offset) {
    checkOffset(offset, 4);
    return Unsafer.UNSAFE.getInt(_address + offset);
  }

  @Override
  public void putInt(long offset, int value) {
    checkOffset(offset, 4);
    Unsafer.UNSAFE.putInt(_address + offset, value);
  }

  @Override
  public long getLong(long offset) {
    checkOffset(offset, 8);
    return Unsafer.UNSAFE.getLong(_address + offset);
  }

  @Override
  public void putLong(long offset, long value) {
    checkOffset(offset, 8);
    Unsafer.UNSAFE.putLong(_address + offset, value);
  }

  @Override
  public float getFloat(long offset) {
    checkOffset(offset, 4);
    return Unsafer.UNSAFE.getFloat(_address + offset);
  }

  @Override
  public void putFloat(long offset, float value) {
    checkOffset(offset, 4);
    Unsafer.UNSAFE.putFloat(_address + offset, value);
  }

  @Override
  public double getDouble(long offset) {
    checkOffset(offset, 8);
    return Unsafer.UNSAFE.getDouble(_address + offset);
  }

  @Override
  public void putDouble(long offset, double value) {
    checkOffset(offset, 8);
    Unsafer.UNSAFE.putDouble(_address + offset, value);
  }

  @Override
  public long size() {
    return _size;
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.nativeOrder();
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    long size = end - start;
    checkOffset(start, size);

    UnsafePinotBuffer nativeView = new UnsafePinotBuffer(_memory, false, _address + start, size);

    if (byteOrder == ByteOrder.nativeOrder()) {
      return nativeView;
    } else {
      return new NonNativePinotDataBuffer(nativeView);
    }
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    checkOffset(offset, size);
    return ByteBufferUtil.newDirectByteBuffer(_address + offset, size, this)
        .order(byteOrder);
  }

  @Override
  public void flush() {
    _memory.flush();
  }

  @Override
  public void release()
      throws IOException {
    _memory.close();
  }
}
