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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DataBufferPinotInputStreamTest {

  ByteBuffer _byteBuffer;
  DataBuffer _dataBuffer;
  DataBufferPinotInputStream _dataBufferPinotInputStream;
  private static final int BUFFER_SIZE = 32;

  @BeforeMethod
  public void setUp() {
    Random r = new Random(42);

    byte[] buffer = new byte[BUFFER_SIZE];
    r.nextBytes(buffer);

    _byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN);
    _dataBuffer = PinotByteBuffer.wrap(_byteBuffer);
    _dataBufferPinotInputStream = new DataBufferPinotInputStream(_dataBuffer);
  }

  @Test
  void readByteBuffer() {
    byte[] buffer = new byte[BUFFER_SIZE];
    ByteBuffer wrap = ByteBuffer.wrap(buffer);
    _dataBufferPinotInputStream.read(wrap);

    assertEquals(wrap, _byteBuffer);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), BUFFER_SIZE);
  }

  @Test
  void readByteMovesCursor() {
    int read = _dataBufferPinotInputStream.read();
    assertEquals(read, _byteBuffer.get(0) & 0xFF);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), 1);
  }

  @Test
  void seekMovesCursor() {
    _dataBufferPinotInputStream.seek(1);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), 1);

    int read = _dataBufferPinotInputStream.read();
    assertEquals(read, _byteBuffer.get(1) & 0xFF);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), 2);
  }

  @Test
  void readAtEndReturnsMinusOne() {
    _dataBufferPinotInputStream.seek(BUFFER_SIZE);
    assertEquals(_dataBufferPinotInputStream.read(), -1);
  }

  @Test
  void testSkip() {
    _dataBufferPinotInputStream.seek(1);
    long skip = _dataBufferPinotInputStream.skip(2);
    assertEquals(skip, 2);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), 3);
  }

  @Test
  void testSkipEnd() {
    _dataBufferPinotInputStream.seek(BUFFER_SIZE - 1);
    long skip = _dataBufferPinotInputStream.skip(2);
    assertEquals(skip, 1);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), BUFFER_SIZE);
  }

  @Test
  void testAvailable() {
    assertEquals(_dataBufferPinotInputStream.available(), BUFFER_SIZE);
    _dataBufferPinotInputStream.seek(1);
    assertEquals(_dataBufferPinotInputStream.available(), BUFFER_SIZE - 1);
  }

  @Test
  void testReadInt()
      throws EOFException {
    int read = _dataBufferPinotInputStream.readInt();

    assertEquals(read, _byteBuffer.getInt(0));
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), Integer.BYTES);
  }

  @Test
  void testReadLong()
      throws EOFException {
    long read = _dataBufferPinotInputStream.readLong();

    assertEquals(read, _byteBuffer.getLong(0));
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), Long.BYTES);
  }

  @Test
  void testReadShort()
      throws EOFException {
    short read = _dataBufferPinotInputStream.readShort();

    assertEquals(read, _byteBuffer.getShort(0));
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), Short.BYTES);
  }

  @Test
  void testReadBoolean()
      throws EOFException {
    boolean read = _dataBufferPinotInputStream.readBoolean();

    assertEquals(read, _byteBuffer.get(0) != 0);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), 1);
  }

  @Test
  void testReadByte()
      throws EOFException {
    int read = _dataBufferPinotInputStream.readByte();

    assertEquals(read, _byteBuffer.get(0) & 0xFF);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), 1);
  }

  @Test
  void testReadUnsignedByte()
      throws EOFException {
    int read = _dataBufferPinotInputStream.readUnsignedByte();

    assertEquals(read, _byteBuffer.get(0) & 0xFF);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), 1);
  }

  @Test
  void testReadUnsignedShort()
      throws EOFException {
    int read = _dataBufferPinotInputStream.readUnsignedShort();

    assertEquals(read, _byteBuffer.getShort(0) & 0xFFFF);
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), Short.BYTES);
  }

  @Test
  void testReadFloat()
      throws EOFException {
    float read = _dataBufferPinotInputStream.readFloat();

    assertEquals(read, _byteBuffer.getFloat(0));
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), Float.BYTES);
  }

  @Test
  void testReadDouble()
      throws EOFException {
    double read = _dataBufferPinotInputStream.readDouble();

    assertEquals(read, _byteBuffer.getDouble(0));
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), Double.BYTES);
  }

  @Test
  void testReadChar()
      throws EOFException {
    char read = _dataBufferPinotInputStream.readChar();

    assertEquals(read, _byteBuffer.getChar(0));
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), Character.BYTES);
  }

  @Test
  void testReadFully()
      throws IOException {
    byte[] buffer = new byte[BUFFER_SIZE];
    _dataBufferPinotInputStream.readFully(buffer);

    for (int i = 0; i < BUFFER_SIZE; i++) {
      assertEquals(buffer[i], _byteBuffer.get(i));
    }
    assertEquals(_dataBufferPinotInputStream.getCurrentOffset(), BUFFER_SIZE);
  }
}
