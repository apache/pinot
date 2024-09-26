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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class CompoundDataBufferTest {

  private CompoundDataBuffer _compoundDataBuffer;
  private static final int BUFFER_SIZE = 32;

  @BeforeMethod
  public void setUp() {
    _compoundDataBuffer = new CompoundDataBuffer(new DataBuffer[]{
        PinotByteBuffer.wrap(new byte[BUFFER_SIZE]),
        PinotByteBuffer.wrap(new byte[BUFFER_SIZE]),
        PinotByteBuffer.wrap(new byte[BUFFER_SIZE])},
        ByteOrder.BIG_ENDIAN,
        true
    );
  }

  @DataProvider
  Object[][] thingsToWrite() {
    Random r = new Random(42);
    byte[] bytes = new byte[BUFFER_SIZE];
    r.nextBytes(bytes);
    return new Object[][] {
        {"int", r.nextInt()},
        {"float", r.nextFloat()},
        {"long", r.nextLong()},
        {"double", r.nextDouble()},
        {"bytes", bytes},
        {"char", 'a'},
        {"short", (short) r.nextInt()},
        {"byte", (byte) r.nextInt()}
    };
  }

  private int positionToWriteInSingleBuffer(int writeSize) {
    if (writeSize > BUFFER_SIZE) {
      throw new IllegalArgumentException("Write size " + writeSize + " is greater than buffer size " + BUFFER_SIZE);
    }
    return 0;
  }

  private int positionToWriteTwoBuffer(int writeSize) {
    if (writeSize > 2 * BUFFER_SIZE) {
      throw new IllegalArgumentException("Write size " + writeSize + " is greater than " + (2 * BUFFER_SIZE));
    }
    return BUFFER_SIZE - writeSize + 1;
  }

  @Test(dataProvider = "thingsToWrite")
  public void writeOnSingleBuffer(String dataType, Object value) {
    int position;
    switch (dataType) {
      case "int":
        position = positionToWriteInSingleBuffer(Integer.BYTES);
        _compoundDataBuffer.putInt(position, (int) value);
        assertEquals(_compoundDataBuffer.getInt(position), value);
        break;
      case "float":
        position = positionToWriteInSingleBuffer(Float.BYTES);
        _compoundDataBuffer.putFloat(position, (float) value);
        assertEquals(_compoundDataBuffer.getFloat(position), value);
        break;
      case "long":
        position = positionToWriteInSingleBuffer(Long.BYTES);
        _compoundDataBuffer.putLong(position, (long) value);
        assertEquals(_compoundDataBuffer.getLong(position), value);
        break;
      case "double":
        position = positionToWriteInSingleBuffer(Double.BYTES);
        _compoundDataBuffer.putDouble(position, (double) value);
        assertEquals(_compoundDataBuffer.getDouble(position), value);
        break;
      case "char":
        position = positionToWriteInSingleBuffer(Character.BYTES);
        _compoundDataBuffer.putChar(position, (char) value);
        assertEquals(_compoundDataBuffer.getChar(position), value);
        break;
      case "short":
        position = positionToWriteInSingleBuffer(Short.BYTES);
        _compoundDataBuffer.putShort(position, (short) value);
        assertEquals(_compoundDataBuffer.getShort(position), value);
        break;
      case "byte":
        position = positionToWriteInSingleBuffer(Byte.BYTES);
        _compoundDataBuffer.putByte(position, (byte) value);
        assertEquals(_compoundDataBuffer.getByte(position), value);
        break;
      case "bytes":
        byte[] bytes = (byte[]) value;
        position = positionToWriteInSingleBuffer(bytes.length);
        _compoundDataBuffer.readFrom(position, bytes);
        byte[] readBytes = new byte[bytes.length];
        _compoundDataBuffer.copyTo(position, readBytes);
        assertEquals(readBytes, bytes);
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type " + dataType);
    }
  }

  @Test(dataProvider = "thingsToWrite")
  public void writeBetweenBuffers(String dataType, Object value) {
    int position;
    switch (dataType) {
      case "int":
        position = positionToWriteTwoBuffer(Integer.BYTES);
        _compoundDataBuffer.putInt(position, (int) value);
        assertEquals(_compoundDataBuffer.getInt(position), value);
        break;
      case "long":
        position = positionToWriteTwoBuffer(Long.BYTES);
        _compoundDataBuffer.putLong(position, (long) value);
        assertEquals(_compoundDataBuffer.getLong(position), value);
        break;
      case "float":
        position = positionToWriteTwoBuffer(Float.BYTES);
        _compoundDataBuffer.putFloat(position, (float) value);
        assertEquals(_compoundDataBuffer.getFloat(position), value);
        break;
      case "double":
        position = positionToWriteTwoBuffer(Double.BYTES);
        _compoundDataBuffer.putDouble(position, (double) value);
        assertEquals(_compoundDataBuffer.getDouble(position), value);
        break;
      case "char":
        position = positionToWriteTwoBuffer(Character.BYTES);
        _compoundDataBuffer.putChar(position, (char) value);
        assertEquals(_compoundDataBuffer.getChar(position), value);
        break;
      case "short":
        position = positionToWriteTwoBuffer(Short.BYTES);
        _compoundDataBuffer.putShort(position, (short) value);
        assertEquals(_compoundDataBuffer.getShort(position), value);
        break;
      case "byte":
        position = positionToWriteTwoBuffer(Byte.BYTES);
        _compoundDataBuffer.putByte(position, (byte) value);
        assertEquals(_compoundDataBuffer.getByte(position), value);
        break;
      case "bytes":
        byte[] bytes = (byte[]) value;
        position = positionToWriteTwoBuffer(bytes.length);
        _compoundDataBuffer.readFrom(position, bytes);
        byte[] readBytes = new byte[bytes.length];
        _compoundDataBuffer.copyTo(position, readBytes);
        assertEquals(readBytes, bytes);
        break;
      default:
        throw new IllegalArgumentException("Unsupported data type " + dataType);
    }
  }

  @Test
  void readWriteByteBuffer() {
    Random r = new Random(42);
    byte[] bytes = new byte[BUFFER_SIZE];

    int position = positionToWriteTwoBuffer(bytes.length);

    r.nextBytes(bytes);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    _compoundDataBuffer.readFrom(position, buffer);
    ByteBuffer readBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    _compoundDataBuffer.copyTo(position, readBuffer, 0, BUFFER_SIZE);
    assertEquals(readBuffer.array(), bytes);
  }

  @Test
  void readWriteFile()
      throws IOException {
    Random r = new Random(42);
    byte[] bytes = new byte[BUFFER_SIZE];

    int position = positionToWriteTwoBuffer(bytes.length);

    r.nextBytes(bytes);
    File file = Files.createTempFile("test", "data").toFile();
    try {
      try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
        randomAccessFile.setLength(BUFFER_SIZE);
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file))) {
          bos.write(bytes);
        }
      }
      _compoundDataBuffer.readFrom(position, file, 0, BUFFER_SIZE);
      byte[] readBytes = new byte[BUFFER_SIZE];
      _compoundDataBuffer.copyTo(position, readBytes);
      assertEquals(readBytes, bytes);
    } finally {
      file.delete();
    }
  }

  @Test
  void testViewTotal() {
    Random r = new Random(42);
    byte[] bytes1 = new byte[BUFFER_SIZE];
    r.nextBytes(bytes1);

    byte[] bytes2 = new byte[BUFFER_SIZE];
    r.nextBytes(bytes2);

    byte[] bytes3 = new byte[BUFFER_SIZE];
    r.nextBytes(bytes3);

    _compoundDataBuffer.readFrom(0, bytes1);
    _compoundDataBuffer.readFrom(BUFFER_SIZE, bytes2);
    _compoundDataBuffer.readFrom(BUFFER_SIZE * 2L, bytes3);

    DataBuffer view = _compoundDataBuffer.view(0, BUFFER_SIZE * 3L);
    assertEquals(view.size(), BUFFER_SIZE * 3L);
    assertEquals(view.order(), ByteOrder.BIG_ENDIAN);
    assertEquals(view, _compoundDataBuffer);
  }

  @Test
  void testViewSeveralBuffers() {
    Random r = new Random(42);
    byte[] bytes1 = new byte[BUFFER_SIZE];
    r.nextBytes(bytes1);

    byte[] bytes2 = new byte[BUFFER_SIZE];
    r.nextBytes(bytes2);

    byte[] bytes3 = new byte[BUFFER_SIZE];
    r.nextBytes(bytes3);

    byte[] totalBytes = new byte[BUFFER_SIZE * 2];

    System.arraycopy(bytes1, 1, totalBytes, 0, BUFFER_SIZE - 1);
    System.arraycopy(bytes2, 0, totalBytes, BUFFER_SIZE - 1, BUFFER_SIZE);
    System.arraycopy(bytes3, 0, totalBytes, BUFFER_SIZE * 2 - 1, 1);

    DataBuffer expected = PinotByteBuffer.wrap(totalBytes);

    // prepare the compound buffer
    _compoundDataBuffer.readFrom(0, bytes1);
    _compoundDataBuffer.readFrom(BUFFER_SIZE, bytes2);
    _compoundDataBuffer.readFrom(BUFFER_SIZE * 2L, bytes3);

    // apply the view
    DataBuffer view = _compoundDataBuffer.view(1, BUFFER_SIZE * 2L + 1);
    assertEquals(view.size(), BUFFER_SIZE * 2L);
    assertEquals(view.order(), ByteOrder.BIG_ENDIAN);

    assertEquals(view, expected);
  }

  @Test
  void testViewOneBuffer() {
    Random r = new Random(42);
    byte[] bytes1 = new byte[BUFFER_SIZE];
    r.nextBytes(bytes1);

    _compoundDataBuffer.readFrom(0, bytes1);

    DataBuffer expected = PinotByteBuffer.wrap(Arrays.copyOfRange(bytes1, 1, BUFFER_SIZE - 1));

    DataBuffer view = _compoundDataBuffer.view(1, BUFFER_SIZE - 1);
    assertEquals(view.size(), BUFFER_SIZE - 2);
    assertEquals(view, expected);
  }

  @Test
  void testCopyOrViewOneBuffer() {
    Random r = new Random(42);
    byte[] bytes1 = new byte[BUFFER_SIZE];
    r.nextBytes(bytes1);

    _compoundDataBuffer.readFrom(0, bytes1);

    ByteBuffer expected = ByteBuffer.wrap(bytes1, 1, BUFFER_SIZE - 1);

    ByteBuffer copyOrView = _compoundDataBuffer.copyOrView(1, BUFFER_SIZE - 1);
    assertEquals(copyOrView, expected);
  }
}
