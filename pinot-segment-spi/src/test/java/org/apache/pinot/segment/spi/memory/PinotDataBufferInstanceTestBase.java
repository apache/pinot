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

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * A test contract that all pinot data buffer implementations should include.
 */
public abstract class PinotDataBufferInstanceTestBase extends PinotDataBufferTestBase {

  public final PinotBufferFactory _factory;

  public PinotDataBufferInstanceTestBase(PinotBufferFactory factory) {
    _factory = factory;
  }

  @BeforeMethod
  public void setFactory() {
    PinotDataBuffer.useFactory(_factory);
  }

  @AfterTest
  public void cleanFactory() {
    PinotDataBuffer.useFactory(PinotDataBuffer.createDefaultFactory());
  }

  @BeforeMethod
  public void cleanStats() {
    PinotDataBuffer.cleanStats();
  }

  @SuppressWarnings("RedundantExplicitClose")
  @Test
  public void testMultipleClose()
      throws Exception {
    try (PinotDataBuffer buffer = _factory.allocateDirect(BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
      buffer.close();
    }
    try (PinotDataBuffer buffer = _factory.allocateDirect(BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
      buffer.close();
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = _factory.readFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
        buffer.close();
      }
      try (PinotDataBuffer buffer = _factory.readFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
        buffer.close();
      }
      try (PinotDataBuffer buffer = _factory
          .mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
        buffer.close();
      }
      try (PinotDataBuffer buffer =
          _factory.mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
        buffer.close();
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testDirectBE()
      throws Exception {
    try (PinotDataBuffer buffer = _factory.allocateDirect(BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
      Assert.assertSame(buffer.order(), ByteOrder.BIG_ENDIAN);
      testPinotDataBuffer(buffer);
    }
  }

  @Test
  public void testDirectLE()
      throws Exception {
    try (PinotDataBuffer buffer = PinotByteBuffer.allocateDirect(BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
      Assert.assertSame(buffer.order(), ByteOrder.LITTLE_ENDIAN);
      testPinotDataBuffer(buffer);
    }
  }

  @Test
  public void testReadFileBE()
      throws Exception {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = _factory
          .readFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
        Assert.assertSame(buffer.order(), ByteOrder.BIG_ENDIAN);
        testPinotDataBuffer(buffer);
      }
    }
  }

  @Test
  public void testReadFileLE()
      throws Exception {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = _factory
          .readFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
        Assert.assertSame(buffer.order(), ByteOrder.LITTLE_ENDIAN);
        testPinotDataBuffer(buffer);
      }
    }
  }

  @Test
  public void testMapFileBE()
      throws Exception {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = _factory
          .mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
        Assert.assertSame(buffer.order(), ByteOrder.BIG_ENDIAN);
        testPinotDataBuffer(buffer);
      }
    }
  }

  @Test
  public void testMapFileLE()
      throws Exception {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = _factory
          .mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
        Assert.assertSame(buffer.order(), ByteOrder.LITTLE_ENDIAN);
        testPinotDataBuffer(buffer);
      }
    }
  }

  protected void testPinotDataBuffer(PinotDataBuffer buffer)
      throws Exception {
    Assert.assertEquals(buffer.size(), BUFFER_SIZE);
    testReadWriteByte(buffer);
    testReadWriteChar(buffer);
    testReadWriteShort(buffer);
    testReadWriteInt(buffer);
    testReadWriteLong(buffer);
    testReadWriteFloat(buffer);
    testReadWriteDouble(buffer);
    testReadWriteBytes(buffer);
    testReadWritePinotDataBuffer(buffer);
    testReadFromByteBuffer(buffer);
    testConcurrentReadWrite(buffer);
  }

  protected void testReadWriteByte(PinotDataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int intOffset = RANDOM.nextInt(BUFFER_SIZE);
      buffer.putByte(intOffset, _bytes[i]);
      Assert.assertEquals(buffer.getByte(intOffset), _bytes[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      long longOffset = RANDOM.nextInt(BUFFER_SIZE);
      buffer.putByte(longOffset, _bytes[i]);
      Assert.assertEquals(buffer.getByte(longOffset), _bytes[i]);
    }
  }

  protected void testReadWriteChar(PinotDataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(CHAR_ARRAY_LENGTH);
      int intOffset = index * Byte.BYTES;
      buffer.putChar(intOffset, _chars[i]);
      Assert.assertEquals(buffer.getChar(intOffset), _chars[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(CHAR_ARRAY_LENGTH);
      long longOffset = index * Byte.BYTES;
      buffer.putChar(longOffset, _chars[i]);
      Assert.assertEquals(buffer.getChar(longOffset), _chars[i]);
    }
  }

  protected void testReadWriteShort(PinotDataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(SHORT_ARRAY_LENGTH);
      int intOffset = index * Byte.BYTES;
      buffer.putShort(intOffset, _shorts[i]);
      Assert.assertEquals(buffer.getShort(intOffset), _shorts[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(SHORT_ARRAY_LENGTH);
      long longOffset = index * Byte.BYTES;
      buffer.putShort(longOffset, _shorts[i]);
      Assert.assertEquals(buffer.getShort(longOffset), _shorts[i]);
    }
  }

  protected void testReadWriteInt(PinotDataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(INT_ARRAY_LENGTH);
      int intOffset = index * Byte.BYTES;
      buffer.putInt(intOffset, _ints[i]);
      Assert.assertEquals(buffer.getInt(intOffset), _ints[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(INT_ARRAY_LENGTH);
      long longOffset = index * Byte.BYTES;
      buffer.putInt(longOffset, _ints[i]);
      Assert.assertEquals(buffer.getInt(longOffset), _ints[i]);
    }
  }

  protected void testReadWriteLong(PinotDataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(LONG_ARRAY_LENGTH);
      int intOffset = index * Byte.BYTES;
      buffer.putLong(intOffset, _longs[i]);
      Assert.assertEquals(buffer.getLong(intOffset), _longs[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(LONG_ARRAY_LENGTH);
      long longOffset = index * Byte.BYTES;
      buffer.putLong(longOffset, _longs[i]);
      Assert.assertEquals(buffer.getLong(longOffset), _longs[i]);
    }
  }

  protected void testReadWriteFloat(PinotDataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(FLOAT_ARRAY_LENGTH);
      int intOffset = index * Byte.BYTES;
      buffer.putFloat(intOffset, _floats[i]);
      Assert.assertEquals(buffer.getFloat(intOffset), _floats[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(FLOAT_ARRAY_LENGTH);
      long longOffset = index * Byte.BYTES;
      buffer.putFloat(longOffset, _floats[i]);
      Assert.assertEquals(buffer.getFloat(longOffset), _floats[i]);
    }
  }

  protected void testReadWriteDouble(PinotDataBuffer buffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(DOUBLE_ARRAY_LENGTH);
      int intOffset = index * Byte.BYTES;
      buffer.putDouble(intOffset, _doubles[i]);
      Assert.assertEquals(buffer.getDouble(intOffset), _doubles[i]);
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(DOUBLE_ARRAY_LENGTH);
      long longOffset = index * Byte.BYTES;
      buffer.putDouble(longOffset, _doubles[i]);
      Assert.assertEquals(buffer.getDouble(longOffset), _doubles[i]);
    }
  }

  protected void testReadWriteBytes(PinotDataBuffer buffer) {
    byte[] readBuffer = new byte[MAX_BYTES_LENGTH];
    byte[] writeBuffer = new byte[MAX_BYTES_LENGTH];
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int length = RANDOM.nextInt(MAX_BYTES_LENGTH);
      int offset = RANDOM.nextInt(BUFFER_SIZE - length);
      int arrayOffset = RANDOM.nextInt(MAX_BYTES_LENGTH - length);
      System.arraycopy(_bytes, offset, readBuffer, arrayOffset, length);
      buffer.readFrom(offset, readBuffer, arrayOffset, length);
      buffer.copyTo(offset, writeBuffer, arrayOffset, length);
      int end = arrayOffset + length;
      for (int j = arrayOffset; j < end; j++) {
        Assert.assertEquals(writeBuffer[j], readBuffer[j]);
      }
    }
  }

  protected void testReadWritePinotDataBuffer(PinotDataBuffer buffer) {
    testReadWritePinotDataBuffer(buffer, PinotByteBuffer.allocateDirect(MAX_BYTES_LENGTH, PinotDataBuffer.NATIVE_ORDER),
        PinotByteBuffer.allocateDirect(MAX_BYTES_LENGTH, PinotDataBuffer.NON_NATIVE_ORDER));
    testReadWritePinotDataBuffer(buffer,
        PinotByteBuffer.allocateDirect(2 * MAX_BYTES_LENGTH, PinotDataBuffer.NON_NATIVE_ORDER)
            .view(MAX_BYTES_LENGTH, 2 * MAX_BYTES_LENGTH),
        PinotByteBuffer.allocateDirect(2 * MAX_BYTES_LENGTH, PinotDataBuffer.NATIVE_ORDER)
            .view(MAX_BYTES_LENGTH, 2 * MAX_BYTES_LENGTH));
    testReadWritePinotDataBuffer(buffer, _factory.allocateDirect(MAX_BYTES_LENGTH, ByteOrder.nativeOrder()),
        _factory.allocateDirect(MAX_BYTES_LENGTH, PinotDataBuffer.NON_NATIVE_ORDER));
    testReadWritePinotDataBuffer(buffer,
        _factory.allocateDirect(2 * MAX_BYTES_LENGTH, ByteOrder.nativeOrder())
            .view(MAX_BYTES_LENGTH, 2 * MAX_BYTES_LENGTH),
        _factory.allocateDirect(2 * MAX_BYTES_LENGTH, PinotDataBuffer.NON_NATIVE_ORDER)
            .view(MAX_BYTES_LENGTH, 2 * MAX_BYTES_LENGTH));
  }

  protected void testReadWritePinotDataBuffer(PinotDataBuffer buffer, PinotDataBuffer readBuffer,
      PinotDataBuffer writeBuffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int length = RANDOM.nextInt(MAX_BYTES_LENGTH);
      int offset = RANDOM.nextInt(BUFFER_SIZE - length);
      readBuffer.readFrom(0, _bytes, RANDOM.nextInt(BUFFER_SIZE - length), length);
      readBuffer.copyTo(0, buffer, offset, length);
      for (int j = 0; j < length; j++) {
        Assert.assertEquals(buffer.getByte(j + offset), readBuffer.getByte(j));
      }
      buffer.copyTo(offset, writeBuffer, 0, length);
      for (int j = 0; j < length; j++) {
        Assert.assertEquals(writeBuffer.getByte(j), readBuffer.getByte(j));
      }
    }
  }

  protected void testReadFromByteBuffer(PinotDataBuffer buffer) {
    byte[] readBuffer = new byte[MAX_BYTES_LENGTH];
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int length = RANDOM.nextInt(MAX_BYTES_LENGTH);
      int offset = RANDOM.nextInt(BUFFER_SIZE - length);
      System.arraycopy(_bytes, offset, readBuffer, 0, length);
      buffer.readFrom(offset, ByteBuffer.wrap(readBuffer, 0, length));
      for (int j = 0; j < length; j++) {
        Assert.assertEquals(buffer.getByte(offset + j), readBuffer[j]);
      }
    }
  }

  protected void testConcurrentReadWrite(PinotDataBuffer buffer)
      throws Exception {
    Future[] futures = new Future[NUM_ROUNDS];
    for (int i = 0; i < NUM_ROUNDS; i++) {
      futures[i] = _executorService.submit(() -> {
        PinotDataBuffer.useFactory(_factory);
        int length = RANDOM.nextInt(MAX_BYTES_LENGTH);
        int offset = RANDOM.nextInt(BUFFER_SIZE - length);
        byte[] readBuffer = new byte[length];
        byte[] writeBuffer = new byte[length];
        System.arraycopy(_bytes, offset, readBuffer, 0, length);
        buffer.readFrom(offset, readBuffer);
        buffer.copyTo(offset, writeBuffer);
        Assert.assertTrue(Arrays.equals(readBuffer, writeBuffer));
        buffer.readFrom(offset, ByteBuffer.wrap(readBuffer));
        buffer.copyTo(offset, writeBuffer);
        Assert.assertTrue(Arrays.equals(readBuffer, writeBuffer));
      });
    }
    for (int i = 0; i < NUM_ROUNDS; i++) {
      futures[i].get();
    }
  }
}
