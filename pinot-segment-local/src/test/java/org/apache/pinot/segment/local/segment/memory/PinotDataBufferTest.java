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
package org.apache.pinot.segment.local.segment.memory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotDataBufferTest {
  private static final Random RANDOM = new Random();
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(10);
  private static final File TEMP_FILE = new File(FileUtils.getTempDirectory(), "PinotDataBufferTest");
  private static final int FILE_OFFSET = 10;      // Not page-aligned
  private static final int BUFFER_SIZE = 10_000;  // Not page-aligned
  private static final int CHAR_ARRAY_LENGTH = BUFFER_SIZE / Character.BYTES;
  private static final int SHORT_ARRAY_LENGTH = BUFFER_SIZE / Short.BYTES;
  private static final int INT_ARRAY_LENGTH = BUFFER_SIZE / Integer.BYTES;
  private static final int LONG_ARRAY_LENGTH = BUFFER_SIZE / Long.BYTES;
  private static final int FLOAT_ARRAY_LENGTH = BUFFER_SIZE / Float.BYTES;
  private static final int DOUBLE_ARRAY_LENGTH = BUFFER_SIZE / Double.BYTES;
  private static final int NUM_ROUNDS = 1000;
  private static final int MAX_BYTES_LENGTH = 100;
  private static final long LARGE_BUFFER_SIZE = Integer.MAX_VALUE + 2L; // Not page-aligned

  private byte[] _bytes = new byte[BUFFER_SIZE];
  private char[] _chars = new char[CHAR_ARRAY_LENGTH];
  private short[] _shorts = new short[SHORT_ARRAY_LENGTH];
  private int[] _ints = new int[INT_ARRAY_LENGTH];
  private long[] _longs = new long[LONG_ARRAY_LENGTH];
  private float[] _floats = new float[FLOAT_ARRAY_LENGTH];
  private double[] _doubles = new double[DOUBLE_ARRAY_LENGTH];

  @BeforeClass
  public void setUp() {
    for (int i = 0; i < BUFFER_SIZE; i++) {
      _bytes[i] = (byte) RANDOM.nextInt();
    }
    for (int i = 0; i < CHAR_ARRAY_LENGTH; i++) {
      _chars[i] = (char) RANDOM.nextInt();
    }
    for (int i = 0; i < SHORT_ARRAY_LENGTH; i++) {
      _shorts[i] = (short) RANDOM.nextInt();
    }
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      _ints[i] = RANDOM.nextInt();
    }
    for (int i = 0; i < LONG_ARRAY_LENGTH; i++) {
      _longs[i] = RANDOM.nextLong();
    }
    for (int i = 0; i < FLOAT_ARRAY_LENGTH; i++) {
      _floats[i] = RANDOM.nextFloat();
    }
    for (int i = 0; i < DOUBLE_ARRAY_LENGTH; i++) {
      _doubles[i] = RANDOM.nextDouble();
    }
  }

  @Test
  public void testPinotByteBuffer()
      throws Exception {
    try (PinotDataBuffer buffer = PinotByteBuffer.allocateDirect(BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
      testPinotDataBuffer(buffer);
    }
    try (PinotDataBuffer buffer = PinotByteBuffer.allocateDirect(BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
      testPinotDataBuffer(buffer);
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = PinotByteBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
        testPinotDataBuffer(buffer);
      }
      try (PinotDataBuffer buffer = PinotByteBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
        testPinotDataBuffer(buffer);
      }
      try (PinotDataBuffer buffer = PinotByteBuffer
          .mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN)) {
        testPinotDataBuffer(buffer);
      }
      try (PinotDataBuffer buffer = PinotByteBuffer
          .mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, ByteOrder.LITTLE_ENDIAN)) {
        testPinotDataBuffer(buffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testPinotNativeOrderLBuffer()
      throws Exception {
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.allocateDirect(BUFFER_SIZE)) {
      testPinotDataBuffer(buffer);
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE)) {
        testPinotDataBuffer(buffer);
      }
      try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE)) {
        testPinotDataBuffer(buffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testPinotNonNativeOrderLBuffer()
      throws Exception {
    try (PinotDataBuffer buffer = PinotNonNativeOrderLBuffer.allocateDirect(BUFFER_SIZE)) {
      testPinotDataBuffer(buffer);
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = PinotNonNativeOrderLBuffer.loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE)) {
        testPinotDataBuffer(buffer);
      }
      try (PinotDataBuffer buffer = PinotNonNativeOrderLBuffer.mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE)) {
        testPinotDataBuffer(buffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  private void testPinotDataBuffer(PinotDataBuffer buffer)
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

  private void testReadWriteByte(PinotDataBuffer buffer) {
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

  private void testReadWriteChar(PinotDataBuffer buffer) {
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

  private void testReadWriteShort(PinotDataBuffer buffer) {
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

  private void testReadWriteInt(PinotDataBuffer buffer) {
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

  private void testReadWriteLong(PinotDataBuffer buffer) {
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

  private void testReadWriteFloat(PinotDataBuffer buffer) {
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

  private void testReadWriteDouble(PinotDataBuffer buffer) {
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

  private void testReadWriteBytes(PinotDataBuffer buffer) {
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

  private void testReadWritePinotDataBuffer(PinotDataBuffer buffer) {
    testReadWritePinotDataBuffer(buffer, PinotByteBuffer.allocateDirect(MAX_BYTES_LENGTH, PinotDataBuffer.NATIVE_ORDER),
        PinotByteBuffer.allocateDirect(MAX_BYTES_LENGTH, PinotDataBuffer.NON_NATIVE_ORDER));
    testReadWritePinotDataBuffer(buffer,
        PinotByteBuffer.allocateDirect(2 * MAX_BYTES_LENGTH, PinotDataBuffer.NON_NATIVE_ORDER)
            .view(MAX_BYTES_LENGTH, 2 * MAX_BYTES_LENGTH),
        PinotByteBuffer.allocateDirect(2 * MAX_BYTES_LENGTH, PinotDataBuffer.NATIVE_ORDER)
            .view(MAX_BYTES_LENGTH, 2 * MAX_BYTES_LENGTH));
    testReadWritePinotDataBuffer(buffer, PinotNativeOrderLBuffer.allocateDirect(MAX_BYTES_LENGTH),
        PinotNonNativeOrderLBuffer.allocateDirect(MAX_BYTES_LENGTH));
    testReadWritePinotDataBuffer(buffer,
        PinotNonNativeOrderLBuffer.allocateDirect(2 * MAX_BYTES_LENGTH).view(MAX_BYTES_LENGTH, 2 * MAX_BYTES_LENGTH),
        PinotNativeOrderLBuffer.allocateDirect(2 * MAX_BYTES_LENGTH).view(MAX_BYTES_LENGTH, 2 * MAX_BYTES_LENGTH));
  }

  private void testReadWritePinotDataBuffer(PinotDataBuffer buffer, PinotDataBuffer readBuffer,
      PinotDataBuffer writeBuffer) {
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int length = RANDOM.nextInt(MAX_BYTES_LENGTH);
      int offset = RANDOM.nextInt(BUFFER_SIZE - length);
      readBuffer.readFrom(0, _bytes, RANDOM.nextInt(BUFFER_SIZE - length), length);
      readBuffer.copyTo(0, buffer, offset, length);
      buffer.copyTo(offset, writeBuffer, 0, length);
      for (int j = 0; j < length; j++) {
        Assert.assertEquals(writeBuffer.getByte(j), readBuffer.getByte(j));
      }
    }
  }

  private void testReadFromByteBuffer(PinotDataBuffer buffer) {
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

  private void testConcurrentReadWrite(PinotDataBuffer buffer)
      throws Exception {
    Future[] futures = new Future[NUM_ROUNDS];
    for (int i = 0; i < NUM_ROUNDS; i++) {
      futures[i] = EXECUTOR_SERVICE.submit(() -> {
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

  @Test
  public void testPinotByteBufferReadWriteFile()
      throws Exception {
    try (PinotDataBuffer writeBuffer = PinotByteBuffer
        .mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
      putInts(writeBuffer);
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
        getInts(readBuffer);
      }
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
        getInts(readBuffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testPinotNativeOrderLBufferReadWriteFile()
      throws Exception {
    try (PinotDataBuffer writeBuffer = PinotNativeOrderLBuffer.mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE)) {
      putInts(writeBuffer);
      try (PinotDataBuffer readBuffer = PinotNativeOrderLBuffer.loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE)) {
        getInts(readBuffer);
      }
      try (PinotDataBuffer readBuffer = PinotNativeOrderLBuffer.mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE)) {
        getInts(readBuffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testPinotNonNativeOrderLBufferReadWriteFile()
      throws Exception {
    try (PinotDataBuffer writeBuffer = PinotNonNativeOrderLBuffer.mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE)) {
      putInts(writeBuffer);
      try (PinotDataBuffer readBuffer = PinotNonNativeOrderLBuffer.loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE)) {
        getInts(readBuffer);
      }
      try (PinotDataBuffer readBuffer = PinotNonNativeOrderLBuffer.mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE)) {
        getInts(readBuffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  private void putInts(PinotDataBuffer buffer) {
    Assert.assertEquals(buffer.size(), BUFFER_SIZE);
    for (int i = 0; i < INT_ARRAY_LENGTH; i++) {
      buffer.putInt(i * Integer.BYTES, _ints[i]);
    }
    buffer.flush();
  }

  private void getInts(PinotDataBuffer buffer) {
    Assert.assertEquals(buffer.size(), BUFFER_SIZE);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(INT_ARRAY_LENGTH);
      Assert.assertEquals(buffer.getInt(index * Integer.BYTES), _ints[index]);
    }
  }

  @Test
  public void testViewAndToDirectByteBuffer()
      throws Exception {
    int startOffset = RANDOM.nextInt(BUFFER_SIZE);
    try (PinotDataBuffer writeBuffer = PinotByteBuffer
        .mapFile(TEMP_FILE, false, FILE_OFFSET, 3 * BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
      putLongs(writeBuffer, startOffset);
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, 3 * BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .mapFile(TEMP_FILE, true, FILE_OFFSET, 3 * BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = PinotNativeOrderLBuffer.loadFile(TEMP_FILE, FILE_OFFSET, 3 * BUFFER_SIZE)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (
          PinotDataBuffer readBuffer = PinotNativeOrderLBuffer.mapFile(TEMP_FILE, true, FILE_OFFSET, 3 * BUFFER_SIZE)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
    try (PinotDataBuffer writeBuffer = PinotByteBuffer
        .mapFile(TEMP_FILE, false, FILE_OFFSET, 3 * BUFFER_SIZE, PinotDataBuffer.NON_NATIVE_ORDER)) {
      putLongs(writeBuffer, startOffset);
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, 3 * BUFFER_SIZE, PinotDataBuffer.NON_NATIVE_ORDER)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .mapFile(TEMP_FILE, true, FILE_OFFSET, 3 * BUFFER_SIZE, PinotDataBuffer.NON_NATIVE_ORDER)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = PinotNonNativeOrderLBuffer.loadFile(TEMP_FILE, FILE_OFFSET, 3 * BUFFER_SIZE)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = PinotNonNativeOrderLBuffer
          .mapFile(TEMP_FILE, true, FILE_OFFSET, 3 * BUFFER_SIZE)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  private void putLongs(PinotDataBuffer buffer, int startOffset) {
    Assert.assertEquals(buffer.size(), 3 * BUFFER_SIZE);
    for (int i = 0; i < LONG_ARRAY_LENGTH; i++) {
      buffer.putLong(startOffset + i * Long.BYTES, _longs[i]);
      buffer.putLong(startOffset + BUFFER_SIZE + i * Long.BYTES, _longs[i]);
    }
    buffer.flush();
  }

  private void testViewAndToDirectByteBuffer(PinotDataBuffer buffer, int startOffset)
      throws Exception {
    Assert.assertEquals(buffer.size(), 3 * BUFFER_SIZE);
    try (PinotDataBuffer view = buffer.view(startOffset, startOffset + 2 * BUFFER_SIZE)) {
      Assert.assertEquals(view.size(), 2 * BUFFER_SIZE);
      for (int i = 0; i < NUM_ROUNDS; i++) {
        int index = RANDOM.nextInt(LONG_ARRAY_LENGTH);
        Assert.assertEquals(view.getLong(index * Long.BYTES), _longs[index]);
        Assert.assertEquals(view.getLong(BUFFER_SIZE + index * Long.BYTES), _longs[index]);
      }
      testByteBuffer(view.toDirectByteBuffer(BUFFER_SIZE, BUFFER_SIZE));

      try (PinotDataBuffer viewOfView = view.view(BUFFER_SIZE, 2 * BUFFER_SIZE)) {
        Assert.assertEquals(viewOfView.size(), BUFFER_SIZE);
        for (int i = 0; i < NUM_ROUNDS; i++) {
          int index = RANDOM.nextInt(LONG_ARRAY_LENGTH);
          Assert.assertEquals(viewOfView.getLong(index * Long.BYTES), _longs[index]);
        }
        testByteBuffer(view.toDirectByteBuffer(0, BUFFER_SIZE));
      }
    }
    testByteBuffer(buffer.toDirectByteBuffer(startOffset, BUFFER_SIZE));
  }

  private void testByteBuffer(ByteBuffer byteBuffer) {
    Assert.assertEquals(byteBuffer.capacity(), BUFFER_SIZE);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int index = RANDOM.nextInt(LONG_ARRAY_LENGTH);
      Assert.assertEquals(byteBuffer.getLong(index * Long.BYTES), _longs[index]);
    }
  }

  @SuppressWarnings("RedundantExplicitClose")
  @Test
  public void testMultipleClose()
      throws Exception {
    try (PinotDataBuffer buffer = PinotByteBuffer.allocateDirect(BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
      buffer.close();
    }
    try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.allocateDirect(BUFFER_SIZE)) {
      buffer.close();
    }
    try (PinotDataBuffer buffer = PinotNonNativeOrderLBuffer.allocateDirect(BUFFER_SIZE)) {
      buffer.close();
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = PinotByteBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
        buffer.close();
      }
      try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE)) {
        buffer.close();
      }
      try (PinotDataBuffer buffer = PinotNonNativeOrderLBuffer.loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE)) {
        buffer.close();
      }
      try (PinotDataBuffer buffer = PinotByteBuffer
          .mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER)) {
        buffer.close();
      }
      try (PinotDataBuffer buffer = PinotNativeOrderLBuffer.mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE)) {
        buffer.close();
      }
      try (PinotDataBuffer buffer = PinotNonNativeOrderLBuffer.mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE)) {
        buffer.close();
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testConstructors()
      throws Exception {
    testBufferStats(0, 0, 0, 0);
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer1 = PinotDataBuffer.allocateDirect(BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER, null)) {
        Assert.assertTrue(buffer1 instanceof PinotByteBuffer);
        testBufferStats(1, BUFFER_SIZE, 0, 0);
        try (PinotDataBuffer buffer2 = PinotDataBuffer
            .loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN, null)) {
          Assert.assertTrue(buffer2 instanceof PinotByteBuffer);
          testBufferStats(2, 2 * BUFFER_SIZE, 0, 0);
          try (PinotDataBuffer buffer3 = PinotDataBuffer
              .mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN, null)) {
            Assert.assertTrue(buffer3 instanceof PinotByteBuffer);
            testBufferStats(2, 2 * BUFFER_SIZE, 1, BUFFER_SIZE);
          }
          testBufferStats(2, 2 * BUFFER_SIZE, 0, 0);
        }
        testBufferStats(1, BUFFER_SIZE, 0, 0);
      }
      testBufferStats(0, 0, 0, 0);
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + LARGE_BUFFER_SIZE);
      try (PinotDataBuffer buffer1 = PinotDataBuffer
          .allocateDirect(LARGE_BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER, null)) {
        Assert.assertTrue(buffer1 instanceof PinotNativeOrderLBuffer);
        testBufferStats(1, LARGE_BUFFER_SIZE, 0, 0);
        try (PinotDataBuffer buffer2 = PinotDataBuffer
            .loadFile(TEMP_FILE, FILE_OFFSET, LARGE_BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER, null)) {
          Assert.assertTrue(buffer2 instanceof PinotNativeOrderLBuffer);
          testBufferStats(2, 2 * LARGE_BUFFER_SIZE, 0, 0);
          try (PinotDataBuffer buffer3 = PinotDataBuffer
              .mapFile(TEMP_FILE, true, FILE_OFFSET, LARGE_BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER, null)) {
            Assert.assertTrue(buffer3 instanceof PinotNativeOrderLBuffer);
            testBufferStats(2, 2 * LARGE_BUFFER_SIZE, 1, LARGE_BUFFER_SIZE);
          }
          testBufferStats(2, 2 * LARGE_BUFFER_SIZE, 0, 0);
        }
        testBufferStats(1, LARGE_BUFFER_SIZE, 0, 0);
      }
      testBufferStats(0, 0, 0, 0);
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + LARGE_BUFFER_SIZE);
      try (PinotDataBuffer buffer1 = PinotDataBuffer
          .allocateDirect(LARGE_BUFFER_SIZE, PinotDataBuffer.NON_NATIVE_ORDER, null)) {
        Assert.assertTrue(buffer1 instanceof PinotNonNativeOrderLBuffer);
        testBufferStats(1, LARGE_BUFFER_SIZE, 0, 0);
        try (PinotDataBuffer buffer2 = PinotDataBuffer
            .loadFile(TEMP_FILE, FILE_OFFSET, LARGE_BUFFER_SIZE, PinotDataBuffer.NON_NATIVE_ORDER, null)) {
          Assert.assertTrue(buffer2 instanceof PinotNonNativeOrderLBuffer);
          testBufferStats(2, 2 * LARGE_BUFFER_SIZE, 0, 0);
          try (PinotDataBuffer buffer3 = PinotDataBuffer
              .mapFile(TEMP_FILE, true, FILE_OFFSET, LARGE_BUFFER_SIZE, PinotDataBuffer.NON_NATIVE_ORDER, null)) {
            Assert.assertTrue(buffer3 instanceof PinotNonNativeOrderLBuffer);
            testBufferStats(2, 2 * LARGE_BUFFER_SIZE, 1, LARGE_BUFFER_SIZE);
          }
          testBufferStats(2, 2 * LARGE_BUFFER_SIZE, 0, 0);
        }
        testBufferStats(1, LARGE_BUFFER_SIZE, 0, 0);
      }
      testBufferStats(0, 0, 0, 0);
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  private void testBufferStats(int directBufferCount, long directBufferUsage, int mmapBufferCount,
      long mmapBufferUsage) {
    Assert.assertEquals(PinotDataBuffer.getAllocationFailureCount(), 0);
    Assert.assertEquals(PinotDataBuffer.getDirectBufferCount(), directBufferCount);
    Assert.assertEquals(PinotDataBuffer.getDirectBufferUsage(), directBufferUsage);
    Assert.assertEquals(PinotDataBuffer.getMmapBufferCount(), mmapBufferCount);
    Assert.assertEquals(PinotDataBuffer.getMmapBufferUsage(), mmapBufferUsage);
    Assert.assertEquals(PinotDataBuffer.getBufferInfo().size(), directBufferCount + mmapBufferCount);
  }

  @AfterClass
  public void tearDown() {
    EXECUTOR_SERVICE.shutdown();
  }
}
