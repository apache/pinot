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
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public abstract class PinotDataBufferTest extends PinotDataBufferInstanceTestBase {

  public PinotDataBufferTest(PinotBufferFactory factory) {
    super(factory);
  }

  protected abstract boolean prioritizeByteBuffer();

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

  private void testOrderBuffer(ByteOrder byteOrder) throws Exception {
    try (PinotDataBuffer buffer = _factory.allocateDirect(BUFFER_SIZE, byteOrder)) {
      testPinotDataBuffer(buffer);
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
      try (PinotDataBuffer buffer = _factory.readFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, byteOrder)) {
        testPinotDataBuffer(buffer);
      }
      try (PinotDataBuffer buffer =
          _factory.mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, byteOrder)) {
        testPinotDataBuffer(buffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testPinotNativeOrderBuffer()
      throws Exception {
    testOrderBuffer(PinotDataBuffer.NATIVE_ORDER);
  }

  @Test
  public void testPinotNonNativeOrderBuffer()
      throws Exception {
    testOrderBuffer(PinotDataBuffer.NON_NATIVE_ORDER);
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

  public void testPinotBufferReadWriteFile(ByteOrder byteOrder)
      throws Exception {
    try (PinotDataBuffer writeBuffer = _factory.mapFile(TEMP_FILE, false, FILE_OFFSET, BUFFER_SIZE, byteOrder)) {
      putInts(writeBuffer);
      try (PinotDataBuffer readBuffer = _factory.readFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, byteOrder)) {
        getInts(readBuffer);
      }
      try (PinotDataBuffer readBuffer = _factory.mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE, byteOrder)) {
        getInts(readBuffer);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
  }

  @Test
  public void testPinotNativeOrderBufferReadWriteFile()
      throws Exception {
    testPinotBufferReadWriteFile(PinotDataBuffer.NATIVE_ORDER);
  }

  @Test
  public void testPinotNonNativeOrderBufferReadWriteFile()
      throws Exception {
    testPinotBufferReadWriteFile(PinotDataBuffer.NON_NATIVE_ORDER);
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
    ByteOrder nativeOrder = PinotDataBuffer.NATIVE_ORDER;
    ByteOrder nonNativeOrder = PinotDataBuffer.NON_NATIVE_ORDER;
    try (PinotDataBuffer writeBuffer = PinotByteBuffer
        .mapFile(TEMP_FILE, false, FILE_OFFSET, 3 * BUFFER_SIZE, nativeOrder)) {
      putLongs(writeBuffer, startOffset);
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, 3 * BUFFER_SIZE, nativeOrder)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .mapFile(TEMP_FILE, true, FILE_OFFSET, 3 * BUFFER_SIZE, nativeOrder)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, 3 * BUFFER_SIZE, nativeOrder)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (
          PinotDataBuffer readBuffer = PinotByteBuffer
              .mapFile(TEMP_FILE, true, FILE_OFFSET, 3 * BUFFER_SIZE, nativeOrder)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
    } finally {
      FileUtils.forceDelete(TEMP_FILE);
    }
    try (PinotDataBuffer writeBuffer = PinotByteBuffer
        .mapFile(TEMP_FILE, false, FILE_OFFSET, 3 * BUFFER_SIZE, nonNativeOrder)) {
      putLongs(writeBuffer, startOffset);
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .loadFile(TEMP_FILE, FILE_OFFSET, 3 * BUFFER_SIZE, nonNativeOrder)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = PinotByteBuffer
          .mapFile(TEMP_FILE, true, FILE_OFFSET, 3 * BUFFER_SIZE, nonNativeOrder)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = _factory.readFile(TEMP_FILE, FILE_OFFSET, 3 * BUFFER_SIZE, nonNativeOrder)) {
        testViewAndToDirectByteBuffer(readBuffer, startOffset);
      }
      try (PinotDataBuffer readBuffer = _factory
          .mapFile(TEMP_FILE, true, FILE_OFFSET, 3 * BUFFER_SIZE, nonNativeOrder)) {
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

  @Test
  public void testConstructors()
      throws Exception {
    testBufferStats(0, 0, 0, 0);
    if (prioritizeByteBuffer()) {
      try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
        randomAccessFile.setLength(FILE_OFFSET + BUFFER_SIZE);
        try (PinotDataBuffer buffer1 =
            PinotDataBuffer.allocateDirect(BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER, null)) {
          Assert.assertTrue(buffer1 instanceof PinotByteBuffer);
          testBufferStats(1, BUFFER_SIZE, 0, 0);
          try (PinotDataBuffer buffer2 =
              PinotDataBuffer.loadFile(TEMP_FILE, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN, null)) {
            Assert.assertTrue(buffer2 instanceof PinotByteBuffer);
            testBufferStats(2, 2 * BUFFER_SIZE, 0, 0);
            try (PinotDataBuffer buffer3 =
                PinotDataBuffer.mapFile(TEMP_FILE, true, FILE_OFFSET, BUFFER_SIZE, ByteOrder.BIG_ENDIAN, null)) {
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
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(TEMP_FILE, "rw")) {
      randomAccessFile.setLength(FILE_OFFSET + LARGE_BUFFER_SIZE);
      try (PinotDataBuffer buffer1 = PinotDataBuffer
          .allocateDirect(LARGE_BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER, null)) {
        Assert.assertSame(buffer1.order(), PinotDataBuffer.NATIVE_ORDER);
        testBufferStats(1, LARGE_BUFFER_SIZE, 0, 0);
        try (PinotDataBuffer buffer2 = PinotDataBuffer
            .loadFile(TEMP_FILE, FILE_OFFSET, LARGE_BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER, null)) {
          Assert.assertSame(buffer2.order(), PinotDataBuffer.NATIVE_ORDER);
          testBufferStats(2, 2 * LARGE_BUFFER_SIZE, 0, 0);
          try (PinotDataBuffer buffer3 = PinotDataBuffer
              .mapFile(TEMP_FILE, true, FILE_OFFSET, LARGE_BUFFER_SIZE, PinotDataBuffer.NATIVE_ORDER, null)) {
            Assert.assertSame(buffer3.order(), PinotDataBuffer.NATIVE_ORDER);
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
        Assert.assertSame(buffer1.order(), PinotDataBuffer.NON_NATIVE_ORDER);
        testBufferStats(1, LARGE_BUFFER_SIZE, 0, 0);
        try (PinotDataBuffer buffer2 = PinotDataBuffer
            .loadFile(TEMP_FILE, FILE_OFFSET, LARGE_BUFFER_SIZE, PinotDataBuffer.NON_NATIVE_ORDER, null)) {
          Assert.assertSame(buffer2.order(), PinotDataBuffer.NON_NATIVE_ORDER);
          testBufferStats(2, 2 * LARGE_BUFFER_SIZE, 0, 0);
          try (PinotDataBuffer buffer3 = PinotDataBuffer
              .mapFile(TEMP_FILE, true, FILE_OFFSET, LARGE_BUFFER_SIZE, PinotDataBuffer.NON_NATIVE_ORDER, null)) {
            Assert.assertSame(buffer3.order(), PinotDataBuffer.NON_NATIVE_ORDER);
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
}
