/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.memory;

import com.linkedin.pinot.common.segment.ReadMode;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PinotDataBufferTest {

  static Random random = new Random();
  static final int ONE_GB = 1024 * 1024 * 1024;

  static void loadVerifyLong(PinotDataBuffer buffer) {
    final int fieldSize = 8;
    int maxElementCount = (int) (buffer.size() / fieldSize);
    int elemCount = Math.min(100_000, maxElementCount);

    Map<Integer, Long> positionValues = new HashMap<>(elemCount);
    for (long i = 0; i < elemCount; i++) {
      int pos = random.nextInt(elemCount);
      long val = random.nextLong();
      positionValues.put(pos, val);
      buffer.putLong(pos * fieldSize, val);
    }
    for (Map.Entry<Integer, Long> entry : positionValues.entrySet()) {
      Assert.assertEquals(buffer.getLong(entry.getKey() * fieldSize), (long) entry.getValue());
    }
  }

  static void loadVerifyInt(PinotDataBuffer buffer) {
    final int fieldSize = 4;
    int maxElementCount = (int) (buffer.size() / fieldSize);
    int elemCount = Math.min(100_000, maxElementCount);

    Map<Integer, Integer> positionValues = new HashMap<>();
    for (long i = 0; i < elemCount; i++) {
      int pos = random.nextInt(elemCount);
      int val = random.nextInt();
      positionValues.put(pos, val);
      buffer.putInt(pos * fieldSize, val);
    }
    for (Map.Entry<Integer, Integer> entry : positionValues.entrySet()) {
      Assert.assertEquals(buffer.getInt(entry.getKey() * fieldSize), (int) entry.getValue(),
          "Failure at index: " + entry.getKey());
    }
  }

  private static void loadVerifyFloat(PinotDataBuffer buffer) {
    final int fieldSize = Float.SIZE / 8;
    int maxElementCount = (int) (buffer.size() / fieldSize);
    int elemCount = Math.min(100_000, maxElementCount);

    Map<Integer, Float> positionValues = new HashMap<>();
    for (long i = 0; i < elemCount; i++) {
      int pos = random.nextInt(elemCount);
      float val = random.nextFloat();
      positionValues.put(pos, val);
      buffer.putFloat(pos * fieldSize, val);
    }
    for (Map.Entry<Integer, Float> entry : positionValues.entrySet()) {
      Assert.assertEquals(buffer.getFloat(entry.getKey() * fieldSize), (float) entry.getValue(),
          "Failure at index: " + entry.getKey());
    }
  }

  private static void loadVerifyByte(PinotDataBuffer buffer) {
    final int fieldSize = Byte.SIZE / 8;
    int maxElementCount = (int) (buffer.size() / fieldSize);
    int elemCount = Math.min(100_000, maxElementCount);

    Map<Integer, Byte> positionValues = new HashMap<>();
    byte[] val = new byte[1];
    for (long i = 0; i < elemCount; i++) {
      int pos = random.nextInt(elemCount);
      random.nextBytes(val);
      positionValues.put(pos, val[0]);
      buffer.putByte(pos * fieldSize, val[0]);
    }
    for (Map.Entry<Integer, Byte> entry : positionValues.entrySet()) {
      Assert.assertEquals(buffer.getByte(entry.getKey() * fieldSize), (byte) entry.getValue(),
          "Failure at index: " + entry.getKey());
    }
  }

  private static void loadVerifyDouble(PinotDataBuffer buffer) {
    final int fieldSize = Double.SIZE / 8;
    int maxElementCount = (int) (buffer.size() / fieldSize);
    int elemCount = Math.min(100_000, maxElementCount);

    Map<Integer, Double> positionValues = new HashMap<>();
    for (long i = 0; i < elemCount; i++) {
      int pos = random.nextInt(elemCount);
      double val = random.nextDouble();
      positionValues.put(pos, val);
      buffer.putDouble(pos * fieldSize, val);
    }
    for (Map.Entry<Integer, Double> entry : positionValues.entrySet()) {
      Assert.assertEquals(buffer.getDouble(entry.getKey() * fieldSize), entry.getValue(),
          "Failure at index: " + entry.getKey());
    }

  }


  public static void loadVerifyAllTypes(PinotDataBuffer buffer) {
    testLoadByte(buffer);
    testLoadInt(buffer);
    testLoadLong(buffer);
    testLoadFloat(buffer);
    testLoadDouble(buffer);
  }


  public static void testLoadByte(PinotDataBuffer byteBuffer) {

    loadVerifyByte(byteBuffer);

    PinotDataBuffer viewBuffer = byteBuffer.view(5789, 20000);
    loadVerifyByte(viewBuffer);
  }


  public static void testLoadLong(PinotDataBuffer longBuffer) {
    loadVerifyLong(longBuffer);
  }


  public static void testLoadInt(PinotDataBuffer intBuffer) {
    loadVerifyInt(intBuffer);

    PinotDataBuffer viewBuffer = intBuffer.view(2L * ONE_GB, 3L * ONE_GB - 1024);
    loadVerifyInt(viewBuffer);
  }


  public static void testLoadFloat(PinotDataBuffer floatBuffer) {
    loadVerifyFloat(floatBuffer);
  }

  public static void testLoadDouble(PinotDataBuffer doubleBuffer) {
    loadVerifyDouble(doubleBuffer);

    PinotDataBuffer viewBuffer = doubleBuffer.view((long)ONE_GB - 1024, (long)ONE_GB + ONE_GB);
    loadVerifyDouble(viewBuffer);
  }

  @Test
  public void testFactory()
      throws IOException {
    PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(1024);
    // default behavior right now
    Assert.assertTrue(buffer instanceof PinotByteBuffer);
    buffer.close();

    File f = new File("temp");
    PinotDataBuffer fileBuffer = null;
    PinotDataBuffer heapBuffer = null;
    try {
      f.createNewFile();
      fileBuffer = PinotDataBuffer.fromFile(f, ReadMode.mmap, FileChannel.MapMode.READ_WRITE, "context");
      heapBuffer = PinotDataBuffer.fromFile(f, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "context");
      Assert.assertTrue(fileBuffer instanceof PinotByteBuffer);
      Assert.assertTrue(heapBuffer instanceof PinotByteBuffer);

    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(f);
      if (fileBuffer != null) {
        fileBuffer.close();
      }
      if (heapBuffer != null) {
        heapBuffer.close();
      }
    }
  }

  int[] generateNumbers(int count, int max) {
    int[] data = new int[count];
    if (max < 0) {
      max = Integer.MAX_VALUE;
    }

    for (int i = 0; i < count; i++) {
      data[i] = random.nextInt(max);
    }
    return data;
  }

  @Test(enabled =  false)
  public void testReadWriteFile()
      throws IOException {
    final int[] data = generateNumbers(10_000_000, -1);
    File file = new File(this.getClass().getName() + ".data");
    file.deleteOnExit();

    int size = data.length * 4;
    PinotDataBuffer buffer = PinotDataBuffer.fromFile(file, 0, size, ReadMode.mmap,
        FileChannel.MapMode.READ_WRITE, "testing");
    int pos = 0;
    for (int val : data) {
      buffer.putInt(pos * 4, val);
      ++pos;
    }
    buffer.close();

    PinotDataBuffer readBuf = PinotDataBuffer.fromFile(file, ReadMode.mmap, FileChannel.MapMode.READ_WRITE, "testing");
    pos = 0;
    for (int i = 0; i < data.length; ++i, ++pos) {
      Assert.assertEquals(data[i], readBuf.getInt(pos * 4));
    }
    readBuf.close();

    PinotDataBuffer heapReadBuf = PinotDataBuffer.fromFile(file, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "testing");
    pos = 0;
    for (int i = 0; i < data.length; ++i, ++pos) {
      Assert.assertEquals(data[i], heapReadBuf.getInt(pos * 4));
    }
    heapReadBuf.close();
  }

  @Test (enabled = false)
  public void testView() {
    PinotDataBuffer buffer = PinotDataBuffer.allocateDirect(0);
    PinotDataBuffer view = buffer.view(0, 0);
    Assert.assertEquals(0, view.size());
  }
}
