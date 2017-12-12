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
package com.linkedin.pinot.index.readerwriter;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.io.reader.impl.ChunkReaderContext;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedByteChunkSingleValueReader;
import com.linkedin.pinot.core.io.writer.impl.v1.FixedByteChunkSingleValueWriter;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link FixedByteChunkSingleValueReader} and {@link FixedByteChunkSingleValueWriter} classes.
 *
 * This test writes {@link #NUM_VALUES} using {@link FixedByteChunkSingleValueWriter}. It then reads
 * the values using {@link FixedByteChunkSingleValueReader}, and asserts that what was written is the same as
 * what was read in.
 *
 * Number of docs and docs per chunk are chosen to generate complete as well partial chunks.
 *
 */
public class FixedByteChunkSingleValueReaderWriteTest {

  private static final int NUM_VALUES = 10009;
  private static final int NUM_DOCS_PER_CHUNK = 5003;
  private static final String TEST_FILE = System.getProperty("java.io.tmpdir") + File.separator + "FixedByteSVRTest";
  private static final Random _random = new Random();

  @Test
  public void testWithCompression()
      throws Exception {
    ChunkCompressorFactory.CompressionType compressionType = ChunkCompressorFactory.CompressionType.SNAPPY;
    testInt(compressionType);
    testLong(compressionType);
    testFloat(compressionType);
    testDouble(compressionType);
  }

  @Test
  public void testWithoutCompression()
      throws Exception {
    ChunkCompressorFactory.CompressionType compressionType = ChunkCompressorFactory.CompressionType.PASS_THROUGH;
    testInt(compressionType);
    testLong(compressionType);
    testFloat(compressionType);
    testDouble(compressionType);
  }

  public void testInt(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    int[] expected = new int[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = _random.nextInt();
    }

    File outFile = new File(TEST_FILE);
    FileUtils.deleteQuietly(outFile);

    FixedByteChunkSingleValueWriter writer =
        new FixedByteChunkSingleValueWriter(outFile, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            V1Constants.Numbers.INTEGER_SIZE);

    for (int i = 0; i < NUM_VALUES; i++) {
      writer.setInt(i, expected[i]);
    }
    writer.close();

    PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.fromFile(outFile, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, getClass().getName());

    FixedByteChunkSingleValueReader reader = new FixedByteChunkSingleValueReader(pinotDataBuffer);
    ChunkReaderContext context = reader.createContext();

    for (int i = 0; i < NUM_VALUES; i++) {
      int actual = reader.getInt(i, context);
      Assert.assertEquals(actual, expected[i]);

      if (compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH)) {
        actual = reader.getInt(i);
        Assert.assertEquals(actual, expected[i]);
      }
    }
    reader.close();
    FileUtils.deleteQuietly(outFile);
  }

  public void testLong(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    long[] expected = new long[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = _random.nextLong();
    }

    File outFile = new File(TEST_FILE);
    FileUtils.deleteQuietly(outFile);

    FixedByteChunkSingleValueWriter writer =
        new FixedByteChunkSingleValueWriter(outFile, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            V1Constants.Numbers.LONG_SIZE);

    for (int i = 0; i < NUM_VALUES; i++) {
      writer.setLong(i, expected[i]);
    }
    writer.close();

    PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.fromFile(outFile, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, getClass().getName());

    FixedByteChunkSingleValueReader reader = new FixedByteChunkSingleValueReader(pinotDataBuffer);
    ChunkReaderContext context = reader.createContext();

    for (int i = 0; i < NUM_VALUES; i++) {
      long actual = reader.getLong(i, context);
      Assert.assertEquals(actual, expected[i]);

      if (compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH)) {
        actual = reader.getLong(i);
        Assert.assertEquals(actual, expected[i]);
      }
    }
    reader.close();
    FileUtils.deleteQuietly(outFile);
  }

  public void testFloat(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    float[] expected = new float[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = _random.nextFloat();
    }

    File outFile = new File(TEST_FILE);
    FileUtils.deleteQuietly(outFile);

    FixedByteChunkSingleValueWriter writer =
        new FixedByteChunkSingleValueWriter(outFile, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            V1Constants.Numbers.FLOAT_SIZE);

    for (int i = 0; i < NUM_VALUES; i++) {
      writer.setFloat(i, expected[i]);
    }
    writer.close();

    PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.fromFile(outFile, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, getClass().getName());

    FixedByteChunkSingleValueReader reader = new FixedByteChunkSingleValueReader(pinotDataBuffer);
    ChunkReaderContext context = reader.createContext();

    for (int i = 0; i < NUM_VALUES; i++) {
      float actual = reader.getFloat(i, context);
      Assert.assertEquals(actual, expected[i]);

      if (compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH)) {
        actual = reader.getFloat(i);
        Assert.assertEquals(actual, expected[i]);
      }
    }
    reader.close();
    FileUtils.deleteQuietly(outFile);
  }

  public void testDouble(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    double[] expected = new double[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      expected[i] = _random.nextDouble();
    }

    File outFile = new File(TEST_FILE);
    FileUtils.deleteQuietly(outFile);

    FixedByteChunkSingleValueWriter writer =
        new FixedByteChunkSingleValueWriter(outFile, compressionType, NUM_VALUES, NUM_DOCS_PER_CHUNK,
            V1Constants.Numbers.DOUBLE_SIZE);

    for (int i = 0; i < NUM_VALUES; i++) {
      writer.setDouble(i, expected[i]);
    }
    writer.close();

    PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.fromFile(outFile, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, getClass().getName());

    FixedByteChunkSingleValueReader reader = new FixedByteChunkSingleValueReader(pinotDataBuffer);
    ChunkReaderContext context = reader.createContext();

    for (int i = 0; i < NUM_VALUES; i++) {
      double actual = reader.getDouble(i, context);
      Assert.assertEquals(actual, expected[i]);

      if (compressionType.equals(ChunkCompressorFactory.CompressionType.PASS_THROUGH)) {
        actual = reader.getDouble(i);
        Assert.assertEquals(actual, expected[i]);
      }
    }
    reader.close();
    FileUtils.deleteQuietly(outFile);
  }

  /**
   * This test ensures that the reader can read in an data file from version 1.
   * @throws IOException
   */
  @Test
  public void testBackwardCompatibility()
      throws IOException {
    // Get v1 from resources folder
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName = "data/fixedByteSVRDoubles.v1";
    URL resource = classLoader.getResource(fileName);
    if (resource == null) {
      throw new RuntimeException("Input file not found: " + fileName);
    }

    File file = new File(resource.getFile());

    PinotDataBuffer pinotDataBuffer =
        PinotDataBuffer.fromFile(file, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, getClass().getName());

    FixedByteChunkSingleValueReader reader = new FixedByteChunkSingleValueReader(pinotDataBuffer);
    ChunkReaderContext context = reader.createContext();

    int numEntries = 10009; // Number of entries in the input file.
    for (int i = 0; i < numEntries; i++) {
      double actual = reader.getDouble(i, context);
      Assert.assertEquals(actual, (double) i);
    }
    reader.close();
  }
}
