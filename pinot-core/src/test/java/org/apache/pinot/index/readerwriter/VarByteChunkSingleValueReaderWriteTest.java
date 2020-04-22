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
package org.apache.pinot.index.readerwriter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.io.compression.ChunkCompressorFactory;
import org.apache.pinot.core.io.reader.impl.ChunkReaderContext;
import org.apache.pinot.core.io.reader.impl.v1.VarByteChunkSingleValueReader;
import org.apache.pinot.core.io.writer.impl.v1.VarByteChunkSingleValueWriter;
import org.apache.pinot.core.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.memory.PinotNativeOrderLBuffer;
import org.apache.pinot.core.segment.memory.PinotNonNativeOrderLBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link VarByteChunkSingleValueReader} and {@link VarByteChunkSingleValueWriter} classes.
 */
public class VarByteChunkSingleValueReaderWriteTest {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private static final int NUM_ENTRIES = 5003;
  private static final int NUM_DOCS_PER_CHUNK = 1009;
  private static final int MAX_STRING_LENGTH = 101;
  private static final String TEST_FILE = System.getProperty("java.io.tmpdir") + File.separator + "varByteSVRTest";

  @Test
  public void testWithCompression()
      throws Exception {
    test(ChunkCompressorFactory.CompressionType.SNAPPY);
  }

  @Test
  public void testWithoutCompression()
      throws Exception {
    test(ChunkCompressorFactory.CompressionType.PASS_THROUGH);
  }

  /**
   * This test writes {@link #NUM_ENTRIES} using {@link VarByteChunkSingleValueWriter}. It then reads
   * the strings & bytes using {@link VarByteChunkSingleValueReader}, and asserts that what was written is the same as
   * what was read in.
   *
   * Number of docs and docs per chunk are chosen to generate complete as well partial chunks.
   *
   * @param compressionType Compression type
   * @throws Exception
   */
  public void test(ChunkCompressorFactory.CompressionType compressionType)
      throws Exception {
    String[] expected = new String[NUM_ENTRIES];
    Random random = new Random();

    File outFile = new File(TEST_FILE);
    FileUtils.deleteQuietly(outFile);

    int maxStringLengthInBytes = 0;
    for (int i = 0; i < NUM_ENTRIES; i++) {
      expected[i] = RandomStringUtils.random(random.nextInt(MAX_STRING_LENGTH));
      maxStringLengthInBytes = Math.max(maxStringLengthInBytes, expected[i].getBytes(UTF_8).length);
    }

    VarByteChunkSingleValueWriter writer =
        new VarByteChunkSingleValueWriter(outFile, compressionType, NUM_ENTRIES, NUM_DOCS_PER_CHUNK,
            maxStringLengthInBytes);

    for (int i = 0; i < NUM_ENTRIES; i += 2) {
      writer.setString(i, expected[i]);
      writer.setBytes(i + 1, expected[i].getBytes(UTF_8));
    }

    writer.close();

    try (VarByteChunkSingleValueReader reader = new VarByteChunkSingleValueReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(outFile))) {
      ChunkReaderContext context = reader.createContext();

      for (int i = 0; i < NUM_ENTRIES; i += 2) {
        String actual = reader.getString(i, context);
        Assert.assertEquals(actual, expected[i]);
        Assert.assertEquals(actual.getBytes(UTF_8), expected[i].getBytes(UTF_8));
        Assert.assertEquals(reader.getBytes(i + 1), expected[i].getBytes(UTF_8));
      }
    }

    FileUtils.deleteQuietly(outFile);
  }

  /**
   * This test ensures that the reader can read in an data file from version 1.
   * @throws IOException
   */
  @Test
  public void testBackwardCompatibilityV1()
      throws Exception {
    String[] expected = new String[]{"abcde", "fgh", "ijklmn", "12345"};
    testBackwardCompatibilityHelper("data/varByteStrings.v1", expected, 1009);
  }

  /**
   * This test ensures that the reader can read in an data file from version 2.
   */
  @Test
  public void testBackwardCompatibilityV2()
      throws Exception {
    String[] data = {"abcdefghijk", "12456887", "pqrstuv", "500"};
    testBackwardCompatibilityHelper("data/varByteStringsCompressed.v2", data, 1000);
    testBackwardCompatibilityHelper("data/varByteStringsRaw.v2", data, 1000);
  }

  private void testBackwardCompatibilityHelper(String fileName, String[] data, int numDocs)
      throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    URL resource = classLoader.getResource(fileName);
    if (resource == null) {
      throw new RuntimeException("Input file not found: " + fileName);
    }
    File file = new File(resource.getFile());
    try (VarByteChunkSingleValueReader reader = new VarByteChunkSingleValueReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(file))) {
      ChunkReaderContext context = reader.createContext();
      for (int i = 0; i < numDocs; i++) {
        String actual = reader.getString(i, context);
        Assert.assertEquals(actual, data[i % data.length]);
      }
    }
  }

  @Test
  public void testVarCharWithDifferentSizes() throws Exception {
    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.SNAPPY, 10, 1000);
    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.PASS_THROUGH, 10, 1000);

    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.SNAPPY, 100, 1000);
    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.PASS_THROUGH, 100, 1000);

    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.SNAPPY, 1000, 1000);
    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.PASS_THROUGH, 1000, 1000);

    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.SNAPPY, 10000, 100);
    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.PASS_THROUGH, 10000, 100);

    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.SNAPPY, 100000, 10);
    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.PASS_THROUGH, 100000, 10);

    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.SNAPPY, 1000000, 10);
    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.PASS_THROUGH, 1000000, 10);

    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.SNAPPY, 2000000, 10);
    testLargeVarcharHelper(ChunkCompressorFactory.CompressionType.PASS_THROUGH, 2000000, 10);
  }

  private void testLargeVarcharHelper(ChunkCompressorFactory.CompressionType compressionType, int numChars, int numDocs)
      throws Exception {
    String[] expected = new String[numDocs];
    Random random = new Random();

    File outFile = new File(TEST_FILE);
    FileUtils.deleteQuietly(outFile);

    int maxStringLengthInBytes = 0;
    for (int i = 0; i < numDocs; i++) {
      expected[i] = RandomStringUtils.random(random.nextInt(numChars));
      maxStringLengthInBytes = Math.max(maxStringLengthInBytes, StringUtil.encodeUtf8(expected[i]).length);
    }

    int numDocsPerChunk = SingleValueVarByteRawIndexCreator.getNumDocsPerChunk(maxStringLengthInBytes);
    VarByteChunkSingleValueWriter writer =
        new VarByteChunkSingleValueWriter(outFile, compressionType, numDocs, numDocsPerChunk,
            maxStringLengthInBytes);

    for (int i = 0; i < numDocs; i += 2) {
      writer.setString(i, expected[i]);
      writer.setBytes(i + 1, StringUtil.encodeUtf8(expected[i]));
    }

    writer.close();
    System.out.println("num docs per chunk: " + numDocsPerChunk + " " + "maxsize: " + maxStringLengthInBytes + " " + "length: " + outFile.length());

    PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(outFile);
    try (VarByteChunkSingleValueReader reader = new VarByteChunkSingleValueReader(buffer)) {
      ChunkReaderContext context = reader.createContext();
      for (int i = 0; i < numDocs; i += 2) {
        String actual = reader.getString(i, context);
        Assert.assertEquals(actual, expected[i]);
        byte[] expectedBytes = StringUtil.encodeUtf8(expected[i]);
        Assert.assertEquals(StringUtil.encodeUtf8(actual), expectedBytes);
        Assert.assertEquals(reader.getBytes(i + 1, context), expectedBytes);
      }
    }

    // For large variable width column values (where total size of data
    // across all rows in the segment is > 2GB), LBuffer will be used for
    // reading the fwd index. However, to test this scenario the unit test
    // will take a long time to execute due to comparison
    // (75000 characters in each row and 10000 rows will hit this scenario).
    // So we specifically test for mapping the index file into a LBuffer
    // to exercise the LBuffer code
    if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
      buffer = PinotNativeOrderLBuffer.mapFile(outFile, true, 0, outFile.length());
    } else {
      buffer = PinotNonNativeOrderLBuffer.mapFile(outFile, true, 0, outFile.length());
    }

    try (VarByteChunkSingleValueReader reader = new VarByteChunkSingleValueReader(buffer)) {
      ChunkReaderContext context = reader.createContext();
      for (int i = 0; i < numDocs; i += 2) {
        String actual = reader.getString(i, context);
        Assert.assertEquals(actual, expected[i]);
        byte[] expectedBytes = StringUtil.encodeUtf8(expected[i]);
        Assert.assertEquals(StringUtil.encodeUtf8(actual), expectedBytes);
        Assert.assertEquals(reader.getBytes(i + 1, context), expectedBytes);
      }
    }

    FileUtils.deleteQuietly(outFile);
  }
}
