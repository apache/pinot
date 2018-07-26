/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.io.reader.impl.ChunkReaderContext;
import com.linkedin.pinot.core.io.reader.impl.v1.VarByteChunkSingleValueReader;
import com.linkedin.pinot.core.io.writer.impl.v1.VarByteChunkSingleValueWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
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
  public void testWithCompression() throws Exception {
    test(ChunkCompressorFactory.CompressionType.SNAPPY);
  }

  @Test
  public void testWithoutCompression() throws Exception {
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
  public void testBackwardCompatibility()
      throws IOException {
    String[] expected = new String[] {"abcde", "fgh", "ijklmn", "12345"};

    // Get v1 from resources folder
    ClassLoader classLoader = getClass().getClassLoader();
    String fileName = "data/varByteStrings.v1";
    URL resource = classLoader.getResource(fileName);
    if (resource == null) {
      throw new RuntimeException("Input file not found: " + fileName);
    }

    File file = new File(resource.getFile());
    try (VarByteChunkSingleValueReader reader = new VarByteChunkSingleValueReader(
        PinotDataBuffer.mapReadOnlyBigEndianFile(file))) {
      ChunkReaderContext context = reader.createContext();

      int numEntries = 1009; // Number of entries in the input file.
      for (int i = 0; i < numEntries; i++) {
        String actual = reader.getString(i, context);
        Assert.assertEquals(actual, expected[i % expected.length]);
      }
    }
  }
}
