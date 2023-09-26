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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.segment.index.readers.forward.BaseVarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class VarByteChunkV4Test {

  private static final File TEST_DIR = new File(FileUtils.getTempDirectory(), "VarByteChunkV4Test");

  private File _file;

  @DataProvider
  public Object[][] params() {
    return new Object[][]{
        {ChunkCompressionType.LZ4, 20, 1024},
        {ChunkCompressionType.LZ4_LENGTH_PREFIXED, 20, 1024},
        {ChunkCompressionType.PASS_THROUGH, 20, 1024},
        {ChunkCompressionType.SNAPPY, 20, 1024},
        {ChunkCompressionType.ZSTANDARD, 20, 1024},
        {ChunkCompressionType.LZ4, 2048, 1024},
        {ChunkCompressionType.LZ4_LENGTH_PREFIXED, 2048, 1024},
        {ChunkCompressionType.PASS_THROUGH, 2048, 1024},
        {ChunkCompressionType.SNAPPY, 2048, 1024},
        {ChunkCompressionType.ZSTANDARD, 2048, 1024}
    };
  }

  @BeforeClass
  public void forceMkDir()
      throws IOException {
    FileUtils.forceMkdir(TEST_DIR);
  }

  @AfterClass
  public void deleteDir() {
    FileUtils.deleteQuietly(TEST_DIR);
  }

  @AfterMethod
  public void after() {
    if (_file != null) {
      FileUtils.deleteQuietly(_file);
    }
  }

  @Test(dataProvider = "params")
  public void testStringSV(ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    _file = new File(TEST_DIR, "testStringSV");
    testSV(compressionType, longestEntry, chunkSize, FieldSpec.DataType.STRING, x -> x,
        VarByteChunkForwardIndexWriterV4::putString, (reader, context, docId) -> reader.getString(docId, context));
  }

  @Test(dataProvider = "params")
  public void testBytesSV(ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    _file = new File(TEST_DIR, "testBytesSV");
    testSV(compressionType, longestEntry, chunkSize, FieldSpec.DataType.BYTES, x -> x.getBytes(StandardCharsets.UTF_8),
        VarByteChunkForwardIndexWriterV4::putBytes, (reader, context, docId) -> reader.getBytes(docId, context));
  }

  private <T> void testSV(ChunkCompressionType compressionType, int longestEntry, int chunkSize,
      FieldSpec.DataType dataType, Function<String, T> forwardMapper,
      BiConsumer<VarByteChunkForwardIndexWriterV4, T> write,
      Read<T> read)
      throws IOException {
    List<T> values = randomStrings(1000, longestEntry).map(forwardMapper).collect(Collectors.toList());
    try (VarByteChunkForwardIndexWriterV4 writer = new VarByteChunkForwardIndexWriterV4(_file, compressionType,
        chunkSize)) {
      for (T value : values) {
        write.accept(writer, value);
      }
    }
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(_file)) {
      try (BaseVarByteChunkForwardIndexReaderV4 reader = new BaseVarByteChunkForwardIndexReaderV4(buffer, dataType,
          true); BaseVarByteChunkForwardIndexReaderV4.ReaderContext context = reader.createContext()) {
        for (int i = 0; i < values.size(); i++) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = 0; i < values.size(); i += 2) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = 1; i < values.size(); i += 2) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = 1; i < values.size(); i += 100) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = values.size() - 1; i >= 0; i--) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = values.size() - 1; i >= 0; i -= 2) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = values.size() - 2; i >= 0; i -= 2) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
        for (int i = values.size() - 1; i >= 0; i -= 100) {
          assertEquals(read.read(reader, context, i), values.get(i));
        }
      }
    }
  }

  private Stream<String> randomStrings(int count, int lengthOfLongestEntry) {
    return IntStream.range(0, count)
        .mapToObj(i -> {
          int length = ThreadLocalRandom.current().nextInt(lengthOfLongestEntry);
          byte[] bytes = new byte[length];
          if (length != 0) {
            bytes[bytes.length - 1] = 'c';
            if (length > 2) {
              Arrays.fill(bytes, 1, bytes.length - 1, (byte) 'b');
            }
            bytes[0] = 'a';
          }
          return new String(bytes, StandardCharsets.UTF_8);
        });
  }

  @FunctionalInterface
  interface Read<T> {
    T read(BaseVarByteChunkForwardIndexReaderV4 reader, BaseVarByteChunkForwardIndexReaderV4.ReaderContext context,
        int docId);
  }
}
