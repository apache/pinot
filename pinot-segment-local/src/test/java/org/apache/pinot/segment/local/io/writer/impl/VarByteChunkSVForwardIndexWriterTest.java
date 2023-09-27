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
package org.apache.pinot.segment.local.io.writer.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;


public class VarByteChunkSVForwardIndexWriterTest {
  private static final File OUTPUT_DIR =
      new File(FileUtils.getTempDirectory(), VarByteChunkSVForwardIndexWriterTest.class.getSimpleName());

  @BeforeClass
  public void setup()
      throws Exception {
    FileUtils.forceMkdir(OUTPUT_DIR);
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(OUTPUT_DIR);
  }

  @DataProvider
  public static Object[][] params() {
    int[] numDocsPerChunks = {1, 2, 20, 500, 1000};
    int[] numbersOfDocs = {10, 1000};
    int[][] entryLengths = {{1, 1}, {0, 10}, {0, 100}, {100, 100}, {900, 1000}};
    int[] versions = {2, 3};
    return Arrays.stream(ChunkCompressionType.values()).flatMap(chunkCompressionType -> IntStream.of(versions).boxed()
        .flatMap(version -> IntStream.of(numbersOfDocs).boxed().flatMap(
            totalDocs -> IntStream.of(numDocsPerChunks).boxed()
                .flatMap(numDocsPerChunk -> Arrays.stream(entryLengths).map(lengths -> new Object[]{
                    chunkCompressionType, totalDocs, numDocsPerChunk, lengths, version
                }))))).toArray(Object[][]::new);
  }

  @Test(dataProvider = "params")
  public void testPutStrings(ChunkCompressionType compressionType, int totalDocs, int numDocsPerChunk, int[] lengths,
      int version)
      throws IOException {
    String column = "testCol-" + UUID.randomUUID();
    File file = new File(OUTPUT_DIR, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    List<String[]> arrays = generateStringArrays(totalDocs, lengths, 50);
    int maxEntryLengthInBytes = arrays.stream().mapToInt(
            array -> Integer.BYTES + Arrays.stream(array).mapToInt(
                str -> Integer.BYTES + str.getBytes(UTF_8).length).sum()).max().orElse(0);
    try (VarByteChunkForwardIndexWriter writer = new VarByteChunkForwardIndexWriter(file, compressionType, totalDocs,
        numDocsPerChunk, maxEntryLengthInBytes, version)) {
      for (String[] array : arrays) {
        writer.putStringMV(array);
      }
    }
    try (VarByteChunkSVForwardIndexReader reader = new VarByteChunkSVForwardIndexReader(
        PinotDataBuffer.loadBigEndianFile(file), FieldSpec.DataType.STRING);
        ChunkReaderContext context = reader.createContext()) {
      for (int i = 0; i < arrays.size(); i++) {
        String[] array = arrays.get(i);
        byte[] bytes = reader.getBytes(i, context);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(buffer.getInt(), array.length);
        int offset = Integer.BYTES * (array.length + 1);
        for (String value : array) {
          int length = buffer.getInt();
          assertEquals(length, value.length());
          assertEquals(new String(bytes, offset, length, UTF_8), value);
          offset += length;
        }
      }
    }
  }

  @Test(dataProvider = "params")
  public void testPutBytes(ChunkCompressionType compressionType, int totalDocs, int numDocsPerChunk, int[] lengths,
      int version)
      throws IOException {
    String column = "testCol-" + UUID.randomUUID();
    File file = new File(OUTPUT_DIR, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    List<String[]> arrays = generateStringArrays(totalDocs, lengths, 50);
    int maxEntryLengthInBytes = arrays.stream().mapToInt(
            array -> Integer.BYTES + Arrays.stream(array).mapToInt(
                str -> Integer.BYTES + str.getBytes(UTF_8).length).sum()).max().orElse(0);
    try (VarByteChunkForwardIndexWriter writer = new VarByteChunkForwardIndexWriter(file, compressionType, totalDocs,
        numDocsPerChunk, maxEntryLengthInBytes, version)) {
      for (String[] array : arrays) {
        writer.putBytesMV(Arrays.stream(array).map(str -> str.getBytes(UTF_8)).toArray(byte[][]::new));
      }
    }
    try (VarByteChunkSVForwardIndexReader reader = new VarByteChunkSVForwardIndexReader(
        PinotDataBuffer.loadBigEndianFile(file), FieldSpec.DataType.BYTES);
        ChunkReaderContext context = reader.createContext()) {
      for (int i = 0; i < arrays.size(); i++) {
        String[] array = arrays.get(i);
        byte[] bytes = reader.getBytes(i, context);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(buffer.getInt(), array.length);
        int offset = Integer.BYTES * (array.length + 1);
        for (String value : array) {
          int length = buffer.getInt();
          assertEquals(length, value.length());
          assertEquals(new String(bytes, offset, length, UTF_8), value);
          offset += length;
        }
      }
    }
  }

  private static List<String[]> generateStringArrays(int count, int[] lengths, int maxElementCount) {
    Iterator<String> strings = generateStrings(lengths[0], lengths[1]);
    Random random = new Random();
    List<String[]> stringArrays = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String[] array = new String[random.nextInt(maxElementCount)];
      for (int j = 0; j < array.length; j++) {
        array[j] = strings.next();
      }
      stringArrays.add(array);
    }
    return stringArrays;
  }

  private static Iterator<String> generateStrings(int minLength, int maxLength) {
    SplittableRandom random = new SplittableRandom();
    return IntStream.generate(() -> random.nextInt(minLength, maxLength + 1)).mapToObj(length -> {
      char[] string = new char[length];
      Arrays.fill(string, 'b');
      if (string.length > 0) {
        string[0] = 'a';
      }
      if (string.length > 1) {
        string[string.length - 1] = 'c';
      }
      return new String(string);
    }).iterator();
  }
}
