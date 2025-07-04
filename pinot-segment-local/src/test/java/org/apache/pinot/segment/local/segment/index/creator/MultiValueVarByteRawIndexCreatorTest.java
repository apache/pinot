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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class MultiValueVarByteRawIndexCreatorTest implements PinotBuffersAfterMethodCheckRule {

  private static final File OUTPUT_DIR =
      new File(FileUtils.getTempDirectory(), MultiValueVarByteRawIndexCreatorTest.class.getSimpleName());

  @BeforeClass
  public void setup()
      throws Exception {
    FileUtils.forceMkdir(OUTPUT_DIR);
  }

  @DataProvider
  public Object[][] params() {
    return Arrays.stream(ChunkCompressionType.values())
        .flatMap(chunkCompressionType -> IntStream.rangeClosed(2, 5)
            .boxed()
            .flatMap(writerVersion -> IntStream.of(10, 100)
                .boxed()
                .flatMap(maxLength -> Stream.of(true, false)
                    .flatMap(useFullSize -> IntStream.of(1, 10, 20).boxed().map(maxNumEntries -> new Object[]{
                        chunkCompressionType, useFullSize, writerVersion, maxLength, maxNumEntries
                    })))))
        .toArray(Object[][]::new);
  }

  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(OUTPUT_DIR);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOverflowElementCount()
      throws IOException {
    new MultiValueVarByteRawIndexCreator(OUTPUT_DIR, ChunkCompressionType.PASS_THROUGH, "column", 10000,
        DataType.STRING, 1, Integer.MAX_VALUE / 2);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOverflowMaxLengthInBytes()
      throws IOException {
    // Contrived to produce a positive chunk size > Integer.MAX_VALUE but not fail num elements checks
    // This check only applies to v2/v3
    new MultiValueVarByteRawIndexCreator(OUTPUT_DIR, ChunkCompressionType.PASS_THROUGH, "column", 10000,
        DataType.STRING, 2, Integer.MAX_VALUE - Integer.BYTES - 2 * Integer.BYTES, 2,
        ForwardIndexConfig.getDefaultTargetMaxChunkSizeBytes(), ForwardIndexConfig.getDefaultTargetDocsPerChunk());
  }

  @Test(dataProvider = "params")
  public void testMVString(ChunkCompressionType compressionType, boolean useFullSize, int writerVersion, int maxLength,
      int maxNumEntries)
      throws IOException {
    String column = "testCol-" + UUID.randomUUID();
    int numDocs = 1000;
    File file = new File(OUTPUT_DIR, column + Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
    List<String[]> inputs = new ArrayList<>();
    Random random = new Random();
    int maxTotalLength = 0;
    int maxElements = 0;
    for (int i = 0; i < numDocs; i++) {
      int numEntries = useFullSize ? maxNumEntries : random.nextInt(maxNumEntries + 1);
      maxElements = Math.max(numEntries, maxElements);
      String[] values = new String[numEntries];
      int serializedLength = 0;
      for (int j = 0; j < numEntries; j++) {
        int length = useFullSize ? maxLength : random.nextInt(maxLength + 1);
        serializedLength += length;
        char[] value = new char[length];
        Arrays.fill(value, 'b');
        if (value.length > 0) {
          value[0] = 'a';
        }
        if (value.length > 1) {
          value[value.length - 1] = 'c';
        }
        values[j] = new String(value);
      }
      maxTotalLength = Math.max(serializedLength, maxTotalLength);
      inputs.add(values);
    }
    try (MultiValueVarByteRawIndexCreator creator = new MultiValueVarByteRawIndexCreator(OUTPUT_DIR, compressionType,
        column, numDocs, DataType.STRING, maxTotalLength, maxElements, writerVersion, 1024 * 1024, 1000)) {
      for (String[] input : inputs) {
        creator.putStringMV(input);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, "");
        ForwardIndexReader reader = ForwardIndexReaderFactory.createRawIndexReader(buffer, DataType.STRING, false);
        ForwardIndexReaderContext context = reader.createContext()) {
      String[] values = new String[maxElements];
      for (int i = 0; i < numDocs; i++) {
        String[] input = inputs.get(i);
        assertEquals(reader.getNumValuesMV(i, context), input.length);
        int length = reader.getStringMV(i, values, context);
        assertEquals(Arrays.copyOf(values, length), input);
      }
    }
  }

  @Test(dataProvider = "params")
  public void testMVBytes(ChunkCompressionType compressionType, boolean useFullSize, int writerVersion, int maxLength,
      int maxNumEntries)
      throws IOException {
    String column = "testCol-" + UUID.randomUUID();
    int numDocs = 1000;
    File file = new File(OUTPUT_DIR, column + Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
    List<byte[][]> inputs = new ArrayList<>();
    Random random = new Random();
    int maxTotalLength = 0;
    int maxElements = 0;
    for (int i = 0; i < numDocs; i++) {
      int numEntries = useFullSize ? maxNumEntries : random.nextInt(maxNumEntries + 1);
      maxElements = Math.max(numEntries, maxElements);
      byte[][] values = new byte[numEntries][];
      int serializedLength = 0;
      for (int j = 0; j < numEntries; j++) {
        int length = useFullSize ? maxLength : random.nextInt(maxLength + 1);
        serializedLength += length;
        byte[] value = new byte[length];
        Arrays.fill(value, (byte) 'b');
        if (value.length > 0) {
          value[0] = 'a';
        }
        if (value.length > 1) {
          value[value.length - 1] = 'c';
        }
        values[j] = value;
      }
      maxTotalLength = Math.max(serializedLength, maxTotalLength);
      inputs.add(values);
    }
    try (MultiValueVarByteRawIndexCreator creator = new MultiValueVarByteRawIndexCreator(OUTPUT_DIR, compressionType,
        column, numDocs, DataType.BYTES, writerVersion, maxTotalLength, maxElements, 1024 * 1024, 1000)) {
      for (byte[][] input : inputs) {
        creator.putBytesMV(input);
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, "");
        ForwardIndexReader reader = ForwardIndexReaderFactory.createRawIndexReader(buffer, DataType.BYTES, false);
        ForwardIndexReaderContext context = reader.createContext()) {
      byte[][] values = new byte[maxElements][];
      for (int i = 0; i < numDocs; i++) {
        byte[][] input = inputs.get(i);
        assertEquals(reader.getNumValuesMV(i, context), input.length);
        int length = reader.getBytesMV(i, values, context);
        assertEquals(Arrays.copyOf(values, length), input);
      }
    }
  }
}
