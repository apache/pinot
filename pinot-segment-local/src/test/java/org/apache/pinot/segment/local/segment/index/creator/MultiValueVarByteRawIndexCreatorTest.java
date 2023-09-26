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
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MultiValueVarByteRawIndexCreatorTest {

  private static final File OUTPUT_DIR =
      new File(FileUtils.getTempDirectory(), MultiValueVarByteRawIndexCreatorTest.class.getSimpleName());

  @BeforeClass
  public void setup()
      throws Exception {
    FileUtils.forceMkdir(OUTPUT_DIR);
  }

  @DataProvider
  public Object[][] params() {
    return Arrays.stream(ChunkCompressionType.values()).flatMap(chunkCompressionType -> IntStream.of(2, 4).boxed()
            .flatMap(writerVersion -> IntStream.of(10, 15, 20, 1000).boxed().flatMap(maxLength -> Stream.of(true, false)
                .flatMap(
                    useFullSize -> IntStream.range(1, 20).map(i -> i * 2 - 1).boxed().map(maxNumEntries -> new Object[]{
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
    new MultiValueVarByteRawIndexCreator(OUTPUT_DIR, ChunkCompressionType.PASS_THROUGH,
        "column", 10000, DataType.STRING, 1, Integer.MAX_VALUE / 2);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOverflowMaxLengthInBytes()
      throws IOException {
    // contrived to produce a positive chunk size > Integer.MAX_VALUE but not fail num elements checks
    new MultiValueVarByteRawIndexCreator(OUTPUT_DIR, ChunkCompressionType.PASS_THROUGH,
        "column", 10000, DataType.STRING, Integer.MAX_VALUE - Integer.BYTES - 2 * Integer.BYTES, 2);
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
        column, numDocs, DataType.STRING, maxTotalLength, maxElements, writerVersion)) {
      for (String[] input : inputs) {
        creator.putStringMV(input);
      }
    }

    //read
    final PinotDataBuffer buffer = PinotDataBuffer.mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, "");
    ForwardIndexReader reader = ForwardIndexReaderFactory.createRawIndexReader(buffer, DataType.STRING, false);
    final ForwardIndexReaderContext context = reader.createContext();
    String[] values = new String[maxElements];
    for (int i = 0; i < numDocs; i++) {
      int length = reader.getStringMV(i, values, context);
      String[] readValue = Arrays.copyOf(values, length);
      Assert.assertEquals(inputs.get(i), readValue);
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
        column, numDocs, DataType.BYTES, maxTotalLength, maxElements, writerVersion)) {
      for (byte[][] input : inputs) {
        creator.putBytesMV(input);
      }
    }

    //read
    final PinotDataBuffer buffer = PinotDataBuffer.mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, "");
    ForwardIndexReader reader = ForwardIndexReaderFactory.createRawIndexReader(buffer, DataType.BYTES, false);
    final ForwardIndexReaderContext context = reader.createContext();
    byte[][] values = new byte[maxElements][];
    for (int i = 0; i < numDocs; i++) {
      int length = reader.getBytesMV(i, values, context);
      byte[][] readValue = Arrays.copyOf(values, length);
      for (int j = 0; j < length; j++) {
        Assert.assertTrue(Arrays.equals(inputs.get(i)[j], readValue[j]));
      }
    }
  }
}
