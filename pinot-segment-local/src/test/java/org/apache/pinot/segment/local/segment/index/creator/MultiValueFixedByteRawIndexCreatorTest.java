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
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.spi.V1Constants.Indexes;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class MultiValueFixedByteRawIndexCreatorTest implements PinotBuffersAfterMethodCheckRule {

  protected static String _outputDir;

  protected static final Random RANDOM = new Random();

  @DataProvider(name = "compressionTypes")
  public Object[][] compressionTypes() {
    return Arrays.stream(ChunkCompressionType.values())
        .flatMap(ct -> IntStream.rangeClosed(2, 5).boxed().map(writerVersion -> new Object[]{ct, writerVersion}))
        .toArray(Object[][]::new);
  }

  @BeforeClass
  public void setup()
      throws Exception {
    _outputDir = System.getProperty("java.io.tmpdir") + File.separator + "mvFixedRawTest";
    FileUtils.forceMkdir(new File(_outputDir));
  }

  /**
   * Clean up after test
   */
  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(_outputDir));
  }

  @Test(dataProvider = "compressionTypes")
  public void testMVInt(ChunkCompressionType compressionType, int writerVersion)
      throws IOException {
    // This tests varying lengths of MV rows
    testMV(DataType.INT, ints(false), x -> x.length, int[]::new, MultiValueFixedByteRawIndexCreator::putIntMV,
        (reader, context, docId, buffer) -> {
          int length = reader.getIntMV(docId, buffer, context);
          return Arrays.copyOf(buffer, length);
        }, compressionType, writerVersion);

    // This tests a fixed length of MV rows to ensure there are no BufferOverflowExceptions on filling up the chunk
    testMV(DataType.INT, ints(true), x -> x.length, int[]::new, MultiValueFixedByteRawIndexCreator::putIntMV,
        (reader, context, docId, buffer) -> {
          int length = reader.getIntMV(docId, buffer, context);
          return Arrays.copyOf(buffer, length);
        }, compressionType, writerVersion);
  }

  @Test(dataProvider = "compressionTypes")
  public void testMVLong(ChunkCompressionType compressionType, int writerVersion)
      throws IOException {
    // This tests varying lengths of MV rows
    testMV(DataType.LONG, longs(false), x -> x.length, long[]::new, MultiValueFixedByteRawIndexCreator::putLongMV,
        (reader, context, docId, buffer) -> {
          int length = reader.getLongMV(docId, buffer, context);
          return Arrays.copyOf(buffer, length);
        }, compressionType, writerVersion);

    // This tests a fixed length of MV rows to ensure there are no BufferOverflowExceptions on filling up the chunk
    testMV(DataType.LONG, longs(true), x -> x.length, long[]::new, MultiValueFixedByteRawIndexCreator::putLongMV,
        (reader, context, docId, buffer) -> {
          int length = reader.getLongMV(docId, buffer, context);
          return Arrays.copyOf(buffer, length);
        }, compressionType, writerVersion);
  }

  @Test(dataProvider = "compressionTypes")
  public void testMVFloat(ChunkCompressionType compressionType, int writerVersion)
      throws IOException {
    // This tests varying lengths of MV rows
    testMV(DataType.FLOAT, floats(false), x -> x.length, float[]::new, MultiValueFixedByteRawIndexCreator::putFloatMV,
        (reader, context, docId, buffer) -> {
          int length = reader.getFloatMV(docId, buffer, context);
          return Arrays.copyOf(buffer, length);
        }, compressionType, writerVersion);

    // This tests a fixed length of MV rows to ensure there are no BufferOverflowExceptions on filling up the chunk
    testMV(DataType.FLOAT, floats(true), x -> x.length, float[]::new, MultiValueFixedByteRawIndexCreator::putFloatMV,
        (reader, context, docId, buffer) -> {
          int length = reader.getFloatMV(docId, buffer, context);
          return Arrays.copyOf(buffer, length);
        }, compressionType, writerVersion);
  }

  @Test(dataProvider = "compressionTypes")
  public void testMVDouble(ChunkCompressionType compressionType, int writerVersion)
      throws IOException {
    // This tests varying lengths of MV rows
    testMV(DataType.DOUBLE, doubles(false), x -> x.length, double[]::new,
        MultiValueFixedByteRawIndexCreator::putDoubleMV, (reader, context, docId, buffer) -> {
          int length = reader.getDoubleMV(docId, buffer, context);
          return Arrays.copyOf(buffer, length);
        }, compressionType, writerVersion);

    // This tests a fixed length of MV rows to ensure there are no BufferOverflowExceptions on filling up the chunk
    testMV(DataType.DOUBLE, doubles(true), x -> x.length, double[]::new,
        MultiValueFixedByteRawIndexCreator::putDoubleMV, (reader, context, docId, buffer) -> {
          int length = reader.getDoubleMV(docId, buffer, context);
          return Arrays.copyOf(buffer, length);
        }, compressionType, writerVersion);
  }

  public MultiValueFixedByteRawIndexCreator getMultiValueFixedByteRawIndexCreator(ChunkCompressionType compressionType,
      String column, int numDocs, DataType dataType, int maxElements, int writerVersion)
      throws IOException {
    return new MultiValueFixedByteRawIndexCreator(new File(_outputDir), compressionType, column, numDocs, dataType,
        maxElements, false, writerVersion, 1024 * 1024, 1000);
  }

  public <T> void testMV(DataType dataType, List<T> inputs, ToIntFunction<T> sizeof, IntFunction<T> constructor,
      Injector<T> injector, Extractor<T> extractor, ChunkCompressionType compressionType, int writerVersion)
      throws IOException {
    String column = "testCol_" + dataType;
    int numDocs = inputs.size();
    int maxElements = inputs.stream().mapToInt(sizeof).max().orElseThrow(RuntimeException::new);
    File file = new File(_outputDir, column + Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(file);
    try (MultiValueFixedByteRawIndexCreator creator = getMultiValueFixedByteRawIndexCreator(compressionType, column,
        numDocs, dataType, maxElements, writerVersion)) {
      inputs.forEach(input -> injector.inject(creator, input));
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, "");
        ForwardIndexReader reader = ForwardIndexReaderFactory.createRawIndexReader(buffer, dataType, false);
        ForwardIndexReaderContext context = reader.createContext()) {
      T valueBuffer = constructor.apply(maxElements);
      for (int i = 0; i < numDocs; i++) {
        T input = inputs.get(i);
        assertEquals(reader.getNumValuesMV(i, context), sizeof.applyAsInt(input));
        assertEquals(extractor.extract(reader, context, i, valueBuffer), input);
      }

      // Value byte range test
      assertTrue(reader.isBufferByteRangeInfoSupported());
      assertFalse(reader.isFixedOffsetMappingType());
      List<ForwardIndexReader.ByteRange> ranges = new ArrayList<>();
      try (ForwardIndexReaderContext valueRangeContext = reader.createContext()) {
        for (int i = 0; i < numDocs; i++) {
          try {
            reader.recordDocIdByteRanges(i, valueRangeContext, ranges);
          } catch (Exception e) {
            fail("Failed to record byte ranges for docId: " + i, e);
          }
        }
      }
    }
  }

  interface Extractor<T> {
    T extract(ForwardIndexReader reader, ForwardIndexReaderContext context, int offset, T buffer);
  }

  interface Injector<T> {
    void inject(MultiValueFixedByteRawIndexCreator creator, T input);
  }

  private static List<int[]> ints(boolean isFixedMVRowLength) {
    return IntStream.range(0, 1000).mapToObj(i -> new int[isFixedMVRowLength ? 50 : RANDOM.nextInt(50)]).peek(array -> {
      for (int i = 0; i < array.length; i++) {
        array[i] = RANDOM.nextInt();
      }
    }).collect(Collectors.toList());
  }

  private static List<long[]> longs(boolean isFixedMVRowLength) {
    return IntStream.range(0, 1000)
        .mapToObj(i -> new long[isFixedMVRowLength ? 50 : RANDOM.nextInt(50)])
        .peek(array -> {
          for (int i = 0; i < array.length; i++) {
            array[i] = RANDOM.nextLong();
          }
        })
        .collect(Collectors.toList());
  }

  private static List<float[]> floats(boolean isFixedMVRowLength) {
    return IntStream.range(0, 1000)
        .mapToObj(i -> new float[isFixedMVRowLength ? 50 : RANDOM.nextInt(50)])
        .peek(array -> {
          for (int i = 0; i < array.length; i++) {
            array[i] = RANDOM.nextFloat();
          }
        })
        .collect(Collectors.toList());
  }

  private static List<double[]> doubles(boolean isFixedMVRowLength) {
    return IntStream.range(0, 1000)
        .mapToObj(i -> new double[isFixedMVRowLength ? 50 : RANDOM.nextInt(50)])
        .peek(array -> {
          for (int i = 0; i < array.length; i++) {
            array[i] = RANDOM.nextDouble();
          }
        })
        .collect(Collectors.toList());
  }
}
