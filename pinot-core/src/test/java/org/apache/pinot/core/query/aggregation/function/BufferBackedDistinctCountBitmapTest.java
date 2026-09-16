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
package org.apache.pinot.core.query.aggregation.function;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.common.DataFetcher;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises mapped/chunked BYTES through projection and aggregation, including borrowed-buffer invalidation.
@SuppressWarnings({"rawtypes", "unchecked"})
public class BufferBackedDistinctCountBitmapTest {
  @DataProvider
  public Object[][] formats() {
    List<Object[]> cases = new ArrayList<>();
    for (int version : new int[]{2, 3, 4, 6}) {
      for (ChunkCompressionType codec : new ChunkCompressionType[]{ChunkCompressionType.PASS_THROUGH,
          ChunkCompressionType.LZ4, ChunkCompressionType.SNAPPY, ChunkCompressionType.ZSTANDARD}) {
        cases.add(new Object[]{version, codec});
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "formats")
  public void testBorrowedInputOwnership(int version, ChunkCompressionType codec)
      throws Exception {
    File dir = Files.createTempDirectory("bitmap-buffer").toFile();
    String column = "bitmap";
    RoaringBitmap large = new RoaringBitmap();
    for (int i = 0; i < 20000; i++) {
      large.add(i * 7919);
    }
    RoaringBitmap run = new RoaringBitmap();
    run.add(40000L, 50000L);
    run.runOptimize();
    RoaringBitmap[] bitmaps = {new RoaringBitmap(), RoaringBitmap.bitmapOf(1, 65535, -1), large, null, run};
    byte[][] values = new byte[bitmaps.length][];
    for (int i = 0; i < values.length; i++) {
      // Invalid serialized data for a null row: aggregation must skip it before attempting to build a bitmap view.
      values[i] = bitmaps[i] == null ? new byte[]{1} : RoaringBitmapUtils.serialize(bitmaps[i]);
    }
    int maxLength = Arrays.stream(values).mapToInt(v -> v.length).max().orElseThrow();
    try {
      try (SingleValueVarByteRawIndexCreator writer = new SingleValueVarByteRawIndexCreator(dir, codec, column,
          values.length, DataType.BYTES, maxLength, false, version, 1024, 2)) {
        for (byte[] value : values) {
          writer.putBytes(value);
        }
      }
      File file = new File(dir, column + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
      byte[] original = Files.readAllBytes(file.toPath());
      DistinctCountBitmapAggregationFunction function =
          new DistinctCountBitmapAggregationFunction(List.of(ExpressionContext.forIdentifier(column)), true);
      AggregationResultHolder aggregate = function.createAggregationResultHolder();
      GroupByResultHolder sv = function.createGroupByResultHolder(4, 10);
      GroupByResultHolder mv = function.createGroupByResultHolder(4, 10);
      RoaringBitmap expected = new RoaringBitmap();
      try (PinotDataBuffer mapped = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
          ForwardIndexReader reader = ForwardIndexReaderFactory.getInstance().createRawIndexReader(mapped,
              DataType.BYTES, true)) {
        // Pin the actual reader view, including huge records, repeated reads, reverse reads and empty values.
        try (ForwardIndexReaderContext context = reader.createContext()) {
          int[] docs = {4, 2, 2, 1, 0, 3};
          reader.readBytesValues(docs, 1, docs.length, (value, index) -> {
            ByteBuffer buffer = (ByteBuffer) value;
            assertTrue(buffer.isDirect());
            assertTrue(buffer.isReadOnly());
            byte[] actual = new byte[buffer.remaining()];
            buffer.get(actual);
            assertEquals(actual, values[docs[index]]);
          }, context);
          assertThrows(IllegalStateException.class, () -> reader.readBytesValues(new int[]{2}, 0, 1,
              (value, index) -> {
                throw new IllegalStateException("consumer failed");
              }, context));
          reader.readBytesValues(new int[]{2}, 0, 1, (value, index) ->
              assertEquals(new ImmutableRoaringBitmap((ByteBuffer) value).toRoaringBitmap(), large), context);
        }
        DataSource source = mock(DataSource.class);
        DataSourceMetadata metadata = mock(DataSourceMetadata.class);
        when(source.getDataSourceMetadata()).thenReturn(metadata);
        when(metadata.getDataType()).thenReturn(DataType.BYTES);
        when(metadata.isSingleValue()).thenReturn(true);
        when(source.getForwardIndex()).thenReturn(reader);
        NullValueVectorReader nulls = mock(NullValueVectorReader.class);
        when(nulls.getNullBitmap()).thenReturn(new ImmutableRoaringBitmap(
            ByteBuffer.wrap(RoaringBitmapUtils.serialize(RoaringBitmap.bitmapOf(3)))));
        when(source.getNullValueVector()).thenReturn(nulls);
        try (DataFetcher disabled = new DataFetcher(Map.of(column, source), Map.of())) {
          assertFalse(disabled.isBytesBufferEnabled());
        }
        try (DataFetcher fetcher = new DataFetcher(Map.of(column, source),
            Map.of(QueryOptionKey.USE_BUFFER_BACKED_DISTINCT_COUNT_BITMAP, "true"))) {
          DataBlockCache cache = new DataBlockCache(fetcher);
          int[][] blocks = {{0, 1, 2, 3, 4}, {4, 3, 2, 2, 1, 0}};
          for (int[] docs : blocks) {
            cache.initNewBlock(docs, docs.length);
            BlockValSet block = new ProjectionBlockValSet(cache, column, source);
            assertTrue(block.isBytesBufferEnabled());
            Map<ExpressionContext, BlockValSet> input = Map.of(ExpressionContext.forIdentifier(column), block);
            int[] groupKeys = new int[docs.length];
            int[][] groups = new int[docs.length][];
            Arrays.setAll(groups, i -> docs[i] <= 1 ? new int[]{0, 1} : new int[]{0});
            function.aggregate(docs.length, aggregate, input);
            function.aggregateGroupBySV(docs.length, groupKeys, sv, input);
            function.aggregateGroupByMV(docs.length, groups, mv, input);
            for (int doc : docs) {
              if (bitmaps[doc] != null) {
                expected.or(bitmaps[doc]);
              }
            }
            assertEquals(function.extractAggregationResult(aggregate), expected);
          }
        }
      }
      // Both mapped source and decompression contexts are now closed. Accumulators must own every result container.
      assertEquals(function.extractAggregationResult(aggregate), expected);
      assertEquals(function.extractGroupByResult(sv, 0), expected);
      assertEquals(function.extractGroupByResult(mv, 0), expected);
      assertEquals(function.extractGroupByResult(mv, 1), bitmaps[1]);
      assertEquals(ObjectSerDeUtils.ROARING_BITMAP_SER_DE.deserialize(
          function.serializeIntermediateResult(function.extractAggregationResult(aggregate)).getBytes()), expected);
      assertEquals(Files.readAllBytes(file.toPath()), original);
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }
}
