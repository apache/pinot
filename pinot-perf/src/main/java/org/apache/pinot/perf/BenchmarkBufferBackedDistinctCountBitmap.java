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
package org.apache.pinot.perf;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.common.DataFetcher;
import org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.DistinctCountBitmapAggregationFunction;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.RoaringBitmap;


/// Measures DISTINCTCOUNTBITMAP over serialized bitmaps stored in a raw SV BYTES forward index, read through the
/// projection layer, with the borrowed-buffer input path (`useBufferBackedDistinctCountBitmap`) off and on.
///
/// The SPARSE fixture spreads the inserted IDs over a wide range so every serialized bitmap decodes into many tiny
/// array containers — the shape where buffer-backed container unions historically regressed. The DENSE fixture packs
/// the IDs into one container's key range.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
public class BenchmarkBufferBackedDistinctCountBitmap {
  private static final String COLUMN = "bitmap";
  private static final int NUM_BITMAPS = 64;
  private static final int IDS_PER_BITMAP = 2048;
  private static final int UNIVERSE_SIZE = 8192;

  @Param({"SPARSE", "DENSE"})
  public String _fixture;

  @Param({"PASS_THROUGH", "LZ4"})
  public String _codec;

  @Param({"2", "4"})
  public int _writerVersion;

  @Param({"false", "true"})
  public boolean _bufferBacked;

  private File _dir;
  private PinotDataBuffer _mapped;
  private ForwardIndexReader<?> _reader;
  private DataFetcher _fetcher;
  private DataBlockCache _blockCache;
  private DataSource _dataSource;
  private DistinctCountBitmapAggregationFunction _function;
  private Map<ExpressionContext, BlockValSet> _input;
  private int[] _docIds;

  @Setup
  public void setUp()
      throws Exception {
    _dir = Files.createTempDirectory("bitmap-buffer-benchmark").toFile();
    SplittableRandom random = new SplittableRandom(42);

    // Shared candidate IDs so the input bitmaps overlap. SPARSE spreads them over the full int range (one array
    // container per high 16 bits, each nearly empty); DENSE keeps them within a single container's key range.
    int[] universe = new int[UNIVERSE_SIZE];
    boolean sparse = "SPARSE".equals(_fixture);
    for (int i = 0; i < UNIVERSE_SIZE; i++) {
      universe[i] = sparse ? random.nextInt(Integer.MAX_VALUE) : random.nextInt(UNIVERSE_SIZE);
    }

    byte[][] values = new byte[NUM_BITMAPS][];
    int maxLength = 0;
    for (int i = 0; i < NUM_BITMAPS; i++) {
      RoaringBitmap bitmap = new RoaringBitmap();
      for (int j = 0; j < IDS_PER_BITMAP; j++) {
        bitmap.add(universe[random.nextInt(UNIVERSE_SIZE)]);
      }
      values[i] = RoaringBitmapUtils.serialize(bitmap);
      maxLength = Math.max(maxLength, values[i].length);
    }

    try (SingleValueVarByteRawIndexCreator writer = new SingleValueVarByteRawIndexCreator(_dir,
        ChunkCompressionType.valueOf(_codec), COLUMN, NUM_BITMAPS, DataType.BYTES, maxLength, false, _writerVersion,
        1 << 20, 64)) {
      for (byte[] value : values) {
        writer.putBytes(value);
      }
    }

    File file = new File(_dir, COLUMN + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    _mapped = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
    _reader = ForwardIndexReaderFactory.getInstance().createRawIndexReader(_mapped, DataType.BYTES, true);

    _dataSource = Mockito.mock(DataSource.class);
    DataSourceMetadata metadata = Mockito.mock(DataSourceMetadata.class);
    Mockito.when(_dataSource.getDataSourceMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getDataType()).thenReturn(DataType.BYTES);
    Mockito.when(metadata.isSingleValue()).thenReturn(true);
    Mockito.when(_dataSource.getForwardIndex()).thenReturn((ForwardIndexReader) _reader);

    Map<String, String> queryOptions =
        _bufferBacked ? Map.of(QueryOptionKey.USE_BUFFER_BACKED_DISTINCT_COUNT_BITMAP, "true") : Map.of();
    _fetcher = new DataFetcher(Map.of(COLUMN, _dataSource), queryOptions);
    _blockCache = new DataBlockCache(_fetcher);
    _function = new DistinctCountBitmapAggregationFunction(List.of(ExpressionContext.forIdentifier(COLUMN)), true);
    _docIds = new int[NUM_BITMAPS];
    Arrays.setAll(_docIds, i -> i);
    _input = Map.of(ExpressionContext.forIdentifier(COLUMN),
        new ProjectionBlockValSet(_blockCache, COLUMN, _dataSource));
  }

  @TearDown
  public void tearDown()
      throws Exception {
    _fetcher.close();
    _reader.close();
    _mapped.close();
    FileUtils.deleteDirectory(_dir);
  }

  @Benchmark
  public int aggregate() {
    _blockCache.initNewBlock(_docIds, _docIds.length);
    AggregationResultHolder holder = _function.createAggregationResultHolder();
    _function.aggregate(_docIds.length, holder, _input);
    return _function.extractAggregationResult(holder).getCardinality();
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkBufferBackedDistinctCountBitmap.class.getSimpleName()).build())
        .run();
  }
}
