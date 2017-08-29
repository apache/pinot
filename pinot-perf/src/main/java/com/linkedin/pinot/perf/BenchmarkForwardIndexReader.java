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
package com.linkedin.pinot.perf;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.io.reader.impl.SortedForwardIndexReader;
import com.linkedin.pinot.core.io.reader.impl.SortedValueReaderContext;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.integration.tests.ClusterTest;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@SuppressWarnings("unused")
@State(Scope.Benchmark)
public class BenchmarkForwardIndexReader {
  private static final Random RANDOM = new Random();
  private static final URL RESOURCE_URL =
      ClusterTest.class.getClassLoader().getResource("On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz");
  private static final String AVRO_FILE_NAME = "On_Time_On_Time_Performance_2014_1.avro";
  private static final String TABLE_NAME = "table";

  private static final String SV_UNSORTED_COLUMN_NAME = "FlightNum";
  private static final String SV_UNSORTED_FORWARD_INDEX_NAME = SV_UNSORTED_COLUMN_NAME + ".sv.unsorted.fwd";
  private static final String SV_SORTED_COLUMN_NAME = "DaysSinceEpoch";
  private static final String SV_SORTED_INDEX_NAME = SV_SORTED_COLUMN_NAME + ".sv.sorted.fwd";
  private static final String MV_COLUMN_NAME = "DivTailNums";
  private static final String MV_FORWARD_INDEX_NAME = MV_COLUMN_NAME + ".mv.fwd";

  private final File _tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());

  private int _numDocs;
  private FixedBitSingleValueReader _fixedBitSingleValueReader;
  private SortedForwardIndexReader _sortedForwardIndexReader;
  private FixedBitMultiValueReader _fixedBitMultiValueReader;
  private int[] _buffer;

  @Setup
  public void setUp() throws Exception {
    Preconditions.checkNotNull(RESOURCE_URL);
    FileUtils.deleteQuietly(_tempDir);

    File avroDir = new File(_tempDir, "avro");
    TarGzCompressionUtils.unTar(new File(TestUtils.getFileFromResourceUrl(RESOURCE_URL)), avroDir);
    File avroFile = new File(avroDir, AVRO_FILE_NAME);

    File dataDir = new File(_tempDir, "index");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(avroFile, dataDir, TABLE_NAME);
    segmentGeneratorConfig.setSegmentVersion(SegmentVersion.v1);
    driver.init(segmentGeneratorConfig);
    driver.build();

    File indexDir = new File(dataDir, TABLE_NAME);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    _numDocs = segmentMetadata.getTotalDocs();
    int svUnsortedNumBitsPerValue = segmentMetadata.getColumnMetadataFor(SV_UNSORTED_COLUMN_NAME).getBitsPerElement();
    int svSortedCardinality = segmentMetadata.getColumnMetadataFor(SV_SORTED_COLUMN_NAME).getCardinality();
    ColumnMetadata mvColumnMetadata = segmentMetadata.getColumnMetadataFor(MV_COLUMN_NAME);
    int mvNumValues = mvColumnMetadata.getTotalNumberOfEntries();
    int mvNumBitsPerValue = mvColumnMetadata.getBitsPerElement();
    int mvMaxNumMultiValues = mvColumnMetadata.getMaxNumberOfMultiValues();

    PinotDataBuffer svUnsortedDataBuffer =
        PinotDataBuffer.fromFile(new File(indexDir, SV_UNSORTED_FORWARD_INDEX_NAME), ReadMode.mmap,
            FileChannel.MapMode.READ_ONLY, SV_UNSORTED_COLUMN_NAME);
    PinotDataBuffer svSortedDataBuffer =
        PinotDataBuffer.fromFile(new File(indexDir, SV_SORTED_INDEX_NAME), ReadMode.mmap, FileChannel.MapMode.READ_ONLY,
            SV_SORTED_COLUMN_NAME);
    PinotDataBuffer mvDataBuffer = PinotDataBuffer.fromFile(new File(indexDir, MV_FORWARD_INDEX_NAME), ReadMode.mmap,
        FileChannel.MapMode.READ_ONLY, MV_COLUMN_NAME);
    _fixedBitSingleValueReader =
        new FixedBitSingleValueReader(svUnsortedDataBuffer, _numDocs, svUnsortedNumBitsPerValue);
    _sortedForwardIndexReader = new SortedForwardIndexReader(
        new FixedByteSingleValueMultiColReader(svSortedDataBuffer, svSortedCardinality, new int[]{4, 4}), _numDocs);
    _fixedBitMultiValueReader = new FixedBitMultiValueReader(mvDataBuffer, _numDocs, mvNumValues, mvNumBitsPerValue);
    _buffer = new int[mvMaxNumMultiValues];
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int fixedBitSingleValueReader() {
    ReaderContext context = _fixedBitSingleValueReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _fixedBitSingleValueReader.getInt(i, context);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int sortedForwardIndexReaderSequential() {
    SortedValueReaderContext context = _sortedForwardIndexReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _sortedForwardIndexReader.getInt(i, context);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int sortedForwardIndexReaderRandom() {
    SortedValueReaderContext context = _sortedForwardIndexReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _sortedForwardIndexReader.getInt(RANDOM.nextInt(_numDocs), context);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int fixedBitMultiValueReaderSequential() {
    FixedBitMultiValueReader.Context context = _fixedBitMultiValueReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _fixedBitMultiValueReader.getIntArray(i, _buffer, context);
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int fixedBitMultiValueReaderRandom() {
    FixedBitMultiValueReader.Context context = _fixedBitMultiValueReader.createContext();
    int ret = 0;
    for (int i = 0; i < _numDocs; i++) {
      ret += _fixedBitMultiValueReader.getIntArray(RANDOM.nextInt(_numDocs), _buffer, context);
    }
    return ret;
  }

  @TearDown
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(BenchmarkForwardIndexReader.class.getSimpleName())
        .warmupTime(TimeValue.seconds(10))
        .warmupIterations(5)
        .measurementTime(TimeValue.seconds(10))
        .measurementIterations(5)
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
