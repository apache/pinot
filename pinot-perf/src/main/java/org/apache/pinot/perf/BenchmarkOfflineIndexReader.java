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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.integration.tests.ClusterTest;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.readers.DoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.FloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.IntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitMVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.sorted.SortedIndexReaderImpl;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.util.TestUtils;
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
public class BenchmarkOfflineIndexReader {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkOfflineIndexReader");
  private static final Random RANDOM = new Random();
  private static final URL RESOURCE_URL =
      ClusterTest.class.getClassLoader().getResource("On_Time_On_Time_Performance_2014_100k_subset_nonulls.tar.gz");
  private static final String AVRO_FILE_NAME = "On_Time_On_Time_Performance_2014_1.avro";
  private static final String TABLE_NAME = "table";

  // Forward index
  private static final String SV_UNSORTED_COLUMN_NAME = "FlightNum";
  private static final String SV_SORTED_COLUMN_NAME = "DaysSinceEpoch";
  private static final String MV_COLUMN_NAME = "DivTailNums";

  // Dictionary
  private static final int NUM_ROUNDS = 10000;
  private static final String INT_COLUMN_NAME = "DivActualElapsedTime";
  private static final String LONG_COLUMN_NAME = "DivTotalGTimes";
  private static final String FLOAT_COLUMN_NAME = "DepDelayMinutes";
  private static final String DOUBLE_COLUMN_NAME = "DepDelay";
  private static final String STRING_COLUMN_NAME = "DestCityName";

  // Forward index
  private int _numDocs;
  private FixedBitSVForwardIndexReader _fixedBitSingleValueReader;
  private SortedIndexReaderImpl _sortedForwardIndexReader;
  private FixedBitMVForwardIndexReader _fixedBitMultiValueReader;
  private int[] _buffer;

  // Dictionary
  private IntDictionary _intDictionary;
  private LongDictionary _longDictionary;
  private FloatDictionary _floatDictionary;
  private DoubleDictionary _doubleDictionary;
  private StringDictionary _stringDictionary;

  @Setup
  public void setUp()
      throws Exception {
    Preconditions.checkNotNull(RESOURCE_URL);
    FileUtils.deleteQuietly(TEMP_DIR);

    File avroDir = new File(TEMP_DIR, "avro");
    TarGzCompressionUtils.untar(new File(TestUtils.getFileFromResourceUrl(RESOURCE_URL)), avroDir);
    File avroFile = new File(avroDir, AVRO_FILE_NAME);

    File dataDir = new File(TEMP_DIR, "index");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(avroFile, dataDir, TABLE_NAME));
    driver.build();

    File indexDir = new File(dataDir, TABLE_NAME);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    Map<String, Object> props = new HashMap<>();
    props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap.toString());

    SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(indexDir.toURI(), new SegmentDirectoryLoaderContext.Builder().setSegmentName(segmentMetadata.getName())
            .setSegmentDirectoryConfigs(new PinotConfiguration(props)).build());
    SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();

    // Forward index
    _numDocs = segmentMetadata.getTotalDocs();
    _fixedBitSingleValueReader = new FixedBitSVForwardIndexReader(
        segmentReader.getIndexFor(SV_UNSORTED_COLUMN_NAME, StandardIndexes.forward()), _numDocs,
        segmentMetadata.getColumnMetadataFor(SV_UNSORTED_COLUMN_NAME).getBitsPerElement());
    _sortedForwardIndexReader =
        new SortedIndexReaderImpl(segmentReader.getIndexFor(SV_SORTED_COLUMN_NAME, StandardIndexes.forward()),
            segmentMetadata.getColumnMetadataFor(SV_SORTED_COLUMN_NAME).getCardinality());
    ColumnMetadata mvColumnMetadata = segmentMetadata.getColumnMetadataFor(MV_COLUMN_NAME);
    _fixedBitMultiValueReader =
        new FixedBitMVForwardIndexReader(segmentReader.getIndexFor(MV_COLUMN_NAME, StandardIndexes.forward()),
            _numDocs, mvColumnMetadata.getTotalNumberOfEntries(), mvColumnMetadata.getBitsPerElement());
    _buffer = new int[mvColumnMetadata.getMaxNumberOfMultiValues()];

    // Dictionary
    _intDictionary = new IntDictionary(segmentReader.getIndexFor(INT_COLUMN_NAME, StandardIndexes.dictionary()),
        segmentMetadata.getColumnMetadataFor(INT_COLUMN_NAME).getCardinality());
    _longDictionary = new LongDictionary(segmentReader.getIndexFor(LONG_COLUMN_NAME, StandardIndexes.dictionary()),
        segmentMetadata.getColumnMetadataFor(LONG_COLUMN_NAME).getCardinality());
    _floatDictionary = new FloatDictionary(segmentReader.getIndexFor(FLOAT_COLUMN_NAME, StandardIndexes.dictionary()),
        segmentMetadata.getColumnMetadataFor(FLOAT_COLUMN_NAME).getCardinality());
    _doubleDictionary =
        new DoubleDictionary(segmentReader.getIndexFor(DOUBLE_COLUMN_NAME, StandardIndexes.dictionary()),
            segmentMetadata.getColumnMetadataFor(DOUBLE_COLUMN_NAME).getCardinality());
    ColumnMetadata stringColumnMetadata = segmentMetadata.getColumnMetadataFor(STRING_COLUMN_NAME);
    _stringDictionary =
        new StringDictionary(segmentReader.getIndexFor(STRING_COLUMN_NAME, StandardIndexes.dictionary()),
            stringColumnMetadata.getCardinality(), stringColumnMetadata.getColumnMaxLength());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int fixedBitSingleValueReader()
      throws IOException {
    try (ForwardIndexReaderContext readerContext = _fixedBitSingleValueReader.createContext()) {
      int ret = 0;
      for (int i = 0; i < _numDocs; i++) {
        ret += _fixedBitSingleValueReader.getDictId(i, readerContext);
      }
      return ret;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int sortedForwardIndexReaderSequential() {
    try (SortedIndexReaderImpl.Context readerContext = _sortedForwardIndexReader.createContext()) {
      int ret = 0;
      for (int i = 0; i < _numDocs; i++) {
        ret += _sortedForwardIndexReader.getDictId(i, readerContext);
      }
      return ret;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int sortedForwardIndexReaderRandom() {
    try (SortedIndexReaderImpl.Context readerContext = _sortedForwardIndexReader.createContext()) {
      int ret = 0;
      for (int i = 0; i < _numDocs; i++) {
        ret += _sortedForwardIndexReader.getDictId(RANDOM.nextInt(_numDocs), readerContext);
      }
      return ret;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int fixedBitMultiValueReaderSequential() {
    try (FixedBitMVForwardIndexReader.Context readerContext = _fixedBitMultiValueReader.createContext()) {
      int ret = 0;
      for (int i = 0; i < _numDocs; i++) {
        ret += _fixedBitMultiValueReader.getDictIdMV(i, _buffer, readerContext);
      }
      return ret;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int fixedBitMultiValueReaderRandom() {
    try (FixedBitMVForwardIndexReader.Context readerContext = _fixedBitMultiValueReader.createContext()) {
      int ret = 0;
      for (int i = 0; i < _numDocs; i++) {
        ret += _fixedBitMultiValueReader.getDictIdMV(RANDOM.nextInt(_numDocs), _buffer, readerContext);
      }
      return ret;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public double intDictionary() {
    int length = _intDictionary.length();
    int ret = 0;
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int value = _intDictionary.getIntValue(RANDOM.nextInt(length));
      ret += _intDictionary.indexOf(Integer.toString(value));
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int longDictionary() {
    int length = _longDictionary.length();
    int ret = 0;
    for (int i = 0; i < NUM_ROUNDS; i++) {
      long value = _longDictionary.getLongValue(RANDOM.nextInt(length));
      ret += _longDictionary.indexOf(Long.toString(value));
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int floatDictionary() {
    int length = _floatDictionary.length();
    int ret = 0;
    for (int i = 0; i < NUM_ROUNDS; i++) {
      float value = _floatDictionary.getFloatValue(RANDOM.nextInt(length));
      ret += _floatDictionary.indexOf(Float.toString(value));
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int doubleDictionary() {
    int length = _doubleDictionary.length();
    int ret = 0;
    for (int i = 0; i < NUM_ROUNDS; i++) {
      double value = _doubleDictionary.getDoubleValue(RANDOM.nextInt(length));
      ret += _doubleDictionary.indexOf(Double.toString(value));
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int stringDictionary() {
    int length = _stringDictionary.length();
    int ret = 0;
    int[] dictIds = new int[NUM_ROUNDS];
    for (int i = 0; i < NUM_ROUNDS; i++) {
      int dictId = RANDOM.nextInt(length);
      String value = _stringDictionary.getStringValue(dictId);
      ret += _stringDictionary.indexOf(value);
      dictIds[i] = dictId;
    }
    String[] outValues = new String[NUM_ROUNDS];
    _stringDictionary.readStringValues(dictIds, NUM_ROUNDS, outValues);
    for (int i = 0; i < NUM_ROUNDS; i++) {
      ret += outValues[0].length();
    }
    return ret;
  }

  @TearDown
  public void tearDown()
      throws IOException {
    _fixedBitSingleValueReader.close();
    _sortedForwardIndexReader.close();
    _fixedBitMultiValueReader.close();
    _intDictionary.close();
    _longDictionary.close();
    _floatDictionary.close();
    _doubleDictionary.close();
    _stringDictionary.close();

    FileUtils.deleteQuietly(TEMP_DIR);
  }

  public static void main(String[] args)
      throws Exception {
    Options opt =
        new OptionsBuilder().include(BenchmarkOfflineIndexReader.class.getSimpleName()).warmupTime(TimeValue.seconds(5))
            .warmupIterations(2).measurementTime(TimeValue.seconds(5)).measurementIterations(3).forks(1).build();

    new Runner(opt).run();
  }
}
