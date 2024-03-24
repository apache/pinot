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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 30)
@Measurement(iterations = 5, time = 30)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkStringVarLengthDictionary {
  private static final File TMP_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkStringVarLengthDictionary");
  private static final String COLUMN_NAME = "test";

  private static final long RANDOM_SEED = 1234567890L;
  private static final int DICTIONARY_LENGTH = 1_000_000;
  private static final int NUM_TEST_LOOKUP = 1_000_000;
  private static final boolean USE_FIXED_SIZE_STRING = true;
  private static final int MAX_STRING_LENGTH = 100;

  private String[] _inputData;
  private int[] _randomReadOrder;
  private ImmutableSegment _immutableSegmentWithFixedDictionary;
  private ImmutableSegment _immutableSegmentWithVarLengthDictionary;

  @Setup
  public void setUp()
      throws Exception {
    // Create directories for index
    String segmentName = "perfTestSegment" + System.currentTimeMillis();
    File fixedLengthIndexDir = new File(TMP_DIR, segmentName);

    String varSegmentName = "perfTestSegmentVarLength" + System.currentTimeMillis();
    File varLengthIndexDir = new File(TMP_DIR, varSegmentName);

    // Create the schema and table config
    Schema schema = new Schema();
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, true);
    schema.addField(fieldSpec);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();

    // Generate the random read order
    _randomReadOrder = new int[NUM_TEST_LOOKUP];
    Random random = new Random(RANDOM_SEED);
    for (int j = 0; j < NUM_TEST_LOOKUP; j++) {
      _randomReadOrder[j] = random.nextInt(DICTIONARY_LENGTH);
    }

    // Generate the sample data to be written.
    random = new Random(RANDOM_SEED);
    List<GenericRow> rows = new ArrayList<>(DICTIONARY_LENGTH);
    Set<String> uniqueStrings = new HashSet<>();
    _inputData = new String[DICTIONARY_LENGTH];
    int i = 0;
    while (i < DICTIONARY_LENGTH) {
      String randomString = RandomStringUtils.randomAlphanumeric(
          USE_FIXED_SIZE_STRING ? MAX_STRING_LENGTH : (1 + random.nextInt(MAX_STRING_LENGTH)));
      if (uniqueStrings.contains(randomString)) {
        continue;
      } else {
        uniqueStrings.add(randomString);
      }
      _inputData[i] = randomString;
      GenericRow genericRow = new GenericRow();
      genericRow.putValue(COLUMN_NAME, _inputData[i++]);
      rows.add(genericRow);
    }

    // Generate Segment with fixed dictionary
    SegmentGeneratorConfig fixedDictionarySegmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    fixedDictionarySegmentGeneratorConfig.setOutDir(fixedLengthIndexDir.getParent());
    fixedDictionarySegmentGeneratorConfig.setFormat(FileFormat.AVRO);
    fixedDictionarySegmentGeneratorConfig.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(fixedDictionarySegmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();

    // Update table config for variable length string dictionary
    ArrayList<String> varLengthDictionaryColumns = new ArrayList<>();
    varLengthDictionaryColumns.add(COLUMN_NAME);
    tableConfig.getIndexingConfig().setVarLengthDictionaryColumns(varLengthDictionaryColumns);

    // Generate Segment with var length dictionary
    SegmentGeneratorConfig varLengthDictionarySegmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    varLengthDictionarySegmentGeneratorConfig.setOutDir(varLengthIndexDir.getParent());
    varLengthDictionarySegmentGeneratorConfig.setFormat(FileFormat.AVRO);
    varLengthDictionarySegmentGeneratorConfig.setSegmentName(varSegmentName);
    driver = new SegmentIndexCreationDriverImpl();
    driver.init(varLengthDictionarySegmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();

    _immutableSegmentWithFixedDictionary = ImmutableSegmentLoader.load(fixedLengthIndexDir, ReadMode.mmap);
    _immutableSegmentWithVarLengthDictionary = ImmutableSegmentLoader.load(varLengthIndexDir, ReadMode.mmap);
  }

  @TearDown
  public void tearDown()
      throws Exception {
    _immutableSegmentWithFixedDictionary.destroy();
    _immutableSegmentWithVarLengthDictionary.destroy();
    FileUtils.deleteDirectory(TMP_DIR);
  }

  @Benchmark
  public void fixedStringDictionaryIndexOf(Blackhole bh) {
    Dictionary fixedLengthStringDictionary = _immutableSegmentWithFixedDictionary.getDictionary(COLUMN_NAME);
    for (int i = 0; i < _randomReadOrder.length; i++) {
      bh.consume(fixedLengthStringDictionary.indexOf(_inputData[_randomReadOrder[i]]));
    }
  }

  @Benchmark
  public void fixedStringDictionaryGet(Blackhole bh) {
    Dictionary fixedLengthStringDictionary = _immutableSegmentWithFixedDictionary.getDictionary(COLUMN_NAME);
    for (int i = 0; i < _randomReadOrder.length; i++) {
      bh.consume(fixedLengthStringDictionary.get(_randomReadOrder[i]));
    }
  }

  @Benchmark
  public void varLengthStringDictionaryIndexOf(Blackhole bh) {
    Dictionary varLengthStringDictionary = _immutableSegmentWithVarLengthDictionary.getDictionary(COLUMN_NAME);
    for (int i = 0; i < _randomReadOrder.length; i++) {
      bh.consume(varLengthStringDictionary.indexOf(_inputData[_randomReadOrder[i]]));
    }
  }

  @Benchmark
  public void varLengthStringDictionaryGet(Blackhole bh) {
    Dictionary varLengthStringDictionary = _immutableSegmentWithVarLengthDictionary.getDictionary(COLUMN_NAME);
    for (int i = 0; i < _randomReadOrder.length; i++) {
      bh.consume(varLengthStringDictionary.get(_randomReadOrder[i]));
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkStringVarLengthDictionary.class.getSimpleName());
    new Runner(opt.build()).run();
  }
}
