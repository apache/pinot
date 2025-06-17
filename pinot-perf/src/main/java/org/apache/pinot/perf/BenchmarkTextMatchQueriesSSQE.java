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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkTextMatchQueriesSSQE extends BaseQueriesTest {

  public static void main(String[] args)
      throws Exception {
    Options opt =
        new OptionsBuilder().include(BenchmarkTextMatchQueriesSSQE.class.getSimpleName())
            //.addProfiler(GCProfiler.class)
            .shouldDoGC(true)
            .build();
    new Runner(opt).run();
  }

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkTextMatchQueriesSSQE");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_TEMPLATE = "testSegment%d";
  private static final String SC_STR_COL_PREFIX = "SC_STR_COL_";
  private static final String MC_STR_COL_PREFIX = "MC_STR_COL_";
  private static final String TIMESTAMP_COL = "TSTMP_COL";

  private static final int STR_COL_COUNT = 5;

  private static List<FieldConfig> _fieldConfigs;
  private static List<String> _singleColIdxCols;
  private static List<String> _multiColIdxCols;
  private static TableConfig _tableConfig;
  private static Schema _schema;

  // benchmark with x columns, built either per-column or per-segment
  public static final String MC_TEXT_MATCH_1 =
      "SELECT count(*) "
          + "from MyTable "
          + "WHERE TEXT_MATCH(MC_STR_COL_0, 'pinot OR java') ";

  public static final String MC_TEXT_MATCH_2 =
      "SELECT count(*) "
          + "from MyTable "
          + "WHERE TEXT_MATCH(MC_STR_COL_0, 'pinot java') "
          + "OR    TEXT_MATCH(MC_STR_COL_1, 'distributed database')";

  public static final String MC_TEXT_MATCH_5 =
      "SELECT count(*) "
          + "from MyTable "
          + "WHERE TEXT_MATCH(MC_STR_COL_0, 'pinot') "
          + "OR    TEXT_MATCH(MC_STR_COL_1, 'java') "
          + "OR    TEXT_MATCH(MC_STR_COL_2, 'database') "
          + "OR    TEXT_MATCH(MC_STR_COL_3, 'distributed') "
          + "OR    TEXT_MATCH(MC_STR_COL_4, 'multi-tenant') ";

  public static final String SC_TEXT_MATCH_1 =
      "SELECT count(*) "
          + "from MyTable "
          + "WHERE TEXT_MATCH(SC_STR_COL_0, 'pinot OR java') ";

  public static final String SC_TEXT_MATCH_2 =
      "SELECT count(*) "
          + "from MyTable "
          + "WHERE TEXT_MATCH(SC_STR_COL_0, 'pinot java') "
          + "OR    TEXT_MATCH(SC_STR_COL_1, 'distributed database')";

  public static final String SC_TEXT_MATCH_5 =
      "SELECT count(*) "
          + "from MyTable "
          + "WHERE TEXT_MATCH(SC_STR_COL_0, 'pinot') "
          + "OR    TEXT_MATCH(SC_STR_COL_1, 'java') "
          + "OR    TEXT_MATCH(SC_STR_COL_2, 'database') "
          + "OR    TEXT_MATCH(SC_STR_COL_3, 'distributed') "
          + "OR    TEXT_MATCH(SC_STR_COL_4, 'multi-tenant') ";

  @Param({/*"1",*/ "5"/*, "10", "50"*/})
  private int _numSegments;

  @Param("1500000")
  private int _numRows;

  @Param({"EXP(0.001)" /*,"EXP(0.5)", "EXP(0.999)"*/})
  String _scenario;
  @Param({
      SC_TEXT_MATCH_1, MC_TEXT_MATCH_1,
      SC_TEXT_MATCH_2, MC_TEXT_MATCH_2,
      SC_TEXT_MATCH_5, MC_TEXT_MATCH_5
  })
  String _query;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private Distribution.DataSupplier _supplier;

  @Setup
  public void setUp()
      throws Exception {
    long start = System.currentTimeMillis();
    _supplier = Distribution.createSupplier(42, _scenario);
    FileUtils.deleteQuietly(INDEX_DIR);

    _fieldConfigs = new ArrayList<>();
    _multiColIdxCols = new ArrayList<>();
    for (int i = 0; i < STR_COL_COUNT; i++) {
      String colName = MC_STR_COL_PREFIX + i;
      _multiColIdxCols.add(colName);
      _fieldConfigs.add(new FieldConfig.Builder(colName)
          .withEncodingType(FieldConfig.EncodingType.RAW).build());
    }

    _singleColIdxCols = new ArrayList<>();
    for (int i = 0; i < STR_COL_COUNT; i++) {
      String colName = SC_STR_COL_PREFIX + i;
      _singleColIdxCols.add(colName);
      _fieldConfigs.add(new FieldConfig.Builder(colName)
          .withEncodingType(FieldConfig.EncodingType.RAW)
          .withIndexTypes(List.of(FieldConfig.IndexType.TEXT))
          .build());
    }

    List<String> noDictCols = new ArrayList<>();
    noDictCols.add(TIMESTAMP_COL);
    noDictCols.addAll(_multiColIdxCols);

    _tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setFieldConfigList(_fieldConfigs)
        .setNoDictionaryColumns(noDictCols)
        .setMultiColumnTextIndexConfig(new MultiColumnTextIndexConfig(_multiColIdxCols))
        .build();

    Schema.SchemaBuilder builder = new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME);

    for (String strCol : _multiColIdxCols) {
      builder.addSingleValueDimension(strCol, FieldSpec.DataType.STRING);
    }
    for (String strCol : _singleColIdxCols) {
      builder.addSingleValueDimension(strCol, FieldSpec.DataType.STRING);
    }

    _schema = builder.addSingleValueDimension(TIMESTAMP_COL, FieldSpec.DataType.TIMESTAMP)
        .build();

    _indexSegments = new ArrayList<>();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(_tableConfig, _schema);
    for (int i = 0; i < _numSegments; i++) {
      buildSegment(String.format(SEGMENT_NAME_TEMPLATE, i));
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, String.format(SEGMENT_NAME_TEMPLATE, i)),
          indexLoadingConfig));
    }
    _indexSegment = _indexSegments.get(0);
    long end = System.currentTimeMillis();

    System.out.println("Loading segments took " + (end - start) + " ms");
    System.gc();
  }

  @TearDown
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

    FileUtils.deleteQuietly(INDEX_DIR);
    EXECUTOR_SERVICE.shutdownNow();
  }

  static LazyDataGenerator createTestData(int numRows, Distribution.DataSupplier supplier) {
    //create data lazily to prevent OOM and speed up setup

    return new LazyDataGenerator() {
      private final Map<Integer, String> _strings = new HashMap<>();
      private Distribution.DataSupplier _supplier = supplier;
      private final StringBuilder _buffer = new StringBuilder();
      private final Random _random = new Random(1);
      private final Function<Integer, String> _function = k -> getString();
      private final int _range = 'Z' - 'A' + 1;
      private final char[] _dict = new char[_range];

      {
        for (int i = 0, max = _range - 1; i < max; i++) {
          _dict[i] = (char) ('A' + i);
        }
        _dict[_range - 1] = ' ';
      }

      private void initStrings() {
        _strings.put(1, "java");
        _strings.put(2, "database");
        _strings.put(3, "pinot");
        _strings.put(4, "distributed");
        _strings.put(5, "multi-tenant");
      }

      @Override
      public int size() {
        return numRows;
      }

      private String getString() {
        _buffer.setLength(0);
        for (int i = 0; i < 20; i++) {
          _buffer.append(_dict[_random.nextInt(_range)]);
        }
        return _buffer.toString();
      }

      @Override
      public GenericRow next(GenericRow row, int i) {
        for (String strCol : _multiColIdxCols) {
          row.putValue(strCol,
              _strings.computeIfAbsent((int) _supplier.getAsLong(), _function));
        }
        for (String strCol : _singleColIdxCols) {
          row.putValue(strCol,
              _strings.computeIfAbsent((int) _supplier.getAsLong(), _function));
        }
        //row.putValue(LOW_CARDINALITY_STRING_COL, _lowCardinalityValues[i % _lowCardinalityValues.length]);
        row.putValue(TIMESTAMP_COL, i * 1200 * 1000L);
        return null;
      }

      @Override
      public void rewind() {
        _strings.clear();
        initStrings();
        _supplier.reset();
      }
    };
  }

  private void buildSegment(String segmentName)
      throws Exception {
    LazyDataGenerator rows = createTestData(_numRows, _supplier);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    //save generator state so that other segments are not identical to this one
    _supplier.snapshot();
  }

  @Benchmark
  public BrokerResponseNative query() {
    return getBrokerResponse(_query);
  }

  @Override
  protected String getFilter() {
    return null;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }
}
