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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.query.pruner.ColumnValueSegmentPruner;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
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
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
public class BenchmarkColumnValueSegmentPruner {

  public static final String QUERY_1 = "SELECT * FROM MyTable WHERE SORTED_COL IN (1, 2, 3, 4)";

  @Param({"10"})
  private int _numRows;
  @Param({"10", "100", "1000"})
  private int _numSegments;

  private String _query = QUERY_1;
  String _scenario = "EXP(0.5)";
  private List<IndexSegment> _indexSegments;
  private LongSupplier _supplier;
  private ColumnValueSegmentPruner _pruner;
  private QueryContext _queryContext;

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkColumnValueSegmentPruner.class.getSimpleName());
    if (args.length > 0 && args[0].equals("jfr")) {
      opt = opt.addProfiler(JavaFlightRecorderProfiler.class)
          .jvmArgsAppend("-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints");
    }
    new Runner(opt.build()).run();
  }

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkServerSegmentPruner");
  private static final String TABLE_NAME = "MyTable";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String SORTED_COL_NAME = "SORTED_COL";
  private static final String RAW_INT_COL_NAME = "RAW_INT_COL";
  private static final String RAW_STRING_COL_NAME = "RAW_STRING_COL";
  private static final String NO_INDEX_INT_COL_NAME = "NO_INDEX_INT_COL";
  private static final String NO_INDEX_STRING_COL = "NO_INDEX_STRING_COL";
  private static final String LOW_CARDINALITY_STRING_COL = "LOW_CARDINALITY_STRING_COL";


  @Setup
  public void setUp()
      throws Exception {
    _supplier = Distribution.createLongSupplier(42, _scenario);
    FileUtils.deleteQuietly(INDEX_DIR);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();

    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(INT_COL_NAME);
    invertedIndexCols.add(LOW_CARDINALITY_STRING_COL);

    Map<String, BloomFilterConfig> bloomFilterConfigMap = new HashMap<>();
    bloomFilterConfigMap.put(SORTED_COL_NAME, new BloomFilterConfig(BloomFilterConfig.DEFAULT_FPP, 10000, false));

    indexLoadingConfig.setRangeIndexColumns(invertedIndexCols);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    indexLoadingConfig.setBloomFilterConfigs(bloomFilterConfigMap);

    _indexSegments = new ArrayList<>();
    for (int i = 0; i < _numSegments; i++) {
      String name = "segment_" + i;
      buildSegment(name);
      _indexSegments.add(ImmutableSegmentLoader.load(new File(INDEX_DIR, name), indexLoadingConfig));
    }

    _pruner = new ColumnValueSegmentPruner();
    _pruner.init(new PinotConfiguration());
    _queryContext = QueryContextConverterUtils.getQueryContext(_query);
  }

  @TearDown
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private List<GenericRow> createTestData(int numRows) {
    Map<Integer, String> strings = new HashMap<>();
    List<GenericRow> rows = new ArrayList<>();
    String[] lowCardinalityValues = IntStream.range(0, 10).mapToObj(i -> "value" + i)
        .toArray(String[]::new);
    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putValue(SORTED_COL_NAME, numRows - i);
      row.putValue(INT_COL_NAME, (int) _supplier.getAsLong());
      row.putValue(NO_INDEX_INT_COL_NAME, (int) _supplier.getAsLong());
      row.putValue(RAW_INT_COL_NAME, (int) _supplier.getAsLong());
      row.putValue(RAW_STRING_COL_NAME,
          strings.computeIfAbsent((int) _supplier.getAsLong(), k -> UUID.randomUUID().toString()));
      row.putValue(NO_INDEX_STRING_COL, row.getValue(RAW_STRING_COL_NAME));
      row.putValue(LOW_CARDINALITY_STRING_COL, lowCardinalityValues[i % lowCardinalityValues.length]);
      rows.add(row);
    }
    return rows;
  }

  private void buildSegment(String segmentName)
      throws Exception {
    List<GenericRow> rows = createTestData(_numRows);
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Collections.singletonList(INT_COL_NAME))
        .setFieldConfigList(fieldConfigs)
        .setNoDictionaryColumns(Arrays.asList(RAW_INT_COL_NAME, RAW_STRING_COL_NAME))
        .setSortedColumn(SORTED_COL_NAME)
        .setVarLengthDictionaryColumns(Collections.singletonList(SORTED_COL_NAME))
        .setBloomFilterColumns(Collections.singletonList(SORTED_COL_NAME))
        .setStarTreeIndexConfigs(Collections.singletonList(new StarTreeIndexConfig(
            Arrays.asList(SORTED_COL_NAME, INT_COL_NAME), null, Collections.singletonList(
                new AggregationFunctionColumnPair(AggregationFunctionType.SUM, RAW_INT_COL_NAME).toColumnName()),
            null,
            Integer.MAX_VALUE)))
        .build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(SORTED_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(NO_INDEX_INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(RAW_INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(RAW_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(NO_INDEX_STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LOW_CARDINALITY_STRING_COL, FieldSpec.DataType.STRING)
        .build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Benchmark
  public List<IndexSegment> query() {
    return _pruner.prune(_indexSegments, _queryContext);
  }
}
