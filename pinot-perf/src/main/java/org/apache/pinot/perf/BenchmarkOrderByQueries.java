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
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
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
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class BenchmarkOrderByQueries extends BaseQueriesTest {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkOrderByQueries.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "FilteredAggregationsTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String FIRST_SEGMENT_NAME = "firstTestSegment";
  private static final String SECOND_SEGMENT_NAME = "secondTestSegment";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String SORTED_COL_NAME = "SORTED_COL";
  private static final String RAW_INT_COL_NAME = "RAW_INT_COL";
  private static final String RAW_STRING_COL_NAME = "RAW_STRING_COL";
  private static final String NO_INDEX_INT_COL_NAME = "NO_INDEX_INT_COL";
  private static final String NO_INDEX_STRING_COL = "NO_INDEX_STRING_COL";
  private static final String LOW_CARDINALITY_STRING_COL = "LOW_CARDINALITY_STRING_COL";

  @Param("1500000")
  private int _numRows;
  @Param({"naive", "null"})
  private String _orderByAlgorithm;
  @Param({"EXP(0.5)"})
  String _scenario;
  @Param({"1", "1000"})
  int _primaryRepetitions;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private LongSupplier _supplier;

  @Setup
  public void setUp()
      throws Exception {
    _supplier = Distribution.createLongSupplier(42, _scenario);
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment(FIRST_SEGMENT_NAME);
    buildSegment(SECOND_SEGMENT_NAME);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();

    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(INT_COL_NAME);
    invertedIndexCols.add(LOW_CARDINALITY_STRING_COL);

    indexLoadingConfig.setRangeIndexColumns(invertedIndexCols);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);

    ImmutableSegment firstImmutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, FIRST_SEGMENT_NAME), indexLoadingConfig);
    ImmutableSegment secondImmutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SECOND_SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = firstImmutableSegment;
    _indexSegments = Arrays.asList(firstImmutableSegment, secondImmutableSegment);
  }

  @TearDown
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

    FileUtils.deleteQuietly(INDEX_DIR);
    EXECUTOR_SERVICE.shutdownNow();
  }

  private List<GenericRow> createTestData(int numRows) {
    Map<Integer, String> strings = new HashMap<>();
    List<GenericRow> rows = new ArrayList<>();
    String[] lowCardinalityValues = IntStream.range(0, 10).mapToObj(i -> "value" + i)
        .toArray(String[]::new);
    for (int i = 0; i < numRows; i += _primaryRepetitions) {
      for (int j = 0; j < _primaryRepetitions; j++) {
        GenericRow row = new GenericRow();
        row.putValue(SORTED_COL_NAME, i);
        row.putValue(INT_COL_NAME, (int) _supplier.getAsLong());
        row.putValue(NO_INDEX_INT_COL_NAME, (int) _supplier.getAsLong());
        row.putValue(RAW_INT_COL_NAME, (int) _supplier.getAsLong());
        row.putValue(RAW_STRING_COL_NAME, strings.computeIfAbsent(
            (int) _supplier.getAsLong(), k -> UUID.randomUUID().toString()));
        row.putValue(NO_INDEX_STRING_COL, row.getValue(RAW_STRING_COL_NAME));
        row.putValue(LOW_CARDINALITY_STRING_COL, lowCardinalityValues[(i + j) % lowCardinalityValues.length]);
        rows.add(row);
      }
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
  public BrokerResponseNative sortedAsc() {
    return getBrokerResponse(
        "SELECT SORTED_COL "
            + "FROM MyTable "
            + "ORDER BY SORTED_COL ASC "
            + "LIMIT 1052 "
            + "option(orderByAlgorithm=" + _orderByAlgorithm + ")");
  }
  @Benchmark
  public BrokerResponseNative sortedAscPartially() {
    return getBrokerResponse(
        "SELECT SORTED_COL "
            + "FROM MyTable "
            + "ORDER BY SORTED_COL ASC, LOW_CARDINALITY_STRING_COL "
            + "LIMIT 1052 "
            + "option(orderByAlgorithm=" + _orderByAlgorithm + ")");
  }

  @Benchmark
  public BrokerResponseNative sortedDesc() {
    return getBrokerResponse(
        "SELECT SORTED_COL "
            + "FROM MyTable "
            + "ORDER BY SORTED_COL DESC "
            + "LIMIT 1052 "
            + "option(orderByAlgorithm=" + _orderByAlgorithm + ")");
  }

  @Benchmark
  public BrokerResponseNative sortedDescPartially() {
    return getBrokerResponse(
        "SELECT SORTED_COL "
            + "FROM MyTable "
            + "ORDER BY SORTED_COL DESC, LOW_CARDINALITY_STRING_COL "
            + "LIMIT 1052 "
            + "option(orderByAlgorithm=" + _orderByAlgorithm + ")");
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
