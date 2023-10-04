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
public class BenchmarkQueries extends BaseQueriesTest {

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkQueries.class.getSimpleName());
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

  public static final String FILTERED_QUERY = "SELECT SUM(INT_COL) FILTER(WHERE INT_COL > 123 AND INT_COL < 599999),"
      + "MAX(INT_COL) FILTER(WHERE INT_COL > 123 AND INT_COL < 599999) "
      + "FROM MyTable WHERE NO_INDEX_INT_COL > 5 AND NO_INDEX_INT_COL < 1499999";

  public static final String NON_FILTERED_QUERY = "SELECT SUM("
      + "CASE "
      + "WHEN (INT_COL > 123 AND INT_COL < 599999) THEN INT_COL "
      + "ELSE 0 "
      + "END) AS total_sum,"
      + "MAX("
      + "CASE "
      + "WHEN (INT_COL > 123 AND INT_COL < 599999) THEN INT_COL "
      + "ELSE 0 "
      + "END) AS total_avg "
      + "FROM MyTable WHERE NO_INDEX_INT_COL > 5 AND NO_INDEX_INT_COL < 1499999";

  public static final String SUM_QUERY = "SELECT SUM(RAW_INT_COL) FROM MyTable";

  public static final String MULTI_GROUP_BY_WITH_RAW_QUERY = "SELECT RAW_INT_COL,INT_COL,COUNT(*) FROM MyTable "
      + "GROUP BY RAW_INT_COL,INT_COL";

  public static final String MULTI_GROUP_BY_WITH_RAW_QUERY_2 = "SELECT RAW_STRING_COL,RAW_INT_COL,INT_COL,COUNT(*) "
      + "FROM MyTable GROUP BY RAW_STRING_COL,RAW_INT_COL,INT_COL";

  public static final String NO_INDEX_LIKE_QUERY = "SELECT RAW_INT_COL FROM MyTable "
      + "WHERE NO_INDEX_STRING_COL LIKE '%foo%'";

  public static final String FILTERING_BITMAP_SCAN_QUERY = "SELECT RAW_INT_COL FROM MyTable "
      + "WHERE INT_COL = 1 AND RAW_INT_COL IN (0, 1, 2)";

  public static final String MULTI_GROUP_BY_ORDER_BY = "SELECT NO_INDEX_STRING_COL,INT_COL,COUNT(*) "
      + "FROM MyTable GROUP BY NO_INDEX_STRING_COL,INT_COL ORDER BY NO_INDEX_STRING_COL, INT_COL ASC";

  public static final String MULTI_GROUP_BY_ORDER_BY_LOW_HIGH = "SELECT "
      + "NO_INDEX_STRING_COL,LOW_CARDINALITY_STRING_COL,COUNT(*) "
      + "FROM MyTable GROUP BY LOW_CARDINALITY_STRING_COL,NO_INDEX_STRING_COL "
      + "ORDER BY LOW_CARDINALITY_STRING_COL, NO_INDEX_STRING_COL ASC";

  public static final String TIME_GROUP_BY = "select count(*), "
      + "year(INT_COL) as y, month(INT_COL) as m "
      + "from MyTable group by y, m";

  public static final String COUNT_OVER_BITMAP_INDEX_IN = "SELECT COUNT(*) FROM MyTable "
      + "WHERE INT_COL IN (0, 1, 2, 3, 4, 5, 7, 9, 10)";

  public static final String COUNT_OVER_BITMAP_INDEX_EQUALS = "SELECT COUNT(*) FROM MyTable "
      + "WHERE LOW_CARDINALITY_STRING_COL = 'value1'";

  public static final String COUNT_OVER_BITMAP_INDEXES = "SELECT COUNT(*) FROM MyTable "
      + "WHERE INT_COL IN (0, 1, 2, 3, 4, 5, 7, 9, 10) "
      + "AND LOW_CARDINALITY_STRING_COL = 'value1' ";

  public static final String COUNT_OVER_BITMAP_AND_SORTED_INDEXES = "SELECT COUNT(*) FROM MyTable "
      + "WHERE INT_COL IN (0, 1, 2, 3, 4, 5, 7, 9, 10) "
      + "AND LOW_CARDINALITY_STRING_COL = 'value1' "
      + "AND SORTED_COL BETWEEN 10 and 50";

  public static final String RAW_COLUMN_SUMMARY_STATS = "SELECT "
      + "MIN(RAW_INT_COL), MAX(RAW_INT_COL), COUNT(*) "
      + "FROM MyTable";

  public static final String STARTREE_SUM_QUERY = "SELECT INT_COL, SORTED_COL, SUM(RAW_INT_COL) from MyTable "
      + "GROUP BY INT_COL, SORTED_COL ORDER BY SORTED_COL, INT_COL ASC";

  public static final String STARTREE_FILTER_QUERY = "SELECT INT_COL, SORTED_COL, SUM(RAW_INT_COL) FROM MyTable "
      + "WHERE INT_COL = 0 and SORTED_COL = 1"
      + "GROUP BY INT_COL, SORTED_COL ORDER BY SORTED_COL, INT_COL ASC";

  public static final String FILTERING_SCAN_QUERY = "SELECT SUM(RAW_INT_COL) FROM MyTable "
      + "WHERE RAW_INT_COL BETWEEN 1 AND 10";

  @Param("1500000")
  private int _numRows;
  @Param({"EXP(0.001)", "EXP(0.5)", "EXP(0.999)"})
  String _scenario;
  @Param({
      MULTI_GROUP_BY_WITH_RAW_QUERY, MULTI_GROUP_BY_WITH_RAW_QUERY_2, FILTERED_QUERY, NON_FILTERED_QUERY,
      SUM_QUERY, NO_INDEX_LIKE_QUERY, MULTI_GROUP_BY_ORDER_BY, MULTI_GROUP_BY_ORDER_BY_LOW_HIGH, TIME_GROUP_BY,
      RAW_COLUMN_SUMMARY_STATS, COUNT_OVER_BITMAP_INDEX_IN, COUNT_OVER_BITMAP_INDEXES,
      COUNT_OVER_BITMAP_AND_SORTED_INDEXES, COUNT_OVER_BITMAP_INDEX_EQUALS, STARTREE_SUM_QUERY, STARTREE_FILTER_QUERY,
      FILTERING_BITMAP_SCAN_QUERY, FILTERING_SCAN_QUERY
  })
  String _query;
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
