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
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.operator.query.JsonIndexGroupByOperator;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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


/**
 * JMH benchmark for {@code GROUP BY jsonExtractIndex(...) + COUNT(*)} comparing the dictionary-scan path
 * ({@link JsonIndexGroupByOperator}) against the row-by-row baseline ({@link GroupByOperator}).
 *
 * <p>The benchmark exists to settle the {@code SELECTIVITY_THRESHOLD} constant in
 * {@link JsonIndexGroupByOperator}. We sweep two dimensions:
 * <ul>
 *   <li>{@code D} — path cardinality (distinct values seen at the GROUP BY path)</li>
 *   <li>{@code M} — matched-doc fraction of the segment (WHERE selectivity)</li>
 * </ul>
 * For each combination we time both operators, with no planner gating, and report the crossover point.
 *
 * <p>Both operators are constructed directly (not via {@code GroupByPlanNode}) so the selectivity gate does not
 * influence the measurement — we want the raw cost of each path at every (D, M) cell.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkJsonIndexGroupByCount {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkJsonIndexGroupByCount");
  private static final String TABLE_NAME = "myTable";
  private static final String SEGMENT_NAME = "jsonIndexGroupByCountSegment";
  private static final String ID_COLUMN = "id";
  private static final String TAGS_COLUMN = "tags";
  // Same-column filter that selects a subset of docs based on the synthetic "match" field embedded in the JSON.
  // We use a cross-path filter so the index lookup for the GROUP BY path runs unfiltered, isolating the cost
  // model of "dictionary scan of D entries vs row-by-row scan of M docs".
  private static final String FILTER_CLAUSE =
      "WHERE JSON_MATCH(tags, '\"$.matchKey\" = ''yes''')";
  private static final String BASE_QUERY =
      "SELECT JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING') AS k, COUNT(*) FROM " + TABLE_NAME
          + " " + FILTER_CLAUSE + " GROUP BY k LIMIT 1000000";

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(TABLE_NAME)
      .setJsonIndexColumns(Collections.singletonList(TAGS_COLUMN))
      .build();

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(TABLE_NAME)
      .addSingleValueDimension(ID_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(TAGS_COLUMN, FieldSpec.DataType.JSON)
      .build();

  @Param({"500000"})
  int _numRows;

  // Sweep path cardinality across two orders of magnitude. The crossover where index path stops winning sits
  // somewhere in this range for typical JSON sizes.
  @Param({"10", "100", "1000", "10000", "100000", "500000"})
  int _pathCardinality;

  // Matched-doc fraction: 1% (very selective), 10%, 50%, 100% (no filter).
  @Param({"0.01", "0.1", "0.5", "1.0"})
  double _matchedFraction;

  private IndexSegment _indexSegment;
  private SegmentContext _segmentContext;
  private QueryContext _queryContext;
  private BaseFilterOperator _filterOperator;
  private AggregationFunctionUtils.AggregationInfo _aggregationInfo;
  private int _expectedDistinctGroupCount;

  @Setup(Level.Trial)
  public void setup()
      throws Exception {
    Preconditions.checkState(_pathCardinality <= _numRows,
        "pathCardinality (%s) must not exceed numRows (%s)", _pathCardinality, _numRows);

    FileUtils.deleteQuietly(INDEX_DIR);
    buildSegment();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    Preconditions.checkState(_indexSegment.getDataSource(TAGS_COLUMN, SCHEMA).getJsonIndex() != null,
        "Loaded segment must expose JSON index on %s", TAGS_COLUMN);
    _segmentContext = new SegmentContext(_indexSegment);

    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(BASE_QUERY);
    _queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    _queryContext.setSchema(SCHEMA);

    FilterPlanNode filterPlanNode = new FilterPlanNode(_segmentContext, _queryContext);
    _filterOperator = filterPlanNode.run();
    _aggregationInfo = AggregationFunctionUtils.buildAggregationInfo(_segmentContext, _queryContext,
        _queryContext.getAggregationFunctions(), _queryContext.getFilter(), _filterOperator,
        filterPlanNode.getPredicateEvaluators());

    // Cross-validate the two operators on a sanity run before measuring.
    int baselineGroups = numGroupsFromBaseline();
    int indexGroups = numGroupsFromIndex();
    Preconditions.checkState(baselineGroups == indexGroups,
        "Baseline and index operators disagreed on group count. pathCardinality=%s matchedFraction=%s "
            + "baseline=%s index=%s", _pathCardinality, _matchedFraction, baselineGroups, indexGroups);
    _expectedDistinctGroupCount = baselineGroups;
  }

  @Benchmark
  public int baselineGroupByOperator() {
    return numGroupsFromBaseline();
  }

  @Benchmark
  public int jsonIndexGroupByOperator() {
    return numGroupsFromIndex();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private int numGroupsFromBaseline() {
    GroupByOperator operator = new GroupByOperator(_queryContext, _aggregationInfo,
        _indexSegment.getSegmentMetadata().getTotalDocs());
    return countGroups(operator);
  }

  private int numGroupsFromIndex() {
    JsonIndexGroupByOperator operator = new JsonIndexGroupByOperator(_indexSegment, _segmentContext, _queryContext,
        _filterOperator);
    return countGroups(operator);
  }

  private static int countGroups(Operator<GroupByResultsBlock> operator) {
    GroupByResultsBlock block = operator.nextBlock();
    if (block.getIntermediateRecords() != null) {
      return block.getIntermediateRecords().size();
    }
    if (block.getAggregationGroupByResult() != null) {
      return block.getAggregationGroupByResult().getNumGroups();
    }
    return 0;
  }

  private void buildSegment()
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(createData())) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private LazyDataGenerator createData() {
    // Round the matched-doc count so that an integer number of docs match. The "matchKey" field flips between
    // "yes" and "no" based on whether the row index falls below the matched-doc threshold.
    int matchedDocs = (int) Math.round(_numRows * _matchedFraction);
    return new LazyDataGenerator() {
      @Override
      public int size() {
        return _numRows;
      }

      @Override
      public GenericRow next(GenericRow row, int index) {
        int instance = index % _pathCardinality;
        String matchValue = index < matchedDocs ? "yes" : "no";
        row.putValue(ID_COLUMN, index);
        row.putValue(TAGS_COLUMN,
            "{\"instance\":\"instance-" + instance + "\",\"matchKey\":\"" + matchValue + "\"}");
        return row;
      }

      @Override
      public void rewind() {
      }
    };
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkJsonIndexGroupByCount.class.getSimpleName())
        .build()).run();
  }
}
