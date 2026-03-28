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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.query.DistinctOperator;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.GapfillUtils;
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
import org.apache.pinot.spi.utils.CommonConstants;
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
 * JMH benchmark for DISTINCT over scalar {@code JSON_EXTRACT_INDEX(...)} on a table that is explicitly configured with
 * a JSON index on {@code tags}. The benchmark state is intended for single-threaded JMH execution and is not
 * thread-safe.
 *
 * <p>The base sample queries are:
 * {@code SELECT DISTINCT JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING') AS tag_value FROM myTable WHERE
 * JSON_MATCH(tags, 'REGEXP_LIKE("$.instance", ''.*test.*'')')}
 * and
 * {@code SELECT DISTINCT JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING', '') AS tag_value FROM myTable WHERE
 * JSON_MATCH(tags, 'REGEXP_LIKE("$.instance", ''.*test.*'')')}.
 *
 * <p>Pinot assigns {@code SELECT DISTINCT} queries without an explicit limit a default limit of {@code 10}. The
 * benchmark validates the exact sample query for planning, then executes the same query with an explicit large limit so
 * the full matching distinct set is measured and compared across operators.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkJsonIndexDistinct {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkJsonIndexDistinct");
  private static final String TABLE_NAME = "myTable";
  private static final String SEGMENT_NAME = "jsonIndexDistinctSegment";
  private static final String ID_COLUMN = "id";
  private static final String TAGS_COLUMN = "tags";
  private static final String JSON_INDEX_DISTINCT_OPERATOR_NAME = "JsonIndexDistinctOperator";
  private static final String SAME_PATH_FILTER =
      "WHERE JSON_MATCH(tags, 'REGEXP_LIKE(\"$.instance\", ''.*test.*'')')";
  private static final String EXTRA_FILTER =
      "WHERE JSON_MATCH(tags, 'REGEXP_LIKE(\"$.instance\", ''.*test.*'')') "
          + "AND JSON_MATCH(tags, '\"$.cluster\" = ''cluster-0''')";
  private static final String THREE_ARG_SAMPLE_QUERY =
      "SELECT DISTINCT JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING') AS tag_value "
          + "FROM myTable " + SAME_PATH_FILTER;
  private static final String FOUR_ARG_SAMPLE_QUERY =
      "SELECT DISTINCT JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING', '') AS tag_value "
          + "FROM myTable " + SAME_PATH_FILTER;
  private static final String THREE_ARG_EXTRA_FILTER_SAMPLE_QUERY =
      "SELECT DISTINCT JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING') AS tag_value "
          + "FROM myTable " + EXTRA_FILTER;
  private static final String FOUR_ARG_EXTRA_FILTER_SAMPLE_QUERY =
      "SELECT DISTINCT JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING', '') AS tag_value "
          + "FROM myTable " + EXTRA_FILTER;
  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final boolean ASSERT_SPEEDUP =
      Boolean.parseBoolean(System.getProperty("pinot.perf.jsonIndexDistinct.assertSpeedup", "true"));
  private static final int REGRESSION_WARMUP_ITERATIONS = 3;
  private static final int REGRESSION_MEASURE_ITERATIONS = 7;
  private static final int REGRESSION_BATCH_SIZE = 10;

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

  @Param({"10", "100", "1000", "10000", "100000"})
  int _instanceCardinality;

  @Param({"0.01", "0.05", "0.1", "0.3", "0.5", "0.75", "0.9", "1.0"})
  double _testInstanceFraction;

  @Param({"1000000"})
  int _distinctLimit;

  @Param({"THREE_ARG", "FOUR_ARG", "THREE_ARG_EXTRA_FILTER", "FOUR_ARG_EXTRA_FILTER"})
  String _queryVariant;

  private IndexSegment _indexSegment;
  private QueryContext _baselineQueryContext;
  private QueryContext _optimizedQueryContext;
  private int _expectedDistinctCount;
  private volatile int _regressionSink;

  @Setup(Level.Trial)
  public void setup()
      throws Exception {
    boolean extraFilter = _queryVariant.contains("EXTRA_FILTER");
    Preconditions.checkState(_numRows >= _instanceCardinality,
        "Benchmark requires numRows >= instanceCardinality but got numRows=%s instanceCardinality=%s",
        _numRows, _instanceCardinality);
    int matchingInstances = Math.max(1, Math.min(_instanceCardinality,
        (int) Math.round(_instanceCardinality * _testInstanceFraction)));
    // With the extra cluster-0 filter, only instances where instanceId % 32 == 0 pass the cross-path filter.
    // Among matching instances [0, matchingInstances), those with id % 32 == 0 survive.
    _expectedDistinctCount = extraFilter
        ? (int) java.util.stream.IntStream.range(0, matchingInstances).filter(i -> i % 32 == 0).count()
        : matchingInstances;
    _expectedDistinctCount = Math.max(_expectedDistinctCount, extraFilter ? 0 : 1);
    Preconditions.checkState(_distinctLimit >= Math.max(_expectedDistinctCount, 1),
        "Distinct limit must cover all matching values for deterministic validation. limit=%s expectedDistinct=%s",
        _distinctLimit, _expectedDistinctCount);

    FileUtils.deleteQuietly(INDEX_DIR);
    buildSegment();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(TABLE_CONFIG, SCHEMA);
    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);

    Preconditions.checkState(TABLE_CONFIG.getIndexingConfig().getJsonIndexColumns().contains(TAGS_COLUMN),
        "Table config must include JSON index on %s", TAGS_COLUMN);
    Preconditions.checkState(_indexSegment.getDataSource(TAGS_COLUMN, SCHEMA).getJsonIndex() != null,
        "Loaded segment must expose JSON index on %s", TAGS_COLUMN);

    String sampleQuery = getSampleQuery();
    PinotQuery samplePinotQuery = CalciteSqlParser.compileToPinotQuery(sampleQuery);
    Preconditions.checkState(samplePinotQuery.getLimit() == 10,
        "Unexpected default DISTINCT limit for sample query: %s", samplePinotQuery.getLimit());

    QueryContext sampleBaselineQueryContext = compileQueryContext(sampleQuery, Collections.emptyMap());
    QueryContext sampleOptimizedQueryContext = compileQueryContext(sampleQuery,
        Collections.singletonMap(CommonConstants.Broker.Request.QueryOptionKey.USE_INDEX_BASED_DISTINCT_OPERATOR,
            "true"));
    Preconditions.checkState(planOperator(sampleBaselineQueryContext) instanceof DistinctOperator,
        "Exact %s sample query must plan to DistinctOperator without %s", _queryVariant,
        CommonConstants.Broker.Request.QueryOptionKey.USE_INDEX_BASED_DISTINCT_OPERATOR);
    assertJsonIndexDistinctOperator(
        "Exact " + _queryVariant + " sample query", planOperator(sampleOptimizedQueryContext));

    String benchmarkQuery = sampleQuery + " LIMIT " + _distinctLimit;
    _baselineQueryContext = compileQueryContext(benchmarkQuery, Collections.emptyMap());
    _optimizedQueryContext = compileQueryContext(benchmarkQuery,
        Collections.singletonMap(CommonConstants.Broker.Request.QueryOptionKey.USE_INDEX_BASED_DISTINCT_OPERATOR,
            "true"));

    Preconditions.checkState(planOperator(_baselineQueryContext) instanceof DistinctOperator,
        "Benchmark query must plan to DistinctOperator without %s",
        CommonConstants.Broker.Request.QueryOptionKey.USE_INDEX_BASED_DISTINCT_OPERATOR);
    assertJsonIndexDistinctOperator("Benchmark query", planOperator(_optimizedQueryContext));

    QueryExecution baselineExecution = executeAndCollect(_baselineQueryContext);
    QueryExecution optimizedExecution = executeAndCollect(_optimizedQueryContext);
    Preconditions.checkState(baselineExecution._values.equals(optimizedExecution._values),
        "Result mismatch. baseline=%s optimized=%s variant=%s",
        baselineExecution._values.size(), optimizedExecution._values.size(), _queryVariant);
    if (!extraFilter) {
      Preconditions.checkState(optimizedExecution._values.size() == _expectedDistinctCount,
          "Unexpected distinct count. expected=%s actual=%s variant=%s",
          _expectedDistinctCount, optimizedExecution._values.size(), _queryVariant);
      // Fast path (fully pushed down): no docs scanned, no entries scanned post-filter
      Preconditions.checkState(optimizedExecution._stats.getNumDocsScanned() == 0,
          "JsonIndexDistinctOperator should not scan docs post-filter but scanned %s",
          optimizedExecution._stats.getNumDocsScanned());
      Preconditions.checkState(optimizedExecution._stats.getNumEntriesScannedPostFilter() == 0,
          "JsonIndexDistinctOperator should not scan post-filter entries but scanned %s",
          optimizedExecution._stats.getNumEntriesScannedPostFilter());
    }
    Preconditions.checkState(baselineExecution._stats.getNumEntriesScannedPostFilter() > 0,
        "Baseline DistinctOperator should scan post-filter entries");
  }

  @Benchmark
  public int distinctOperatorPath() {
    return runQueryCount(_baselineQueryContext);
  }

  @Benchmark
  public int jsonIndexDistinctOperatorPath() {
    return runQueryCount(_optimizedQueryContext);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    try {
      verifySpeedup();
    } finally {
      if (_indexSegment != null) {
        _indexSegment.destroy();
      }
      FileUtils.deleteQuietly(INDEX_DIR);
    }
  }

  private void verifySpeedup() {
    if (!ASSERT_SPEEDUP || _queryVariant.contains("EXTRA_FILTER")) {
      return;
    }

    long optimizedNs = measureMedianNs(() -> runQueryCount(_optimizedQueryContext));
    long baselineNs = measureMedianNs(() -> runQueryCount(_baselineQueryContext));
    Preconditions.checkState(optimizedNs < baselineNs,
        "JsonIndexDistinctOperator must stay faster. rows=%s instanceCardinality=%s testFraction=%s optimized=%sns "
            + "baseline=%sns expectedDistinct=%s",
        _numRows, _instanceCardinality, _testInstanceFraction, optimizedNs, baselineNs, _expectedDistinctCount);
  }

  private void buildSegment()
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratedDataRecordReader(createTestData())) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private LazyDataGenerator createTestData() {
    int matchingInstances = _expectedDistinctCount;
    String[] tagsValues = new String[_instanceCardinality];
    for (int instanceId = 0; instanceId < _instanceCardinality; instanceId++) {
      boolean matchesFilter = instanceId < matchingInstances;
      String instanceValue = matchesFilter ? "service-test-" + instanceId : "service-prod-" + instanceId;
      tagsValues[instanceId] =
          "{\"instance\":\"" + instanceValue + "\",\"cluster\":\"cluster-" + (instanceId % 32) + "\"}";
    }

    return new LazyDataGenerator() {
      @Override
      public int size() {
        return _numRows;
      }

      @Override
      public GenericRow next(GenericRow row, int index) {
        row.putValue(ID_COLUMN, index);
        row.putValue(TAGS_COLUMN, tagsValues[index % tagsValues.length]);
        return row;
      }

      @Override
      public void rewind() {
      }
    };
  }

  private QueryContext compileQueryContext(String query, Map<String, String> queryOptions) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    if (!queryOptions.isEmpty()) {
      pinotQuery.setQueryOptions(new HashMap<>(queryOptions));
    }
    PinotQuery serverPinotQuery = GapfillUtils.stripGapfill(pinotQuery);
    if (serverPinotQuery != pinotQuery && !queryOptions.isEmpty()) {
      serverPinotQuery.setQueryOptions(new HashMap<>(queryOptions));
    }
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(serverPinotQuery);
    queryContext.setSchema(SCHEMA);
    return queryContext;
  }

  private Operator planOperator(QueryContext queryContext) {
    return PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(_indexSegment), queryContext).run();
  }

  private static boolean isJsonIndexDistinctOperator(Operator operator) {
    return JSON_INDEX_DISTINCT_OPERATOR_NAME.equals(operator.getClass().getSimpleName());
  }

  private static void assertJsonIndexDistinctOperator(String queryDescription, Operator operator) {
    Preconditions.checkState(isJsonIndexDistinctOperator(operator),
        "%s must plan to JsonIndexDistinctOperator with %s=true but planned %s",
        queryDescription, CommonConstants.Broker.Request.QueryOptionKey.USE_INDEX_BASED_DISTINCT_OPERATOR,
        operator.getClass().getName());
  }

  private int runQueryCount(QueryContext queryContext) {
    Operator operator = planOperator(queryContext);
    DistinctResultsBlock resultsBlock = (DistinctResultsBlock) operator.nextBlock();
    return resultsBlock.getNumRows();
  }

  private QueryExecution executeAndCollect(QueryContext queryContext) {
    Operator operator = planOperator(queryContext);
    DistinctResultsBlock resultsBlock = (DistinctResultsBlock) operator.nextBlock();
    ExecutionStatistics stats = operator.getExecutionStatistics();
    Set<String> values = new TreeSet<>();
    for (Object[] row : resultsBlock.getRows()) {
      values.add((String) row[0]);
    }
    return new QueryExecution(values, stats);
  }

  private String getSampleQuery() {
    switch (_queryVariant) {
      case "THREE_ARG":
        return THREE_ARG_SAMPLE_QUERY;
      case "FOUR_ARG":
        return FOUR_ARG_SAMPLE_QUERY;
      case "THREE_ARG_EXTRA_FILTER":
        return THREE_ARG_EXTRA_FILTER_SAMPLE_QUERY;
      case "FOUR_ARG_EXTRA_FILTER":
        return FOUR_ARG_EXTRA_FILTER_SAMPLE_QUERY;
      default:
        throw new IllegalStateException("Unsupported query variant: " + _queryVariant);
    }
  }

  private long measureMedianNs(IntSupplier runner) {
    for (int i = 0; i < REGRESSION_WARMUP_ITERATIONS; i++) {
      _regressionSink = runRegressionBatch(runner);
    }

    long[] samples = new long[REGRESSION_MEASURE_ITERATIONS];
    for (int i = 0; i < REGRESSION_MEASURE_ITERATIONS; i++) {
      long startNs = System.nanoTime();
      _regressionSink = runRegressionBatch(runner);
      samples[i] = System.nanoTime() - startNs;
    }
    Arrays.sort(samples);
    return samples[samples.length / 2];
  }

  private int runRegressionBatch(IntSupplier runner) {
    int sum = 0;
    for (int i = 0; i < REGRESSION_BATCH_SIZE; i++) {
      sum += runner.getAsInt();
    }
    return sum;
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder()
        .include(BenchmarkJsonIndexDistinct.class.getSimpleName())
        .build()).run();
  }

  private static final class QueryExecution {
    final Set<String> _values;
    final ExecutionStatistics _stats;

    QueryExecution(Set<String> values, ExecutionStatistics stats) {
      _values = values;
      _stats = stats;
    }
  }
}
