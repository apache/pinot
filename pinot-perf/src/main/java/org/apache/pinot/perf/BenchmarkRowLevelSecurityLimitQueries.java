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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.queries.BaseQueriesTest;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.rewriter.RlsUtils;
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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/// Measures single-stage selection queries with a LIMIT under row-level security (RLS), end to end on the server: plan,
/// filter, projection and the combine of 4 segments.
///
/// Each segment has 1M rows of 10 tenants, interleaved. The RLS filter is `tenant = 'T0'`, on an inverted index, so it
/// allows 10% of the rows. As the broker does, each query has the RLS predicate in its WHERE clause and in the
/// `rlsFilters-<table>` query option.
///
/// `_mode` is the value of the `andRestrictionPushdownMode` query option:
///
/// - `ALWAYS`: the eager push-down. This is what every query under RLS does without the streaming push-down.
/// - `NEVER`: the default. Under RLS it resolves as `AUTO`, which streams a selection without ORDER BY.
///
/// The queries:
///
/// - `SCAN`: one scan predicate on a column without an index.
/// - `EXPRESSION`: one predicate on an expression, evaluated by `ExpressionScanDocIdIterator`.
/// - `OR`: an OR of two scans that matches many rows.
/// - `OR_FEW_MATCHES`: an OR of two scans that matches fewer rows than the LIMIT: `id = 3` is a row of another tenant,
///   and only 5 rows of `T0` have the rare name. The query streams, then reads every candidate anyway: the worst case
///   of the streaming push-down.
/// - `COUNT`: an aggregation, as a control. Both modes resolve to the eager push-down.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
public class BenchmarkRowLevelSecurityLimitQueries extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "BenchmarkRowLevelSecurityLimitQueries");
  private static final String TABLE_NAME = "testTable";
  private static final int NUM_ROWS = 1_000_000;
  private static final int NUM_TENANTS = 10;
  private static final int NUM_NAMES = 1000;
  private static final String RLS_FILTER = "tenant = 'T0'";
  /// Rows of tenant T0 with this name. Fewer than the LIMIT of `OR_FEW_MATCHES` on every segment.
  private static final String RARE_NAME = "needle";
  private static final int NUM_RARE_ROWS = 5;

  private static final Map<String, String> QUERIES = Map.of(
      "SCAN", "SELECT id, payload FROM testTable WHERE " + RLS_FILTER + " AND payload > 500 LIMIT 10",
      "EXPRESSION", "SELECT id, payload FROM testTable WHERE " + RLS_FILTER + " AND ABS(payload - 500) < 100 LIMIT 10",
      "OR", "SELECT id, payload FROM testTable WHERE " + RLS_FILTER + " AND (payload > 900 OR name = 'n7') LIMIT 10",
      "OR_FEW_MATCHES", "SELECT id, payload FROM testTable WHERE " + RLS_FILTER
          + " AND (id = 3 OR name = '" + RARE_NAME + "') LIMIT 100",
      "COUNT", "SELECT COUNT(*) FROM testTable WHERE " + RLS_FILTER + " AND payload > 500");

  @Param({"SCAN", "EXPRESSION", "OR", "OR_FEW_MATCHES", "COUNT"})
  private String _query;

  @Param({"ALWAYS", "NEVER"})
  private String _mode;

  private ImmutableSegment _segment;
  private List<IndexSegment> _segments;
  private Map<String, String> _queryOptions;

  public static void main(String[] args)
      throws RunnerException {
    new Runner(
        new OptionsBuilder().include(BenchmarkRowLevelSecurityLimitQueries.class.getSimpleName()).build()).run();
  }

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension("id", DataType.INT)
        .addSingleValueDimension("tenant", DataType.STRING)
        .addSingleValueDimension("payload", DataType.INT)
        .addSingleValueDimension("name", DataType.STRING)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of("tenant"))
        .build();

    Random random = new Random(42);
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    int numRareRows = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      int tenant = i % NUM_TENANTS;
      String name = "n" + random.nextInt(NUM_NAMES);
      // Spread the rare rows over the segment, so that the query reads every chunk to find them
      if (tenant == 0 && numRareRows < NUM_RARE_ROWS && i >= (long) numRareRows * NUM_ROWS / NUM_RARE_ROWS) {
        name = RARE_NAME;
        numRareRows++;
      }
      GenericRow row = new GenericRow();
      row.putValue("id", i);
      row.putValue("tenant", "T" + tenant);
      row.putValue("payload", random.nextInt(1000));
      row.putValue("name", name);
      rows.add(row);
    }
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(TABLE_NAME);
    config.setSegmentName("segment");
    config.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    _segment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, "segment"), new IndexLoadingConfig(tableConfig, schema));
    _segments = List.of(_segment, _segment);
    _queryOptions = Map.of(RlsUtils.buildRlsFilterKey(TABLE_NAME), RLS_FILTER,
        QueryOptionKey.AND_RESTRICTION_PUSHDOWN_MODE, _mode);

    // Both modes must return the same number of rows. Print the filter cost, which the timings do not show.
    BrokerResponseNative response = runQuery();
    if (!response.getExceptions().isEmpty()) {
      throw new IllegalStateException("Query failed: " + response.getExceptions());
    }
    System.out.printf("%s %s: rows=%d, numDocsScanned=%d, numEntriesScannedInFilter=%d%n", _query, _mode,
        response.getResultTable().getRows().size(), response.getNumDocsScanned(),
        response.getNumEntriesScannedInFilter());
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws Exception {
    _segment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Benchmark
  public BrokerResponseNative query() {
    return runQuery();
  }

  private BrokerResponseNative runQuery() {
    return getBrokerResponse(QUERIES.get(_query), _queryOptions);
  }

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _segment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _segments;
  }
}
