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
package org.apache.pinot.core.query.distinct.table;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.TrackingScope;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Verifies that [DistinctTable#mergeDistinctTable] reports resource usage to the query accountant *while* the
/// combined distinct set grows, instead of only when the result is serialized in [DistinctTable#toDataTable()].
///
/// This is the cross-segment merge performed by `DistinctCombineOperator` on the main query thread. It is unbounded
/// when the query has no limit, and neither it nor its caller
/// (`BaseSingleBlockCombineOperator#mergeResults`) used to sample or check for termination, so a runaway
/// `SELECT DISTINCT` could not be killed by memory footprint for as long as it was growing.
///
/// Every concrete [DistinctTable] subclass has three merge branches (unbounded, limited without order-by, limited
/// with order-by) and each carries its own hand-written sampling call, so all 24 are covered here.
public class DistinctTableMergeAccountingTest {
  private static final int SAMPLE_INTERVAL = QueryThreadContext.CHECK_TERMINATION_AND_SAMPLE_USAGE_RECORD_MASK + 1;

  /// Number of values merged per case. Chosen to cross [#SAMPLE_INTERVAL] exactly twice, so the merge loop is expected
  /// to sample at value `0`, `SAMPLE_INTERVAL` and `2 * SAMPLE_INTERVAL`.
  private static final int NUM_VALUES = 3 * SAMPLE_INTERVAL;
  private static final int EXPECTED_SAMPLES = 3;

  /// Limit used by the two limited branches. High enough that the merge never satisfies the limit and returns early.
  private static final int LIMIT = NUM_VALUES + 1;

  /// Builds a populated merge source plus the (empty) destination it is merged into.
  @FunctionalInterface
  private interface TableFactory {
    MergeCase create(int limit, boolean withOrderBy);
  }

  @DataProvider(name = "mergeCases")
  public Object[][] mergeCases() {
    List<Object[]> cases = new ArrayList<>();
    addBranches(cases, "INT", DistinctTableMergeAccountingTest::intTables);
    addBranches(cases, "LONG", DistinctTableMergeAccountingTest::longTables);
    addBranches(cases, "FLOAT", DistinctTableMergeAccountingTest::floatTables);
    addBranches(cases, "DOUBLE", DistinctTableMergeAccountingTest::doubleTables);
    addBranches(cases, "BIG_DECIMAL", DistinctTableMergeAccountingTest::bigDecimalTables);
    addBranches(cases, "BYTES", DistinctTableMergeAccountingTest::bytesTables);
    addBranches(cases, "STRING", DistinctTableMergeAccountingTest::stringTables);
    addBranches(cases, "MULTI_COLUMN", DistinctTableMergeAccountingTest::multiColumnTables);
    return cases.toArray(new Object[0][]);
  }

  private static void addBranches(List<Object[]> cases, String type, TableFactory factory) {
    cases.add(new Object[]{factory.create(Integer.MAX_VALUE, false).named(type + " unbounded")});
    cases.add(new Object[]{factory.create(LIMIT, false).named(type + " limited")});
    cases.add(new Object[]{factory.create(LIMIT, true).named(type + " limited with order-by")});
  }

  @Test(dataProvider = "mergeCases")
  public void testMergeSamplesUsage(MergeCase mergeCase) {
    TrackingAccountant accountant = new TrackingAccountant();
    try (QueryThreadContext ignored = QueryThreadContext.open(QueryExecutionContext.forSseTest(), accountant)) {
      mergeCase.merge();
    }
    assertEquals(mergeCase.destinationSize(), NUM_VALUES);
    assertEquals(accountant.getSampleUsageCount(), EXPECTED_SAMPLES);
  }

  @Test(dataProvider = "mergeCases")
  public void testMergeIsKillable(MergeCase mergeCase) {
    // Terminate the query on the second sample, mimicking the accountant's watcher thread killing the query with the
    // largest memory footprint. The merge notices at its next termination check, i.e. after 2 * SAMPLE_INTERVAL values,
    // instead of running to completion. Asserting the exact size also pins the counter to post-increment semantics.
    TrackingAccountant accountant = new TrackingAccountant(2);
    try (QueryThreadContext ignored = QueryThreadContext.open(QueryExecutionContext.forSseTest(), accountant)) {
      assertThrows(TerminationException.class, mergeCase::merge);
    }
    assertEquals(mergeCase.destinationSize(), 2 * SAMPLE_INTERVAL);
  }

  private static DataSchema singleColumnSchema(ColumnDataType columnDataType) {
    return new DataSchema(new String[]{"col"}, new ColumnDataType[]{columnDataType});
  }

  private static OrderByExpressionContext orderBy(String column) {
    return new OrderByExpressionContext(ExpressionContext.forIdentifier(column), true);
  }

  private static MergeCase intTables(int limit, boolean withOrderBy) {
    DataSchema schema = singleColumnSchema(ColumnDataType.INT);
    IntDistinctTable source = new IntDistinctTable(schema, Integer.MAX_VALUE, false, null);
    for (int i = 0; i < NUM_VALUES; i++) {
      source.addUnbounded(i);
    }
    return new MergeCase(source,
        new IntDistinctTable(schema, limit, false, withOrderBy ? orderBy("col") : null));
  }

  private static MergeCase longTables(int limit, boolean withOrderBy) {
    DataSchema schema = singleColumnSchema(ColumnDataType.LONG);
    LongDistinctTable source = new LongDistinctTable(schema, Integer.MAX_VALUE, false, null);
    for (int i = 0; i < NUM_VALUES; i++) {
      source.addUnbounded(i);
    }
    return new MergeCase(source,
        new LongDistinctTable(schema, limit, false, withOrderBy ? orderBy("col") : null));
  }

  private static MergeCase floatTables(int limit, boolean withOrderBy) {
    DataSchema schema = singleColumnSchema(ColumnDataType.FLOAT);
    FloatDistinctTable source = new FloatDistinctTable(schema, Integer.MAX_VALUE, false, null);
    for (int i = 0; i < NUM_VALUES; i++) {
      source.addUnbounded(i);
    }
    return new MergeCase(source,
        new FloatDistinctTable(schema, limit, false, withOrderBy ? orderBy("col") : null));
  }

  private static MergeCase doubleTables(int limit, boolean withOrderBy) {
    DataSchema schema = singleColumnSchema(ColumnDataType.DOUBLE);
    DoubleDistinctTable source = new DoubleDistinctTable(schema, Integer.MAX_VALUE, false, null);
    for (int i = 0; i < NUM_VALUES; i++) {
      source.addUnbounded(i);
    }
    return new MergeCase(source,
        new DoubleDistinctTable(schema, limit, false, withOrderBy ? orderBy("col") : null));
  }

  private static MergeCase bigDecimalTables(int limit, boolean withOrderBy) {
    DataSchema schema = singleColumnSchema(ColumnDataType.BIG_DECIMAL);
    BigDecimalDistinctTable source = new BigDecimalDistinctTable(schema, Integer.MAX_VALUE, false, null);
    for (int i = 0; i < NUM_VALUES; i++) {
      source.addUnbounded(BigDecimal.valueOf(i));
    }
    return new MergeCase(source,
        new BigDecimalDistinctTable(schema, limit, false, withOrderBy ? orderBy("col") : null));
  }

  private static MergeCase bytesTables(int limit, boolean withOrderBy) {
    DataSchema schema = singleColumnSchema(ColumnDataType.BYTES);
    BytesDistinctTable source = new BytesDistinctTable(schema, Integer.MAX_VALUE, false, null);
    for (int i = 0; i < NUM_VALUES; i++) {
      source.addUnbounded(new ByteArray(Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
    }
    return new MergeCase(source,
        new BytesDistinctTable(schema, limit, false, withOrderBy ? orderBy("col") : null));
  }

  private static MergeCase stringTables(int limit, boolean withOrderBy) {
    DataSchema schema = singleColumnSchema(ColumnDataType.STRING);
    StringDistinctTable source = new StringDistinctTable(schema, Integer.MAX_VALUE, false, null);
    for (int i = 0; i < NUM_VALUES; i++) {
      source.addUnbounded(Integer.toString(i));
    }
    return new MergeCase(source,
        new StringDistinctTable(schema, limit, false, withOrderBy ? orderBy("col") : null));
  }

  private static MergeCase multiColumnTables(int limit, boolean withOrderBy) {
    DataSchema schema =
        new DataSchema(new String[]{"col1", "col2"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    MultiColumnDistinctTable source = new MultiColumnDistinctTable(schema, Integer.MAX_VALUE, false, null);
    for (int i = 0; i < NUM_VALUES; i++) {
      source.addUnbounded(new Record(new Object[]{i, i}));
    }
    return new MergeCase(source,
        new MultiColumnDistinctTable(schema, limit, false, withOrderBy ? List.of(orderBy("col1")) : null));
  }

  /// One merge scenario: a populated source table and the destination it is merged into. TestNG renders each
  /// data-provider parameter with `toString()`, so [#named] supplies a readable label for the test report.
  private static class MergeCase {
    private final DistinctTable _source;
    private final DistinctTable _destination;

    private String _name = "";

    MergeCase(DistinctTable source, DistinctTable destination) {
      _source = source;
      _destination = destination;
    }

    MergeCase named(String name) {
      _name = name;
      return this;
    }

    void merge() {
      _destination.mergeDistinctTable(_source);
    }

    int destinationSize() {
      return _destination.size();
    }

    @Override
    public String toString() {
      return _name;
    }
  }

  /// Counts [#sampleUsage()] calls and optionally terminates the query once a given number of samples has been taken,
  /// standing in for the real accountant's watcher thread. Only ever used from a single thread.
  private static class TrackingAccountant implements ThreadAccountant {
    private final AtomicInteger _sampleUsageCount = new AtomicInteger();
    private final int _terminateAfterSamples;

    private QueryThreadContext _threadContext;

    TrackingAccountant() {
      this(Integer.MAX_VALUE);
    }

    TrackingAccountant(int terminateAfterSamples) {
      _terminateAfterSamples = terminateAfterSamples;
    }

    int getSampleUsageCount() {
      return _sampleUsageCount.get();
    }

    @Override
    public void setupTask(QueryThreadContext threadContext) {
      _threadContext = threadContext;
    }

    @Override
    public void sampleUsage() {
      if (_sampleUsageCount.incrementAndGet() == _terminateAfterSamples) {
        _threadContext.getExecutionContext()
            .terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "Terminated by test accountant");
      }
    }

    @Override
    public void clear() {
      _threadContext = null;
    }

    @Override
    public void updateUntrackedResourceUsage(String identifier, long cpuTimeNs, long allocatedBytes,
        TrackingScope trackingScope) {
    }

    @Override
    public Collection<? extends ThreadResourceTracker> getThreadResources() {
      return List.of();
    }

    @Override
    public Map<String, ? extends QueryResourceTracker> getQueryResources() {
      return Map.of();
    }
  }
}
