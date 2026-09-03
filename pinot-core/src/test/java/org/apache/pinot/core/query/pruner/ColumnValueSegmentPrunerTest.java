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
package org.apache.pinot.core.query.pruner;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.VariantEnvelope;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class ColumnValueSegmentPrunerTest {
  private static final ColumnValueSegmentPruner PRUNER = new ColumnValueSegmentPruner();

  @BeforeClass
  public void setUp() {
    Map<String, Object> properties = new HashMap<>();
    // override default value
    properties.put(ColumnValueSegmentPruner.IN_PREDICATE_THRESHOLD, 5);
    PinotConfiguration configuration = new PinotConfiguration(properties);
    PRUNER.init(configuration);
  }

  @Test
  public void testMinMaxValuePruning() {
    IndexSegment indexSegment = mockIndexSegment();

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq("column"), any(Schema.class))).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.getMinValue()).thenReturn(10);
    when(dataSourceMetadata.getMaxValue()).thenReturn(20);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    // Equality predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 20"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 30"));
    // Range predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column <= 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 20"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20 AND 30"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 30 AND 40"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 10 AND 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20 AND 20"));
    // Invalid range predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 20 AND 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 30 AND 20"));
    // In Predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 5, 8)"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (21, 30)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (10)"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (5, 10, 15)"));
    //although the segment can be pruned, it will not be pruned as the size of values is greater than threshold
    assertFalse(
        runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)"));
    assertFalse(
        runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)"));
    // AND operator
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 AND column > 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 0 AND column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 0 AND column <= 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 20 AND column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 20 AND column < 30"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 0 AND column BETWEEN 0 AND 10"));
    // OR operator
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column > 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column < 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 0 OR column <= 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column > 30 OR column < 10"));
    assertTrue(runPruner(indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column BETWEEN 0 AND 5 OR column BETWEEN 30 AND 35"));
  }

  @Test
  public void testRawVariantPredicatesNeverPruneOnEnvelopeBytes() {
    IndexSegment indexSegment = mockIndexSegment();
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq("column"), any(Schema.class))).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.VARIANT);
    String minValue = variantEnvelopeHex(0x10);
    String maxValue = variantEnvelopeHex(0x20);
    when(dataSourceMetadata.getMinValue()).thenReturn(DataType.VARIANT.convertInternal(minValue));
    when(dataSourceMetadata.getMaxValue()).thenReturn(DataType.VARIANT.convertInternal(maxValue));
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    String belowMin = variantEnvelopeHex(0x01);
    String aboveMax = variantEnvelopeHex(0x30);
    assertFalse(runPruner(indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column = '" + belowMin + "'"));
    assertFalse(runPruner(indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column IN ('" + belowMin + "', '" + aboveMax + "')"));
    assertFalse(runPruner(indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column BETWEEN '" + belowMin + "' AND '" + belowMin + "'"));
  }

  @Test
  public void testInPredicatePruningThresholdOverride() {
    IndexSegment indexSegment = mockIndexSegment();

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq("column"), any(Schema.class))).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.getMinValue()).thenReturn(10);
    when(dataSourceMetadata.getMaxValue()).thenReturn(20);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    // Without the option, a large IN list (more than the default threshold of 10) is not pruned even though all
    // values are out of min/max range
    assertFalse(runPruner(indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 21)"));
    // A negative query threshold always prunes, regardless of the IN clause size
    assertTrue(runPruner(indexSegment,
        "SET inPredicatePruningThreshold=-1; "
            + "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 21)"));
    // A positive query threshold above the clause size also prunes
    assertTrue(runPruner(indexSegment,
        "SET inPredicatePruningThreshold=20; "
            + "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 21)"));
  }

  @Test
  public void testNegativeServerThresholdAlwaysPrunes() {
    // The server config follows the same convention: a negative `inpredicate.threshold` always prunes
    ColumnValueSegmentPruner pruner = new ColumnValueSegmentPruner();
    pruner.init(new PinotConfiguration(Map.of(ColumnValueSegmentPruner.IN_PREDICATE_THRESHOLD, -1)));

    IndexSegment indexSegment = mockIndexSegment();

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq("column"), any(Schema.class))).thenReturn(dataSource);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.getMinValue()).thenReturn(10);
    when(dataSourceMetadata.getMaxValue()).thenReturn(20);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    // 11 values, all out of min/max range, pruned without any query option
    assertTrue(runPruner(pruner, indexSegment,
        "SELECT COUNT(*) FROM testTable WHERE column IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 21)"));
  }

  @Test
  public void testPartitionPruning() {
    IndexSegment indexSegment = mockIndexSegment();

    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq("column"), any(Schema.class))).thenReturn(dataSource);

    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.getPartitionFunction()).thenReturn(
        PartitionFunctionFactory.getPartitionFunction("Modulo", 5, null));
    when(dataSourceMetadata.getPartitions()).thenReturn(Set.of(2));
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    // Equality predicate
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 2"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 7"));
    // AND operator
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 AND column = 2"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column >= 0 AND column = 10"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 2 AND column > 0"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column <= 10 AND column = 7"));
    // OR operator
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column = 2"));
    assertFalse(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column < 10"));
    assertTrue(runPruner(indexSegment, "SELECT COUNT(*) FROM testTable WHERE column = 0 OR column = 10"));
  }

  @Test
  public void testIsApplicableTo() {
    // EQ, RANGE and IN (with small number of values) are applicable for min/max/partitionId based pruning.
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column = 1");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column IN (1, 2)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column BETWEEN 1 AND 2");
    assertTrue(PRUNER.isApplicableTo(queryContext));

    // NOT is not applicable
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE NOT column = 1");
    assertFalse(PRUNER.isApplicableTo(queryContext));
    // Too many values for IN clause
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)");
    assertFalse(PRUNER.isApplicableTo(queryContext));
    // ... but applicable when the query threshold is negative
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SET inPredicatePruningThreshold=-1; SELECT COUNT(*) FROM testTable WHERE column IN "
            + "(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
    // Other predicate types are not applicable
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE column LIKE 5");
    assertFalse(PRUNER.isApplicableTo(queryContext));

    // AND with one applicable child filter is applicable
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column NOT IN (1, 2) AND column = 3");
    assertTrue(PRUNER.isApplicableTo(queryContext));

    // OR with one child filter that's not applicable is not applicable
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 3 OR column NOT IN (1, 2)");
    assertFalse(PRUNER.isApplicableTo(queryContext));

    // Nested with AND/OR
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 3 OR (column NOT IN (1, 2) AND column BETWEEN 4 AND 5)");
    assertTrue(PRUNER.isApplicableTo(queryContext));
  }

  @Test
  public void testParallelPruningSelectsTheSameSegments() throws Exception {
    int numSegments = 40;
    Set<Thread> accessThreads = ConcurrentHashMap.newKeySet();
    List<IndexSegment> segments = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      // Alternate: half the segments hold values the predicate can match, half cannot and must be pruned.
      segments.add(segmentWithRange(i % 2 == 0 ? 0 : 100, i % 2 == 0 ? 50 : 150,
          () -> accessThreads.add(Thread.currentThread())));
    }
    QueryContext serialQuery = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 10");
    serialQuery.setSchema(mock(Schema.class));
    QueryContext parallelQuery = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 10");
    parallelQuery.setSchema(mock(Schema.class));
    parallelQuery.setEndTimeMs(System.currentTimeMillis() + 30_000);
    parallelQuery.setMaxExecutionThreads(4);

    List<IndexSegment> serial = PRUNER.prune(segments, serialQuery);
    assertEquals(accessThreads, Set.of(Thread.currentThread()));
    accessThreads.clear();
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<IndexSegment> parallel = PRUNER.prune(segments, parallelQuery, executor);
      assertEquals(new HashSet<>(parallel), new HashSet<>(serial));
      assertEquals(parallel.size(), numSegments / 2);
      assertFalse(accessThreads.isEmpty());
      assertFalse(accessThreads.contains(Thread.currentThread()), "Pruning must run on the supplied executor");
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testParallelPruningFallsBackToCallingThread() {
    Set<Thread> accessThreads = new HashSet<>();
    IndexSegment segment = segmentWithRange(0, 50, () -> accessThreads.add(Thread.currentThread()));
    QueryContext query = pruningQuery();
    ExecutorService executor = mock(ExecutorService.class);

    assertTrue(PRUNER.prune(List.of(), query, executor).isEmpty());
    assertEquals(PRUNER.prune(Collections.nCopies(10, segment), query, executor).size(), 10);
    verifyNoInteractions(executor);
    assertEquals(PRUNER.prune(Collections.nCopies(40, segment), query, null).size(), 40);
    assertEquals(accessThreads, Set.of(Thread.currentThread()));
  }

  @Test(timeOut = 10_000)
  public void testParallelPruningRejectsExpiredDeadline() throws Exception {
    CountDownLatch releaseWorker = new CountDownLatch(1);
    IndexSegment segment = segmentWithRange(0, 50, () -> {
      try {
        releaseWorker.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new QueryCancelledException("Pruning worker interrupted", e);
      }
    });
    QueryContext query = pruningQuery();
    query.setEndTimeMs(0);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      RuntimeException failure = expectThrows(RuntimeException.class,
          () -> PRUNER.prune(Collections.nCopies(40, segment), query, executor));
      assertTrue(ExceptionUtils.indexOfType(failure, TimeoutException.class) >= 0);
    } finally {
      releaseWorker.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @DataProvider
  public Object[][] invalidPredicates() {
    return new Object[][]{
        {false, "column = 'potato'"},
        {false, "column IN (1, 'potato')"},
        {false, "column > 'potato'"},
        {true, "column = 'potato'"},
        {true, "column IN (1, 'potato')"}
    };
  }

  @Test(dataProvider = "invalidPredicates", timeOut = 10_000)
  public void testParallelPruningPreservesValidationErrors(boolean useBloomFilter, String predicate)
      throws Exception {
    IndexSegment segment = segmentWithRange(0, 50, () -> { });
    QueryContext query = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE " + predicate);
    query.setSchema(mock(Schema.class));
    query.setMaxExecutionThreads(1);
    query.setEndTimeMs(System.currentTimeMillis() + 30_000);
    DataSource dataSource = segment.getDataSource("column", query.getSchema());
    when(segment.getDataSourceNullable("column")).thenReturn(dataSource);
    ValueBasedSegmentPruner pruner = useBloomFilter ? new BloomFilterSegmentPruner() : new ColumnValueSegmentPruner();
    pruner.init(new PinotConfiguration());
    List<IndexSegment> segments = Collections.nCopies(40, segment);
    BadQueryRequestException serial = expectThrows(BadQueryRequestException.class, () -> pruner.prune(segments, query));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      BadQueryRequestException parallel = expectThrows(BadQueryRequestException.class,
          () -> pruner.prune(segments, query, executor));
      assertEquals(parallel.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
      assertEquals(parallel.getMessage(), serial.getMessage());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeOut = 10_000)
  public void testParallelPruningStopsInterruptedWorkerBetweenSegments() throws Exception {
    AtomicInteger visits = new AtomicInteger();
    IndexSegment segment = segmentWithRange(0, 50, () -> {
      visits.incrementAndGet();
      Thread.currentThread().interrupt();
    });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      expectThrows(QueryCancelledException.class,
          () -> PRUNER.prune(Collections.nCopies(40, segment), pruningQuery(), executor));
      assertEquals(visits.get(), 1, "An interrupted worker must not keep visiting segments");
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeOut = 10_000)
  public void testParallelPruningPropagatesWorkerFailure() throws Exception {
    IllegalStateException workerFailure = new IllegalStateException("Cannot read segment metadata");
    IndexSegment segment = segmentWithRange(0, 50, () -> {
      throw workerFailure;
    });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      RuntimeException failure = expectThrows(RuntimeException.class,
          () -> PRUNER.prune(Collections.nCopies(40, segment), pruningQuery(), executor));
      assertTrue(ExceptionUtils.getThrowableList(failure).contains(workerFailure));
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private QueryContext pruningQuery() {
    QueryContext query = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE column = 10");
    query.setSchema(mock(Schema.class));
    query.setMaxExecutionThreads(1);
    query.setEndTimeMs(System.currentTimeMillis() + 30_000);
    return query;
  }

  private IndexSegment segmentWithRange(int minValue, int maxValue, Runnable onAccess) {
    IndexSegment indexSegment = mockIndexSegment();
    DataSource dataSource = mock(DataSource.class);
    when(indexSegment.getDataSource(eq("column"), any(Schema.class))).thenAnswer(invocation -> {
      onAccess.run();
      return dataSource;
    });
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getDataType()).thenReturn(DataType.INT);
    when(metadata.getMinValue()).thenReturn(minValue);
    when(metadata.getMaxValue()).thenReturn(maxValue);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    return indexSegment;
  }

  private IndexSegment mockIndexSegment() {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(ImmutableSet.of("column"));
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(20);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    return indexSegment;
  }

  private boolean runPruner(IndexSegment indexSegment, String query) {
    return runPruner(PRUNER, indexSegment, query);
  }

  private boolean runPruner(ColumnValueSegmentPruner pruner, IndexSegment indexSegment, String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setSchema(mock(Schema.class));
    return pruner.prune(Arrays.asList(indexSegment), queryContext).isEmpty();
  }

  private static String variantEnvelopeHex(int valueByte) {
    byte[] envelope = VariantEnvelope.encode(ByteBuffer.allocate(0), ByteBuffer.wrap(new byte[]{(byte) valueByte}));
    return BytesUtils.toHexString(envelope);
  }
}
