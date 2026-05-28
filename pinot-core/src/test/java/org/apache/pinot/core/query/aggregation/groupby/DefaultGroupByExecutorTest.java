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
package org.apache.pinot.core.query.aggregation.groupby;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for the OOM instrumentation added to {@link DefaultGroupByExecutor#process(ValueBlock)}.
 *
 * <p>The {@code process(ValueBlock)} method samples resource usage and checks query termination once per block via
 * {@link QueryThreadContext#checkTerminationAndSampleUsage(String)}. This test verifies both behaviors:
 * <ul>
 *   <li>each {@code process()} call samples usage exactly once per block;</li>
 *   <li>{@code process()} throws a {@link TerminationException} when the query has been terminated.</li>
 * </ul>
 */
@SuppressWarnings("rawtypes")
public class DefaultGroupByExecutorTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DefaultGroupByExecutorTest");
  private static final String SEGMENT_NAME = "TestGroupBy";
  private static final String DIM_COLUMN = "dimCol";
  private static final String METRIC_COLUMN = "metric";
  // Use enough rows to span multiple blocks (block size is DocIdSetPlanNode.MAX_DOC_PER_CALL = 10000).
  private static final int NUM_ROWS = 25000;

  private IndexSegment _indexSegment;
  private QueryContext _queryContext;

  @BeforeClass
  public void setUp()
      throws Exception {
    setupSegment();
    _queryContext = QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY dimCol");
  }

  /**
   * Verifies that {@link DefaultGroupByExecutor#process(ValueBlock)} samples resource usage exactly once per block.
   */
  @Test
  public void testProcessSamplesUsageOncePerBlock() {
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    ThreadAccountant accountant = Mockito.mock(ThreadAccountant.class);
    try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, accountant)) {
      TransformOperator transformOperator = buildTransformOperator();
      ExpressionContext[] groupByExpressions = _queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);
      DefaultGroupByExecutor executor =
          new DefaultGroupByExecutor(_queryContext, groupByExpressions, transformOperator);

      int numBlocks = 0;
      ValueBlock block;
      while ((block = transformOperator.nextBlock()) != null) {
        // Ignore samples from the project/DocIdSet chain so we isolate process()'s own sampling.
        Mockito.clearInvocations(accountant);
        executor.process(block);
        // process() samples exactly once per block.
        Mockito.verify(accountant, Mockito.times(1)).sampleUsage();
        numBlocks++;
      }
      Assert.assertTrue(numBlocks >= 2, "expected multiple blocks, got " + numBlocks);
    }
  }

  /**
   * Verifies that {@link DefaultGroupByExecutor#process(ValueBlock)} throws when the query has been terminated.
   */
  @Test
  public void testProcessThrowsWhenTerminated() {
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    ThreadAccountant accountant = Mockito.mock(ThreadAccountant.class);
    try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, accountant)) {
      TransformOperator transformOperator = buildTransformOperator();
      ExpressionContext[] groupByExpressions = _queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);
      DefaultGroupByExecutor executor =
          new DefaultGroupByExecutor(_queryContext, groupByExpressions, transformOperator);

      // Pull one block before terminating so getting the block succeeds.
      ValueBlock block = transformOperator.nextBlock();
      Assert.assertNotNull(block);

      Assert.assertTrue(executionContext.terminate(QueryErrorCode.QUERY_CANCELLATION, "test cancellation"));

      // process() is invoked directly (not through the operator chain) to isolate its own termination check.
      Assert.assertThrows(TerminationException.class, () -> executor.process(block));
    }
  }

  /**
   * Verifies that {@link DefaultGroupByExecutor#process(ValueBlock)} throws when the query deadline has passed.
   */
  @Test
  public void testProcessThrowsWhenDeadlineExceeded() {
    ThreadAccountant accountant = Mockito.mock(ThreadAccountant.class);
    ValueBlock block;
    DefaultGroupByExecutor executor;
    // Build the executor and pull a block under a context with a healthy deadline.
    try (QueryThreadContext ignored = QueryThreadContext.open(QueryExecutionContext.forSseTest(), accountant)) {
      TransformOperator transformOperator = buildTransformOperator();
      ExpressionContext[] groupByExpressions = _queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);
      executor = new DefaultGroupByExecutor(_queryContext, groupByExpressions, transformOperator);
      block = transformOperator.nextBlock();
      Assert.assertNotNull(block);
    }

    // Re-open with a deadline in the past so the per-block check trips the timeout branch.
    long pastDeadlineMs = System.currentTimeMillis() - 1000;
    QueryExecutionContext expiredContext =
        new QueryExecutionContext(QueryExecutionContext.QueryType.SSE, 123L, "cid", "workload",
            System.currentTimeMillis(), pastDeadlineMs, pastDeadlineMs, "brokerId", "instanceId", "");
    ValueBlock finalBlock = block;
    DefaultGroupByExecutor finalExecutor = executor;
    try (QueryThreadContext ignored = QueryThreadContext.open(expiredContext, accountant)) {
      QueryException exception = Assert.expectThrows(QueryException.class, () -> finalExecutor.process(finalBlock));
      Assert.assertEquals(exception.getErrorCode(), QueryErrorCode.EXECUTION_TIMEOUT);
    }
  }

  private TransformOperator buildTransformOperator() {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    List<ExpressionContext> expressions = new ArrayList<>();
    for (String column : _indexSegment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    MatchAllFilterOperator matchAllFilterOperator = new MatchAllFilterOperator(totalDocs);
    DocIdSetOperator docIdSetOperator =
        new DocIdSetOperator(matchAllFilterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator =
        new ProjectionOperator(dataSourceMap, docIdSetOperator, new QueryContext.Builder().build());
    return new TransformOperator(_queryContext, projectionOperator, expressions);
  }

  private void setupSegment()
      throws Exception {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    SegmentGeneratorConfig config =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build(),
            buildSchema());
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      // Low-cardinality dimension column.
      row.putValue(DIM_COLUMN, i % 10);
      row.putValue(METRIC_COLUMN, (double) i);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.heap);
  }

  private Schema buildSchema() {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(DIM_COLUMN, FieldSpec.DataType.INT, true));
    schema.addField(new MetricFieldSpec(METRIC_COLUMN, FieldSpec.DataType.DOUBLE));
    return schema;
  }

  @AfterClass
  public void tearDown() {
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
