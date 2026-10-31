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
package org.apache.pinot.queries;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Checks that the query deadline stops the group-by processing of a block between two transform evaluations.
///
/// The segment has one block. `DefaultGroupByExecutor.process()` handles the whole block in one call. Each aggregation
/// evaluates `sleep(...)` on the whole block, which takes
/// `NUM_RECORDS * SLEEP_MS` = 100 ms. The 10 aggregations take 1 s in total. No termination check runs inside one
/// block, except the check that `TransformBlockValSet` runs before each transform evaluation. So with a 200 ms
/// deadline, the query must fail with a timeout after about 300 ms. Without that check, the query completes after 1 s
/// without an error.
public class TransformEvaluationDeadlineQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "TransformEvaluationDeadlineQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String COLUMN = "col";
  private static final int NUM_RECORDS = 20;
  private static final int SLEEP_MS = 5;
  private static final int NUM_AGGREGATIONS = 10;
  private static final long DEADLINE_MS = 200;
  // The full query takes NUM_AGGREGATIONS * NUM_RECORDS * SLEEP_MS = 1 s. Stopping at the deadline plus at most one
  // more evaluation (100 ms) is well below this limit.
  private static final long MAX_ELAPSED_MS = 800;

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(COLUMN, DataType.LONG)
      .build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return List.of(_indexSegment);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      // Every row sleeps SLEEP_MS: the aggregations call sleep(col + k) with k >= 0, and sleep(0) does not sleep.
      record.putValue(COLUMN, (long) SLEEP_MS);
      records.add(record);
    }
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
  }

  @Test
  public void testDeadlineStopsBlockBetweenTransformEvaluations() {
    if (!FunctionUtils.isAssertEnabled()) {
      throw new SkipException("sleep() only sleeps when assertions are enabled");
    }
    StringBuilder query = new StringBuilder("SELECT col");
    for (int k = 0; k < NUM_AGGREGATIONS; k++) {
      // A different constant per aggregation, so that each one is a different transform expression.
      query.append(", SUM(sleep(col + ").append(k).append("))");
    }
    query.append(" FROM testTable GROUP BY col");

    // Plan before the deadline starts: planning the first query of the JVM can take longer than the deadline.
    BaseOperator<?> operator = getOperator(query.toString());
    long startMs = System.currentTimeMillis();
    QueryExecutionContext executionContext =
        new QueryExecutionContext(QueryExecutionContext.QueryType.SSE, 123L, "cid", "default", startMs,
            startMs + DEADLINE_MS, startMs + DEADLINE_MS, "brokerId", "instanceId", "");
    try (QueryThreadContext ignore = QueryThreadContext.open(executionContext,
        ThreadAccountantUtils.getNoOpAccountant())) {
      QueryException e = expectThrows(QueryException.class, operator::nextBlock);
      // The block must start before the deadline, so that the timeout comes from inside the block.
      assertTrue(e.getMessage().contains("transform function"), e.getMessage());
      assertEquals(e.getErrorCode(), QueryErrorCode.EXECUTION_TIMEOUT);
    }
    long elapsedMs = System.currentTimeMillis() - startMs;
    assertTrue(elapsedMs < MAX_ELAPSED_MS,
        "The query must stop soon after the deadline (" + DEADLINE_MS + " ms), but it ran " + elapsedMs + " ms");
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
