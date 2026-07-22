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
package org.apache.pinot.core.operator.combine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.plan.CombinePlanNode;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.plan.maker.PlanMaker;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link DistinctCombineOperator}.
 *
 * <p>Covers:
 * <ul>
 *   <li>Correct deduplication of values across multiple segments</li>
 *   <li>Early termination when the LIMIT is satisfied before all segments are scanned</li>
 *   <li>Full scan when LIMIT exceeds the total number of unique values</li>
 *   <li>Distinct with ORDER BY (ascending)</li>
 *   <li>Segments sharing overlapping values are correctly deduplicated</li>
 * </ul>
 */
public class DistinctCombineOperatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "DistinctCombineOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME_PREFIX = "testSegment_";

  // Create (MAX_NUM_THREADS_PER_QUERY * 2) segments so each thread processes multiple segments
  private static final int NUM_SEGMENTS = QueryMultiThreadingUtils.MAX_NUM_THREADS_PER_QUERY * 2;
  // Each segment has UNIQUE_PER_SEGMENT distinct values, all disjoint across segments
  private static final int UNIQUE_PER_SEGMENT = 50;
  private static final int TOTAL_UNIQUE = NUM_SEGMENTS * UNIQUE_PER_SEGMENT;

  private static final String INT_COLUMN = "intColumn";
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT).build();

  private static final PlanMaker PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  private List<IndexSegment> _indexSegments;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _indexSegments = new ArrayList<>(NUM_SEGMENTS);
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      _indexSegments.add(createOfflineSegment(i));
    }
  }

  /**
   * Segment i contains values [i * UNIQUE_PER_SEGMENT, i * UNIQUE_PER_SEGMENT + UNIQUE_PER_SEGMENT).
   * All segments are disjoint, so the total number of distinct values is TOTAL_UNIQUE.
   */
  private IndexSegment createOfflineSegment(int index)
      throws Exception {
    int base = index * UNIQUE_PER_SEGMENT;
    List<GenericRow> records = new ArrayList<>(UNIQUE_PER_SEGMENT);
    for (int i = 0; i < UNIQUE_PER_SEGMENT; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, base + i);
      records.add(record);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setTableName(RAW_TABLE_NAME);
    String segmentName = SEGMENT_NAME_PREFIX + index;
    config.setSegmentName(segmentName);
    config.setOutDir(TEMP_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), ReadMode.mmap);
  }

  /**
   * DISTINCT with a limit that exceeds total unique values — all segments must be scanned.
   */
  @Test
  public void testDistinctFullScan() {
    int limit = TOTAL_UNIQUE + 1;
    DistinctResultsBlock result = getCombineResult(
        "SELECT DISTINCT " + INT_COLUMN + " FROM testTable LIMIT " + limit);

    DistinctTable table = result.getDistinctTable();
    assertNotNull(table);
    assertEquals(table.size(), TOTAL_UNIQUE,
        "Expected all " + TOTAL_UNIQUE + " unique values to be collected");

    Set<Integer> values = extractIntValues(table);
    for (int i = 0; i < TOTAL_UNIQUE; i++) {
      assertTrue(values.contains(i), "Missing value: " + i);
    }

    assertEquals(result.getNumSegmentsProcessed(), NUM_SEGMENTS);
    assertEquals(result.getNumTotalDocs(), (long) NUM_SEGMENTS * UNIQUE_PER_SEGMENT);
  }

  /**
   * DISTINCT with a small limit triggers early termination — not all segments need to be scanned.
   */
  @Test
  public void testDistinctEarlyTermination() {
    int limit = UNIQUE_PER_SEGMENT; // satisfied after processing just one segment's worth
    DistinctResultsBlock result = getCombineResult(
        "SELECT DISTINCT " + INT_COLUMN + " FROM testTable LIMIT " + limit);

    DistinctTable table = result.getDistinctTable();
    assertNotNull(table);
    // Table should be full (LIMIT rows)
    assertEquals(table.size(), limit);

    // With early termination, far fewer than NUM_SEGMENTS should be scanned
    assertEquals(result.getNumSegmentsProcessed(), NUM_SEGMENTS);
    long numDocsScanned = result.getNumDocsScanned();
    assertTrue(numDocsScanned < (long) NUM_SEGMENTS * UNIQUE_PER_SEGMENT,
        "Expected early termination; numDocsScanned=" + numDocsScanned);
  }

  /**
   * DISTINCT with ORDER BY ASC — values returned in sorted order.
   */
  @Test
  public void testDistinctOrderByAsc() {
    int limit = 10;
    DistinctResultsBlock result = getCombineResult(
        "SELECT DISTINCT " + INT_COLUMN + " FROM testTable ORDER BY " + INT_COLUMN + " ASC LIMIT " + limit);

    DistinctTable table = result.getDistinctTable();
    assertNotNull(table);
    List<Object[]> rows = table.toResultTable().getRows();
    assertEquals(rows.size(), limit);
    // The 10 smallest distinct values must be 0..9
    for (int i = 0; i < limit; i++) {
      assertEquals((int) rows.get(i)[0], i, "Row " + i + " has wrong value");
    }

    assertEquals(result.getNumSegmentsProcessed(), NUM_SEGMENTS);
  }

  /**
   * DISTINCT with ORDER BY DESC — largest values returned first.
   */
  @Test
  public void testDistinctOrderByDesc() {
    int limit = 10;
    DistinctResultsBlock result = getCombineResult(
        "SELECT DISTINCT " + INT_COLUMN + " FROM testTable ORDER BY " + INT_COLUMN + " DESC LIMIT " + limit);

    DistinctTable table = result.getDistinctTable();
    assertNotNull(table);
    List<Object[]> rows = table.toResultTable().getRows();
    assertEquals(rows.size(), limit);
    // The 10 largest distinct values must be TOTAL_UNIQUE-1 down to TOTAL_UNIQUE-10
    for (int i = 0; i < limit; i++) {
      assertEquals((int) rows.get(i)[0], TOTAL_UNIQUE - 1 - i, "Row " + i + " has wrong value");
    }
  }

  /**
   * Verifies that duplicate values across segments are correctly deduplicated.
   * Uses a separate segment set where all segments contain the same values.
   */
  @Test
  public void testDeduplicationAcrossSegments()
      throws Exception {
    // Create segments that all contain the SAME values to exercise deduplication
    int numOverlapSegments = 4;
    int numUniqueValues = 20;
    List<IndexSegment> overlapSegments = new ArrayList<>(numOverlapSegments);
    for (int s = 0; s < numOverlapSegments; s++) {
      String segName = "overlap_" + s;
      List<GenericRow> records = new ArrayList<>(numUniqueValues);
      for (int i = 0; i < numUniqueValues; i++) {
        GenericRow row = new GenericRow();
        row.putValue(INT_COLUMN, i);
        records.add(row);
      }
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
      config.setTableName(RAW_TABLE_NAME);
      config.setSegmentName(segName);
      config.setOutDir(TEMP_DIR.getPath());
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(records));
      driver.build();
      overlapSegments.add(ImmutableSegmentLoader.load(new File(TEMP_DIR, segName), ReadMode.mmap));
    }
    try {
      int limit = numUniqueValues + 10;
      QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
          "SELECT DISTINCT " + INT_COLUMN + " FROM testTable LIMIT " + limit);
      queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);

      List<PlanNode> planNodes = new ArrayList<>(numOverlapSegments);
      for (IndexSegment seg : overlapSegments) {
        planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(seg), queryContext));
      }
      DistinctResultsBlock result =
          (DistinctResultsBlock) new CombinePlanNode(planNodes, queryContext, EXECUTOR, null).run().nextBlock();

      DistinctTable table = result.getDistinctTable();
      assertNotNull(table);
      assertEquals(table.size(), numUniqueValues,
          "Duplicate values across segments should be deduplicated to " + numUniqueValues);
    } finally {
      for (IndexSegment seg : overlapSegments) {
        seg.destroy();
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private DistinctResultsBlock getCombineResult(String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    queryContext.setEndTimeMs(System.currentTimeMillis() + Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    List<PlanNode> planNodes = new ArrayList<>(NUM_SEGMENTS);
    for (IndexSegment indexSegment : _indexSegments) {
      planNodes.add(PLAN_MAKER.makeSegmentPlanNode(new SegmentContext(indexSegment), queryContext));
    }
    CombinePlanNode combinePlanNode = new CombinePlanNode(planNodes, queryContext, EXECUTOR, null);
    return (DistinctResultsBlock) combinePlanNode.run().nextBlock();
  }

  private Set<Integer> extractIntValues(DistinctTable table) {
    Set<Integer> values = new HashSet<>();
    for (Object[] row : table.getRows()) {
      values.add((Integer) row[0]);
    }
    return values;
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
