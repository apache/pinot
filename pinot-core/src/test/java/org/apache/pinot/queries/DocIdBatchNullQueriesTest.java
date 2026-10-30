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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.dociditerators.EmptyDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.MatchAllDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SortedDocIdIterator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Verifies nullable group aggregation across full and tail doc-id blocks and separately executed server responses.
/// The fixture belongs to this test instance; it has no shared mutable state with other query tests.
public class DocIdBatchNullQueriesTest extends BaseQueriesTest {
  private static final String TABLE = "DocIdBatchNullQueriesTest";
  private static final int FIRST_ROWS = 10017;
  private static final int TOTAL_ROWS = FIRST_ROWS + 3;
  private static final Map<String, String> OPTIONS = Map.of("enableNullHandling", "true", "skipStarTree", "true",
      "maxExecutionThreads", "1", "numGroupsLimit", "100");

  private final List<IndexSegment> _segments = new ArrayList<>(2);
  private File _directory;

  @BeforeClass
  public void setUp()
      throws Exception {
    _directory = Files.createTempDirectory("doc-id-batch-null-queries").toFile();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).setSortedColumn("s")
        .setNoDictionaryColumns(List.of("m")).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE).addSingleValueDimension("s", DataType.INT)
        .addSingleValueDimension("g", DataType.INT).addMetric("m", DataType.DOUBLE).build();
    for (int segmentId = 0; segmentId < 2; segmentId++) {
      int start = segmentId == 0 ? 0 : FIRST_ROWS;
      int end = segmentId == 0 ? FIRST_ROWS : TOTAL_ROWS;
      List<GenericRow> rows = new ArrayList<>(end - start);
      for (int id = start; id < end; id++) {
        GenericRow row = new GenericRow();
        row.putValue("s", id);
        row.putValue("g", group(id));
        row.putValue("m", metric(id));
        rows.add(row);
      }
      String segmentName = "segment-" + segmentId;
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(_directory.getPath());
      config.setSegmentName(segmentName);
      config.setSegmentVersion(SegmentVersion.v3);
      config.setDefaultNullHandlingEnabled(true);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      try (GenericRowRecordReader reader = new GenericRowRecordReader(rows)) {
        driver.init(config, reader);
        driver.build();
      }
      IndexSegment segment = ImmutableSegmentLoader.load(new File(_directory, segmentName), ReadMode.mmap);
      _segments.add(segment);
      assertTrue(segment.getDataSource("s").getDataSourceMetadata().isSorted());
      assertNotNull(segment.getDataSource("s").getDictionary());
      assertNull(segment.getDataSource("m").getDictionary());
      assertNotNull(segment.getDataSource("m").getNullValueVector());
      assertTrue(segment.getStarTrees() == null || segment.getStarTrees().isEmpty());
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    for (IndexSegment segment : _segments) {
      segment.destroy();
    }
    if (_directory != null) {
      FileUtils.deleteDirectory(_directory);
    }
  }

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _segments.get(0);
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _segments;
  }

  @Override
  protected List<List<IndexSegment>> getDistinctInstances() {
    return List.of(List.of(_segments.get(0)), List.of(_segments.get(1)));
  }

  @DataProvider
  public Object[][] filters() {
    IntPredicate all = id -> true;
    IntPredicate range = id -> id >= 5;
    IntPredicate gaps = id -> id != 7 && id != 10000;
    IntPredicate tail = id -> id >= 10015;
    IntPredicate allNull = id -> id == 3;
    IntPredicate empty = id -> id > 20000;
    return new Object[][]{
        {"", all}, {" WHERE s >= 5", range}, {" WHERE s NOT IN (7, 10000)", gaps},
        {" WHERE s >= 10015", tail}, {" WHERE s = 3", allNull}, {" WHERE s > 20000", empty}
    };
  }

  @Test(dataProvider = "filters")
  public void testNullableGroupByAcrossBatches(String filter, IntPredicate matches) {
    String query = "SELECT g AS group_key, SUM(m) AS total_value, AVG(m) AS average_value,"
        + " COUNT(*) AS row_count, COUNT(m) AS value_count FROM " + TABLE + filter
        + " GROUP BY g ORDER BY g NULLS FIRST LIMIT 100";
    verifyDocIdBlocks(query, matches);

    Map<Integer, ExpectedGroup> groups = new TreeMap<>(Comparator.nullsFirst(Comparator.naturalOrder()));
    long numDocs = 0;
    for (int id = 0; id < TOTAL_ROWS; id++) {
      if (matches.test(id)) {
        numDocs++;
        ExpectedGroup expected = groups.computeIfAbsent(group(id), ignored -> new ExpectedGroup());
        expected._numRows++;
        Double metric = metric(id);
        if (metric != null) {
          expected._sum += metric;
          expected._numValues++;
        }
      }
    }
    List<Object[]> expectedRows = new ArrayList<>(groups.size());
    groups.forEach((key, values) -> expectedRows.add(new Object[]{key,
        values._numValues == 0 ? null : values._sum,
        values._numValues == 0 ? null : values._sum / values._numValues, values._numRows, values._numValues}));

    BrokerResponseNative response = getBrokerResponse(query, OPTIONS);
    assertFalse(response.isPartialResult(), response.getExceptions().toString());
    assertEquals(response.getTotalDocs(), TOTAL_ROWS);
    assertEquals(response.getNumDocsScanned(), numDocs);
    assertEquals(response.getNumEntriesScannedInFilter(), 0L);
    assertEquals(response.getNumEntriesScannedPostFilter(), numDocs * 2);
    ResultTable result = response.getResultTable();
    assertNotNull(result);
    assertEquals(result.getDataSchema().getColumnNames(),
        new String[]{"group_key", "total_value", "average_value", "row_count", "value_count"});
    assertEquals(result.getDataSchema().getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.INT,
        ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.LONG, ColumnDataType.LONG});
    assertEquals(result.getRows().size(), expectedRows.size());
    for (int row = 0; row < expectedRows.size(); row++) {
      assertEquals(result.getRows().get(row), expectedRows.get(row), "Row " + row + " for " + query);
    }
  }

  private void verifyDocIdBlocks(String query, IntPredicate matches) {
    try (QueryThreadContext ignored = QueryThreadContext.openForSseTest()) {
      for (int segmentId = 0; segmentId < _segments.size(); segmentId++) {
        int start = segmentId == 0 ? 0 : FIRST_ROWS;
        int end = segmentId == 0 ? FIRST_ROWS : TOTAL_ROWS;
        int[] expected = IntStream.range(start, end).filter(matches).map(id -> id - start).toArray();
        IndexSegment segment = _segments.get(segmentId);
        try (BlockDocIdIterator iterator = new FilterPlanNode(new SegmentContext(segment), context(query)).run()
            .nextBlock().getBlockDocIdSet().iterator()) {
          if (expected.length == 0) {
            assertTrue(iterator instanceof EmptyDocIdIterator);
          } else if (expected.length == end - start) {
            assertTrue(iterator instanceof MatchAllDocIdIterator);
          } else {
            assertTrue(iterator instanceof SortedDocIdIterator);
          }
        }
        DocIdSetOperator operator = new DocIdSetOperator(
            new FilterPlanNode(new SegmentContext(segment), context(query)).run(), DocIdSetPlanNode.MAX_DOC_PER_CALL);
        int offset = 0;
        DocIdSetBlock block;
        while ((block = operator.nextBlock()) != null) {
          int expectedSize = Math.min(DocIdSetPlanNode.MAX_DOC_PER_CALL, expected.length - offset);
          assertEquals(block.getLength(), expectedSize);
          for (int i = 0; i < expectedSize; i++) {
            assertEquals(block.getDocIds()[i], expected[offset++]);
          }
        }
        assertEquals(offset, expected.length);
        assertNull(operator.nextBlock());
      }
    }
  }

  private static QueryContext context(String sql) {
    PinotQuery query = CalciteSqlParser.compileToPinotQuery(sql);
    query.setQueryOptions(new HashMap<>(OPTIONS));
    QueryContext context = QueryContextConverterUtils.getQueryContext(query);
    context.setEndTimeMs(System.currentTimeMillis() + 60_000);
    return context;
  }

  @Nullable
  private static Integer group(int id) {
    return id % 11 == 0 ? null : id % 4;
  }

  @Nullable
  private static Double metric(int id) {
    // Group 3 has only null metrics; other groups mix null and non-null values.
    return id % 4 == 3 || id % 7 == 0 ? null : (id % 17 - 8) * 0.25;
  }

  /// Independent exact-quarter arithmetic oracle, including rows excluded by COUNT(metric).
  private static final class ExpectedGroup {
    private double _sum;
    private long _numRows;
    private long _numValues;
  }
}
