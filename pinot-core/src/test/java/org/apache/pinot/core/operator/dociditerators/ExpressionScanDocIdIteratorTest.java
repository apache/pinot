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
package org.apache.pinot.core.operator.dociditerators;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.operator.BaseDocIdSetOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.ProjectionOperatorUtils;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunctionFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests for [ExpressionScanDocIdIterator], in particular that the matching docIds it emits are attributed to the
/// correct rows when the projection operator pulls multiple blocks from the doc-id source before the first block is
/// processed (as a pluggable projection operator registered via
/// [org.apache.pinot.core.operator.ProjectionOperatorUtils] may do to prefetch data from remote storage).
///
/// Regression test for a stale scratch-buffer read: the doc-id source fills a buffer shared with the iterator, and
/// every `nextBlock()` call on the source overwrites it. If the iterator resolves match positions through that shared
/// buffer instead of the docIds carried by the projection block being processed, all blocks except the last one in a
/// look-ahead window get their matches attributed to the last block's docIds, silently returning wrong results.
public class ExpressionScanDocIdIteratorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ExpressionScanDocIdIteratorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INT_COLUMN = "intColumn";
  private static final String NULLABLE_INT_COLUMN = "nullableIntColumn";
  private static final String INT_MV_COLUMN = "intMvColumn";
  // Enough docs for the candidate set to span multiple 10k doc-id batches
  private static final int NUM_DOCS = 50_000;

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(NULLABLE_INT_COLUMN, FieldSpec.DataType.INT)
      .addMultiValueDimension(INT_MV_COLUMN, FieldSpec.DataType.INT)
      .build();

  private IndexSegment _segment;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    // Column values == docId, so expected results are directly computable from docIds; nullableIntColumn is null for
    // every docId divisible by 3; intMvColumn holds [docId % 100, 500 + docId % 100]
    List<GenericRow> records = new ArrayList<>(NUM_DOCS);
    for (int i = 0; i < NUM_DOCS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, i);
      if (i % 3 == 0) {
        record.addNullValueField(NULLABLE_INT_COLUMN);
      } else {
        record.putValue(NULLABLE_INT_COLUMN, i);
      }
      record.putValue(INT_MV_COLUMN, new Object[]{i % 100, 500 + i % 100});
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setDefaultNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    _segment = ImmutableSegmentLoader.load(new File(TEMP_DIR, SEGMENT_NAME), ReadMode.mmap);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _segment.destroy();
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testApplyAndAttributesMatchesToCorrectDocIds() {
    // Candidates: every even docId (25k candidates -> three 10k doc-id batches)
    MutableRoaringBitmap candidates = new MutableRoaringBitmap();
    for (int i = 0; i < NUM_DOCS; i += 2) {
      candidates.add(i);
    }
    // Expected: candidates whose value (== docId) is divisible by 7
    MutableRoaringBitmap expected = new MutableRoaringBitmap();
    for (int i = 0; i < NUM_DOCS; i += 2) {
      if (i % 7 == 0) {
        expected.add(i);
      }
    }

    // Control: default projection operator (pulls one block at a time)
    assertEquals(runApplyAnd(candidates, "SELECT COUNT(*) FROM testTable WHERE MOD(intColumn, 7) = 0"),
        expected);

    // Regression: a projection operator that pulls all blocks from the doc-id source up front, like prefetching
    // implementations plugged in via ProjectionOperatorUtils. Matches must still be attributed to the docIds of the
    // block being processed, not to whatever the source's shared scratch buffer holds after the last pull.
    ProjectionOperatorUtils.setImplementation(PullAheadProjectionOperator::new);
    try {
      assertEquals(runApplyAnd(candidates, "SELECT COUNT(*) FROM testTable WHERE MOD(intColumn, 7) = 0"),
          expected);
    } finally {
      ProjectionOperatorUtils.setImplementation(new ProjectionOperatorUtils.DefaultImplementation());
    }
  }

  @Test
  public void testApplyAndNullHandlingAttributesMatchesToCorrectDocIds() {
    // Candidates: every even docId (25k candidates -> three 10k doc-id batches)
    MutableRoaringBitmap candidates = new MutableRoaringBitmap();
    for (int i = 0; i < NUM_DOCS; i += 2) {
      candidates.add(i);
    }
    // TRUE result: candidates whose value (== docId) is divisible by 7 and is not null (docId not divisible by 3)
    MutableRoaringBitmap expectedTrues = new MutableRoaringBitmap();
    // NULL result: candidates whose value is null (docId divisible by 3)
    MutableRoaringBitmap expectedNulls = new MutableRoaringBitmap();
    for (int i = 0; i < NUM_DOCS; i += 2) {
      if (i % 3 == 0) {
        expectedNulls.add(i);
      } else if (i % 7 == 0) {
        expectedTrues.add(i);
      }
    }

    // Control: default projection operator (pulls one block at a time)
    assertEquals(runApplyAndOnNullableColumn(candidates, false), expectedTrues);
    assertEquals(runApplyAndOnNullableColumn(candidates, true), expectedNulls);

    // Regression: same pull-ahead scenario as testApplyAndAttributesMatchesToCorrectDocIds, exercising the
    // null-handling-aware emission branches
    ProjectionOperatorUtils.setImplementation(PullAheadProjectionOperator::new);
    try {
      assertEquals(runApplyAndOnNullableColumn(candidates, false), expectedTrues);
      assertEquals(runApplyAndOnNullableColumn(candidates, true), expectedNulls);
    } finally {
      ProjectionOperatorUtils.setImplementation(new ProjectionOperatorUtils.DefaultImplementation());
    }
  }

  @Test
  public void testApplyAndMultiValueAttributesMatchesToCorrectDocIds() {
    // Candidates: every even docId (25k candidates -> three 10k doc-id batches)
    MutableRoaringBitmap candidates = new MutableRoaringBitmap();
    for (int i = 0; i < NUM_DOCS; i += 2) {
      candidates.add(i);
    }
    // Expected: candidates whose MV values [docId % 100, 500 + docId % 100] contain 8
    MutableRoaringBitmap expected = new MutableRoaringBitmap();
    for (int i = 0; i < NUM_DOCS; i += 2) {
      if (i % 100 == 8) {
        expected.add(i);
      }
    }
    // CAST keeps the multi-value-ness of its argument, driving the MV emission branches
    String query = "SELECT COUNT(*) FROM testTable WHERE CAST(intMvColumn AS LONG) = 8";

    // Control: default projection operator (pulls one block at a time)
    assertEquals(runApplyAnd(candidates, query), expected);

    // Regression: same pull-ahead scenario as testApplyAndAttributesMatchesToCorrectDocIds for the MV branches
    ProjectionOperatorUtils.setImplementation(PullAheadProjectionOperator::new);
    try {
      assertEquals(runApplyAnd(candidates, query), expected);
    } finally {
      ProjectionOperatorUtils.setImplementation(new ProjectionOperatorUtils.DefaultImplementation());
    }
  }

  private MutableRoaringBitmap runApplyAnd(MutableRoaringBitmap candidates, String query) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    Predicate predicate = queryContext.getFilter().getPredicate();
    ExpressionFilterOperator filterOperator =
        new ExpressionFilterOperator(_segment, queryContext, predicate, NUM_DOCS);
    ScanBasedDocIdIterator docIdIterator =
        (ScanBasedDocIdIterator) filterOperator.nextBlock().getBlockDocIdSet().iterator();
    return docIdIterator.applyAnd(candidates);
  }

  /// Evaluates `MOD(nullableIntColumn, 7) = 0` with null handling enabled over the given candidates. With
  /// `nullResult == false` returns the docIds where the predicate evaluates to true (exercising the
  /// null-bitmap-aware emission branches); with `nullResult == true` returns the docIds where it evaluates to null
  /// (exercising the `PredicateEvaluationResult.NULL` emission branch, which
  /// `ExpressionFilterOperator#getNulls()` drives in production).
  private MutableRoaringBitmap runApplyAndOnNullableColumn(MutableRoaringBitmap candidates, boolean nullResult) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE MOD(nullableIntColumn, 7) = 0");
    queryContext.setNullHandlingEnabled(true);
    Predicate predicate = queryContext.getFilter().getPredicate();
    if (!nullResult) {
      ExpressionFilterOperator filterOperator =
          new ExpressionFilterOperator(_segment, queryContext, predicate, NUM_DOCS);
      ScanBasedDocIdIterator docIdIterator =
          (ScanBasedDocIdIterator) filterOperator.nextBlock().getBlockDocIdSet().iterator();
      return docIdIterator.applyAnd(candidates);
    }
    // The NULL evaluation path is only reachable through the protected ExpressionFilterOperator#getNulls(), so
    // construct the iterator directly the same way getNulls() does
    ExpressionContext lhs = predicate.getLhs();
    Set<String> columns = new HashSet<>();
    lhs.getColumns(columns);
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    Map<String, ColumnContext> columnContextMap = new HashMap<>();
    for (String column : columns) {
      DataSource dataSource = _segment.getDataSource(column, queryContext.getSchema());
      dataSourceMap.put(column, dataSource);
      columnContextMap.put(column, ColumnContext.fromDataSource(dataSource));
    }
    TransformFunction transformFunction = TransformFunctionFactory.get(lhs, columnContextMap, queryContext);
    ExpressionScanDocIdIterator docIdIterator = new ExpressionScanDocIdIterator(transformFunction, null,
        dataSourceMap, NUM_DOCS, ExpressionScanDocIdIterator.PredicateEvaluationResult.NULL, queryContext);
    return docIdIterator.applyAnd(candidates);
  }

  /// Minimal stand-in for a prefetching projection operator: on the first call it pulls ALL blocks from the doc-id
  /// source (copying each block's docIds for value fetching, as such implementations must), leaving the source's
  /// shared scratch buffer holding the LAST block's docIds while earlier blocks are processed.
  private static class PullAheadProjectionOperator extends ProjectionOperator {
    private final Queue<ProjectionBlock> _blocks = new ArrayDeque<>();
    private boolean _pulled;

    PullAheadProjectionOperator(Map<String, DataSource> dataSourceMap,
        @Nullable BaseDocIdSetOperator docIdSetOperator, QueryContext queryContext) {
      super(dataSourceMap, docIdSetOperator, queryContext);
    }

    @Override
    protected ProjectionBlock getNextBlock() {
      assert _docIdSetOperator != null;
      if (!_pulled) {
        _pulled = true;
        DocIdSetBlock docIdSetBlock;
        while ((docIdSetBlock = _docIdSetOperator.nextBlock()) != null) {
          int length = docIdSetBlock.getLength();
          DataBlockCache dataBlockCache = new DataBlockCache(_dataFetcher);
          dataBlockCache.initNewBlock(Arrays.copyOf(docIdSetBlock.getDocIds(), length), length);
          _blocks.add(new ProjectionBlock(_dataSourceMap, dataBlockCache));
        }
      }
      return _blocks.poll();
    }
  }
}
