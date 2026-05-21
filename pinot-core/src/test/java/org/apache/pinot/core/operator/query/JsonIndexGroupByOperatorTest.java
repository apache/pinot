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
package org.apache.pinot.core.operator.query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapCollection;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/**
 * Unit tests for {@link JsonIndexGroupByOperator}.
 */
public class JsonIndexGroupByOperatorTest {
  private static final String EXTRACT_INDEX = "JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING')";
  private static final String EXTRACT_INDEX_WITH_DEFAULT =
      "JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING', 'missing')";
  private static final String SAME_PATH_FILTER = "REGEXP_LIKE(\"$.instance\", '.*test.*')";
  private static final String CROSS_PATH_FILTER = "REGEXP_LIKE(\"$.env\", 'prod.*')";
  private static final String SAME_PATH_IS_NULL_FILTER = "\"$.instance\" IS NULL";

  @Test
  public void testGroupByWithoutFilterCountsAllDocs() {
    QueryContext queryContext = groupByQuery(EXTRACT_INDEX, null);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("east", bitmap(100, 101));
    flattenedDocsByValue.put("west", bitmap(200));
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", null)).thenReturn(flattenedDocsByValue);
    stubConvertedDocIds(jsonIndexReader, Map.of(
        "east", bitmap(0, 1),
        "west", bitmap(2)));

    GroupByResultsBlock block = buildOperatorMatchAll(queryContext, jsonIndexReader, 3).nextBlock();

    assertEquals(extractCounts(block), Map.of("east", 2L, "west", 1L));
  }

  @Test
  public void testSamePathJsonMatchPushedDownNoMissingPathGroup() {
    QueryContext queryContext = groupByQuery(EXTRACT_INDEX, SAME_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    // The pushed-down filter restricts which flattened docs come back.
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("test-east", bitmap(10));
    flattenedDocsByValue.put("test-west", bitmap(20, 21));
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", SAME_PATH_FILTER))
        .thenReturn(flattenedDocsByValue);
    stubConvertedDocIds(jsonIndexReader, Map.of(
        "test-east", bitmap(0),
        "test-west", bitmap(1, 2)));

    GroupByResultsBlock block = buildOperator(queryContext, jsonIndexReader, bufferBitmap(0, 1, 2), 5).nextBlock();

    // Even though only 3 of 5 docs are covered, no missing-path group is emitted because the same-path JSON_MATCH
    // filter cannot match docs that lack the path.
    assertEquals(extractCounts(block), Map.of("test-east", 1L, "test-west", 2L));
  }

  @Test
  public void testCrossPathFilterAppliedAsResidual() {
    QueryContext queryContext = groupByQuery(EXTRACT_INDEX, CROSS_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("prod-a", bitmap(100));
    flattenedDocsByValue.put("prod-b", bitmap(200));
    flattenedDocsByValue.put("other", bitmap(300));
    // The cross-path filter is NOT pushed down so getMatchingFlattenedDocsMap is called with null.
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", null)).thenReturn(flattenedDocsByValue);
    stubConvertedDocIds(jsonIndexReader, Map.of(
        "prod-a", bitmap(0),
        "prod-b", bitmap(1),
        "other", bitmap(2)));

    // Filter operator says docs 0 and 1 pass the cross-path filter.
    GroupByResultsBlock block = buildOperator(queryContext, jsonIndexReader, bufferBitmap(0, 1), 3).nextBlock();

    // "other" is filtered out; both surviving docs have a value at the path so no missing-path group.
    assertEquals(extractCounts(block), Map.of("prod-a", 1L, "prod-b", 1L));
    verify(jsonIndexReader).getMatchingFlattenedDocsMap("$.instance", null);
  }

  @Test
  public void testMissingPathDocsUseDefaultLiteral() {
    QueryContext queryContext = groupByQuery(EXTRACT_INDEX_WITH_DEFAULT, null);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("east", bitmap(0));
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", null)).thenReturn(flattenedDocsByValue);
    stubConvertedDocIds(jsonIndexReader, Map.of("east", bitmap(0)));

    // 3 total docs, only doc 0 has a value at the path; docs 1 and 2 should land in the "missing" group.
    GroupByResultsBlock block = buildOperatorMatchAll(queryContext, jsonIndexReader, 3).nextBlock();

    assertEquals(extractCounts(block), Map.of("east", 1L, "missing", 2L));
  }

  @Test
  public void testMissingPathDocsUseNullWhenNullHandlingEnabled() {
    QueryContext queryContext = groupByQuery(EXTRACT_INDEX, null, true);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("east", bitmap(0));
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", null)).thenReturn(flattenedDocsByValue);
    stubConvertedDocIds(jsonIndexReader, Map.of("east", bitmap(0)));

    GroupByResultsBlock block = buildOperatorMatchAll(queryContext, jsonIndexReader, 3).nextBlock();

    Map<Object, Long> counts = extractCountsAllowingNull(block);
    assertEquals(counts.get("east"), Long.valueOf(1L));
    assertEquals(counts.get(null), Long.valueOf(2L));
    assertEquals(counts.size(), 2);
  }

  @Test
  public void testMissingPathDocsThrowWhenNoDefaultAndNullHandlingDisabled() {
    QueryContext queryContext = groupByQuery(EXTRACT_INDEX, null);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("east", bitmap(0));
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", null)).thenReturn(flattenedDocsByValue);
    stubConvertedDocIds(jsonIndexReader, Map.of("east", bitmap(0)));

    RuntimeException ex = expectThrows(RuntimeException.class,
        () -> buildOperatorMatchAll(queryContext, jsonIndexReader, 3).nextBlock());
    assertTrue(ex.getMessage().contains("Illegal Json Path"));
  }

  @Test
  public void testSamePathIsNullEmitsOnlyMissingPathGroup() {
    QueryContext queryContext = groupByQuery(EXTRACT_INDEX_WITH_DEFAULT, SAME_PATH_IS_NULL_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    // The operator does NOT push the IS_NULL filter into the index (it would rely on undocumented "returns empty"
    // SPI behavior). Instead, it asks for all flattened docs at the path and lets the row-side filter operator
    // produce the IS_NULL bitmap.
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("east", bitmap(100));
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", null)).thenReturn(flattenedDocsByValue);
    stubConvertedDocIds(jsonIndexReader, Map.of("east", bitmap(0)));

    // Filter operator returns docs 1 and 2 (docs without a value at the path), satisfying the IS_NULL filter.
    GroupByResultsBlock block = buildOperator(queryContext, jsonIndexReader, bufferBitmap(1, 2), 3).nextBlock();

    // Doc 0 has the "east" value but is filtered out by IS_NULL. Docs 1 and 2 have no value and fall into "missing".
    assertEquals(extractCounts(block), Map.of("missing", 2L));
    verify(jsonIndexReader).getMatchingFlattenedDocsMap("$.instance", null);
    verify(jsonIndexReader, never()).getMatchingFlattenedDocsMap("$.instance", SAME_PATH_IS_NULL_FILTER);
  }

  @Test
  public void testMultikeyArrayPathSplitsCountsAcrossElements() {
    // A document with an implicit-array-traversal path lands in multiple value bitmaps after flattening.
    // The operator reports per-element counts ("multikey" semantics), diverging from scalar jsonExtractScalar
    // evaluation. This test locks in the documented behavior.
    QueryContext queryContext = groupByQuery(EXTRACT_INDEX, null);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("east", bitmap(10, 11));
    flattenedDocsByValue.put("west", bitmap(20));
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", null)).thenReturn(flattenedDocsByValue);
    // Doc 0 has BOTH values "east" and "west" (array traversal), doc 1 has only "east".
    stubConvertedDocIds(jsonIndexReader, Map.of(
        "east", bitmap(0, 1),
        "west", bitmap(0)));

    GroupByResultsBlock block = buildOperatorMatchAll(queryContext, jsonIndexReader, 2).nextBlock();

    // Multikey: doc 0 contributes to both groups → counts sum to 3, exceeding total docs (2).
    // The missing-path bucket is correctly 0 because both docs appear in the covered set (union).
    assertEquals(extractCounts(block), Map.of("east", 2L, "west", 1L));
  }

  @Test
  public void testEmptyFilterShortCircuitsToEmptyBlock() {
    QueryContext queryContext = groupByQuery(EXTRACT_INDEX, CROSS_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);

    // Filter operator returns no docs.
    GroupByResultsBlock block = buildOperator(queryContext, jsonIndexReader, bufferBitmap(), 3).nextBlock();

    assertEquals(extractCounts(block), Map.of());
    // We never need to consult the index when the filter rules out every doc.
    verify(jsonIndexReader, never()).getMatchingFlattenedDocsMap(any(), any());
  }

  @Test
  public void testCanUseAcceptsThreeAndFourArgForms() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    assertTrue(JsonIndexGroupByOperator.canUse(indexSegment, groupByQuery(EXTRACT_INDEX, null), matchAll(1000)));
    assertTrue(JsonIndexGroupByOperator.canUse(indexSegment, groupByQuery(EXTRACT_INDEX_WITH_DEFAULT, null),
        matchAll(1000)));
  }

  @Test
  public void testCanUseRejectsMultipleAggregations() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT " + EXTRACT_INDEX + " AS k, COUNT(*), MAX(rating) FROM myTable GROUP BY k");
    assertFalse(JsonIndexGroupByOperator.canUse(indexSegment, queryContext, matchAll(1000)));
  }

  @Test
  public void testCanUseRejectsNonCountAggregation() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT " + EXTRACT_INDEX + " AS k, SUM(rating) FROM myTable GROUP BY k");
    assertFalse(JsonIndexGroupByOperator.canUse(indexSegment, queryContext, matchAll(1000)));
  }

  @Test
  public void testCanUseRejectsCountColumn() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT " + EXTRACT_INDEX + " AS k, COUNT(rating) FROM myTable GROUP BY k "
            + "OPTION(enableNullHandling=true)");
    assertFalse(JsonIndexGroupByOperator.canUse(indexSegment, queryContext, matchAll(1000)));
  }

  @Test
  public void testCanUseRejectsHaving() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT " + EXTRACT_INDEX + " AS k, COUNT(*) AS c FROM myTable GROUP BY k HAVING COUNT(*) > 10");
    assertFalse(JsonIndexGroupByOperator.canUse(indexSegment, queryContext, matchAll(1000)));
  }

  @Test
  public void testCanUseRejectsMultipleGroupByKeys() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT " + EXTRACT_INDEX + " AS k, env, COUNT(*) FROM myTable GROUP BY k, env");
    assertFalse(JsonIndexGroupByOperator.canUse(indexSegment, queryContext, matchAll(1000)));
  }

  @Test
  public void testCanUseRejectsWhenPathNotIndexed() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(false);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    assertFalse(JsonIndexGroupByOperator.canUse(indexSegment, groupByQuery(EXTRACT_INDEX, null), matchAll(1000)));
  }

  /// Selectivity-gate tests below. SELECTIVITY_THRESHOLD = 2.0 (placeholder until JMH bench lands).

  @Test
  public void testCanUseRejectsHighCardinalityRelativeToMatchedDocs() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    // Path cardinality 1000, segment total 100 → D >> SELECTIVITY_THRESHOLD * M, bail.
    when(jsonIndexReader.getDistinctValueCountForPath("$.instance")).thenReturn(1000L);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader, 100);

    assertFalse(JsonIndexGroupByOperator.canUse(indexSegment, groupByQuery(EXTRACT_INDEX, null), matchAll(100)));
  }

  @Test
  public void testCanUseAcceptsAtSelectivityBoundary() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    // D = floor(SELECTIVITY_THRESHOLD * M) = 2 * 100 = 200 → exactly at the boundary, route to the index.
    when(jsonIndexReader.getDistinctValueCountForPath("$.instance")).thenReturn(200L);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader, 100);

    assertTrue(JsonIndexGroupByOperator.canUse(indexSegment, groupByQuery(EXTRACT_INDEX, null), matchAll(100)));
  }

  @Test
  public void testCanUseRejectsJustAboveSelectivityBoundary() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    // D = SELECTIVITY_THRESHOLD * M + 1 → just above the boundary, bail.
    when(jsonIndexReader.getDistinctValueCountForPath("$.instance")).thenReturn(201L);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader, 100);

    assertFalse(JsonIndexGroupByOperator.canUse(indexSegment, groupByQuery(EXTRACT_INDEX, null), matchAll(100)));
  }

  @Test
  public void testCanUseRejectsZeroMatchingDocs() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    when(jsonIndexReader.getDistinctValueCountForPath("$.instance")).thenReturn(5L);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader, 100);

    // Empty WHERE bitmap → M = 0 → no docs to count; the default operator handles this trivially.
    assertFalse(JsonIndexGroupByOperator.canUse(indexSegment, groupByQuery(EXTRACT_INDEX, null),
        new StaticBitmapFilterOperator(100, new MutableRoaringBitmap())));
  }

  @Test
  public void testCanUseAcceptsWhenPathHasNoValues() {
    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    // D = 0 (path not present in the index) → still cheaper than parsing every matched doc.
    when(jsonIndexReader.getDistinctValueCountForPath("$.instance")).thenReturn(0L);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader, 100);

    assertTrue(JsonIndexGroupByOperator.canUse(indexSegment, groupByQuery(EXTRACT_INDEX, null), matchAll(100)));
  }

  private static QueryContext groupByQuery(String groupKeyExpression, String jsonMatchFilter) {
    return groupByQuery(groupKeyExpression, jsonMatchFilter, false);
  }

  private static QueryContext groupByQuery(String groupKeyExpression, String jsonMatchFilter,
      boolean enableNullHandling) {
    StringBuilder sql = new StringBuilder("SELECT ");
    sql.append(groupKeyExpression).append(" AS tag_value, COUNT(*) FROM myTable");
    if (jsonMatchFilter != null) {
      sql.append(" WHERE JSON_MATCH(tags, '").append(jsonMatchFilter.replace("'", "''")).append("')");
    }
    sql.append(" GROUP BY tag_value");
    if (enableNullHandling) {
      sql.append(" OPTION(enableNullHandling=true)");
    }
    return QueryContextConverterUtils.getQueryContext(sql.toString());
  }

  private static void stubConvertedDocIds(JsonIndexReader jsonIndexReader,
      Map<String, RoaringBitmap> convertedDocIds) {
    doAnswer(invocation -> {
      @SuppressWarnings("unchecked")
      Map<String, RoaringBitmap> docsByValue = (Map<String, RoaringBitmap>) invocation.getArgument(0);
      docsByValue.clear();
      docsByValue.putAll(convertedDocIds);
      return null;
    }).when(jsonIndexReader).convertFlattenedDocIdsToDocIds(any());
  }

  private static IndexSegment buildCanUseIndexSegment(JsonIndexReader jsonIndexReader) {
    return buildCanUseIndexSegment(jsonIndexReader, 1000);
  }

  private static IndexSegment buildCanUseIndexSegment(JsonIndexReader jsonIndexReader, int totalDocs) {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getJsonIndex()).thenReturn(jsonIndexReader);

    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(totalDocs);

    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getDataSourceNullable("tags")).thenReturn(dataSource);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    return indexSegment;
  }

  private static BaseFilterOperator matchAll(int numDocs) {
    return new MatchAllFilterOperator(numDocs);
  }

  private static JsonIndexGroupByOperator buildOperator(QueryContext queryContext, JsonIndexReader jsonIndexReader,
      MutableRoaringBitmap filterBitmap, int numDocs) {
    return buildOperator(queryContext, jsonIndexReader,
        new StaticBitmapFilterOperator(numDocs, filterBitmap), numDocs);
  }

  /**
   * Used by tests that simulate a query with no WHERE clause — the FilterPlanNode would produce a match-all operator.
   */
  private static JsonIndexGroupByOperator buildOperatorMatchAll(QueryContext queryContext,
      JsonIndexReader jsonIndexReader, int numDocs) {
    return buildOperator(queryContext, jsonIndexReader, new MatchAllFilterOperator(numDocs), numDocs);
  }

  private static JsonIndexGroupByOperator buildOperator(QueryContext queryContext, JsonIndexReader jsonIndexReader,
      BaseFilterOperator filterOperator, int numDocs) {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(numDocs);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getJsonIndex()).thenReturn(jsonIndexReader);

    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(indexSegment.getSegmentName()).thenReturn("testSegment");
    when(indexSegment.getDataSource(eq("tags"), any())).thenReturn(dataSource);
    when(indexSegment.getDataSourceNullable("tags")).thenReturn(dataSource);

    return new JsonIndexGroupByOperator(indexSegment, new SegmentContext(indexSegment), queryContext, filterOperator);
  }

  private static RoaringBitmap bitmap(int... docIds) {
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int docId : docIds) {
      bitmap.add(docId);
    }
    return bitmap;
  }

  private static MutableRoaringBitmap bufferBitmap(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int docId : docIds) {
      bitmap.add(docId);
    }
    return bitmap;
  }

  private static Map<String, Long> extractCounts(GroupByResultsBlock block) {
    Map<String, Long> result = new HashMap<>();
    List<IntermediateRecord> records = block.getIntermediateRecords();
    if (records == null) {
      return result;
    }
    for (IntermediateRecord record : records) {
      Object[] values = record._record.getValues();
      result.put((String) values[0], (Long) values[1]);
    }
    return result;
  }

  private static Map<Object, Long> extractCountsAllowingNull(GroupByResultsBlock block) {
    Map<Object, Long> result = new HashMap<>();
    List<IntermediateRecord> records = block.getIntermediateRecords();
    if (records == null) {
      return result;
    }
    for (IntermediateRecord record : records) {
      Object[] values = record._record.getValues();
      result.put(values[0], (Long) values[1]);
    }
    return result;
  }

  private static final class MatchAllFilterOperator extends BaseFilterOperator {
    MatchAllFilterOperator(int numDocs) {
      super(numDocs, false);
    }

    @Override
    public boolean isResultMatchingAll() {
      return true;
    }

    @Override
    public List<Operator> getChildOperators() {
      return List.of();
    }

    @Override
    protected org.apache.pinot.core.common.BlockDocIdSet getTrues() {
      throw new UnsupportedOperationException("Match-all should never reach getTrues");
    }

    @Override
    public String toExplainString() {
      return "MATCH_ALL";
    }
  }

  private static final class StaticBitmapFilterOperator extends BaseFilterOperator {
    private final MutableRoaringBitmap _bitmap;

    StaticBitmapFilterOperator(int numDocs, MutableRoaringBitmap bitmap) {
      super(numDocs, false);
      _bitmap = bitmap;
    }

    @Override
    public boolean canProduceBitmaps() {
      return true;
    }

    @Override
    public BitmapCollection getBitmaps() {
      return new BitmapCollection(_numDocs, false, _bitmap);
    }

    @Override
    public List<Operator> getChildOperators() {
      return List.of();
    }

    @Override
    protected org.apache.pinot.core.common.BlockDocIdSet getTrues() {
      throw new UnsupportedOperationException("Bitmap path only");
    }

    @Override
    public String toExplainString() {
      return "STATIC_BITMAP_FILTER";
    }
  }
}
