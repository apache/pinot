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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
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
 * Unit tests for {@link JsonIndexDistinctOperator}.
 */
public class JsonIndexDistinctOperatorTest {
  private static final String STRING_EXTRACT = "JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING')";
  private static final String STRING_EXTRACT_WITH_EMPTY_DEFAULT =
      "JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING', '')";
  private static final String STRING_EXTRACT_WITH_DEFAULT =
      "JSON_EXTRACT_INDEX(tags, '$.instance', 'STRING', 'missing')";
  private static final String INVALID_INT_DEFAULT_EXTRACT =
      "JSON_EXTRACT_INDEX(tags, '$.instance', 'INT', 'abc')";
  private static final String SAME_PATH_FILTER = "REGEXP_LIKE(\"$.instance\", '.*test.*')";
  private static final String CROSS_PATH_FILTER = "REGEXP_LIKE(\"$.env\", 'prod.*')";
  private static final String SAME_PATH_IS_NULL_FILTER = "\"$.instance\" IS NULL";

  @Test
  public void testSamePathJsonMatchUsesDistinctValuesFastPathForFourArgScalarForm() {
    QueryContext queryContext = distinctQuery(STRING_EXTRACT_WITH_EMPTY_DEFAULT, SAME_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.getMatchingDistinctValues("$.instance", SAME_PATH_FILTER))
        .thenReturn(Set.of("test-east", "test-west"));

    DistinctResultsBlock resultsBlock =
        buildOperator(queryContext, jsonIndexReader, bufferBitmap(0, 1), 2).nextBlock();

    assertEquals(extractValues(resultsBlock), Set.of("test-east", "test-west"));
    verify(jsonIndexReader).getMatchingDistinctValues("$.instance", SAME_PATH_FILTER);
    verify(jsonIndexReader, never()).getMatchingFlattenedDocsMap(any(), any());
    verify(jsonIndexReader, never()).convertFlattenedDocIdsToDocIds(any());
  }

  @Test
  public void testSamePathJsonMatchUsesDistinctValuesFastPathForThreeArgScalarForm() {
    QueryContext queryContext = distinctQuery(STRING_EXTRACT, SAME_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.getMatchingDistinctValues("$.instance", SAME_PATH_FILTER))
        .thenReturn(Set.of("test-east", "test-west"));

    DistinctResultsBlock resultsBlock =
        buildOperator(queryContext, jsonIndexReader, bufferBitmap(0, 1), 2).nextBlock();

    assertEquals(extractValues(resultsBlock), Set.of("test-east", "test-west"));
    verify(jsonIndexReader).getMatchingDistinctValues("$.instance", SAME_PATH_FILTER);
    verify(jsonIndexReader, never()).getMatchingFlattenedDocsMap(any(), any());
    verify(jsonIndexReader, never()).convertFlattenedDocIdsToDocIds(any());
  }

  @Test
  public void testDifferentPathJsonMatchIsAppliedAtDocLevel() {
    QueryContext queryContext = distinctQuery(STRING_EXTRACT, CROSS_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("prod-a", bitmap(100));
    flattenedDocsByValue.put("prod-b", bitmap(200));
    flattenedDocsByValue.put("other-doc", bitmap(300));
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", null)).thenReturn(flattenedDocsByValue);
    stubConvertedDocIds(jsonIndexReader, Map.of("prod-a", bitmap(0), "prod-b", bitmap(1), "other-doc", bitmap(2)));

    DistinctResultsBlock resultsBlock =
        buildOperator(queryContext, jsonIndexReader, bufferBitmap(0, 1), 3).nextBlock();

    assertEquals(extractValues(resultsBlock), Set.of("prod-a", "prod-b"));
    verify(jsonIndexReader).getMatchingFlattenedDocsMap("$.instance", null);
    verify(jsonIndexReader, never()).getMatchingFlattenedDocsMap("$.instance",
        "REGEXP_LIKE(\"$.env\", ''prod.*'')");
    verify(jsonIndexReader).convertFlattenedDocIdsToDocIds(any());
  }

  @Test
  public void testCanUseJsonIndexDistinctAllowsThreeArgScalarForm() {
    QueryContext queryContext = distinctQuery(STRING_EXTRACT, CROSS_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    assertTrue(JsonIndexDistinctOperator.canUseJsonIndexDistinct(indexSegment,
        queryContext.getSelectExpressions().get(0)));
  }

  @Test
  public void testCanUseJsonIndexDistinctAllowsFourArgScalarForm() {
    QueryContext queryContext = distinctQuery(STRING_EXTRACT_WITH_EMPTY_DEFAULT, CROSS_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    assertTrue(JsonIndexDistinctOperator.canUseJsonIndexDistinct(indexSegment,
        queryContext.getSelectExpressions().get(0)));
  }

  @Test
  public void testCanUseJsonIndexDistinctRejectsInvalidDefaultArgument() {
    QueryContext queryContext = distinctQuery(INVALID_INT_DEFAULT_EXTRACT, CROSS_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.isPathIndexed("$.instance")).thenReturn(true);
    IndexSegment indexSegment = buildCanUseIndexSegment(jsonIndexReader);

    assertFalse(JsonIndexDistinctOperator.canUseJsonIndexDistinct(indexSegment,
        queryContext.getSelectExpressions().get(0)));
  }

  @Test
  public void testFourArgAddsDefaultForDocsWithoutJsonPath() {
    QueryContext queryContext = distinctQuery(STRING_EXTRACT_WITH_DEFAULT, CROSS_PATH_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    Map<String, RoaringBitmap> flattenedDocsByValue = new HashMap<>();
    flattenedDocsByValue.put("prod-a", bitmap(100));
    flattenedDocsByValue.put("prod-b", bitmap(200));
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", null)).thenReturn(flattenedDocsByValue);
    stubConvertedDocIds(jsonIndexReader, Map.of("prod-a", bitmap(0), "prod-b", bitmap(1)));

    DistinctResultsBlock resultsBlock =
        buildOperator(queryContext, jsonIndexReader, bufferBitmap(0, 1, 2), 3).nextBlock();

    assertEquals(extractValues(resultsBlock), Set.of("prod-a", "prod-b", "missing"));
  }

  @Test
  public void testSamePathIsNullStillAddsDefaultForMissingPath() {
    QueryContext queryContext = distinctQuery(STRING_EXTRACT_WITH_DEFAULT, SAME_PATH_IS_NULL_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", SAME_PATH_IS_NULL_FILTER)).thenReturn(
        new HashMap<>());

    DistinctResultsBlock resultsBlock =
        buildOperator(queryContext, jsonIndexReader, bufferBitmap(2), 3).nextBlock();

    assertEquals(extractValues(resultsBlock), Set.of("missing"));
    verify(jsonIndexReader).getMatchingFlattenedDocsMap("$.instance", SAME_PATH_IS_NULL_FILTER);
    verify(jsonIndexReader).convertFlattenedDocIdsToDocIds(any());
  }

  @Test
  public void testMissingPathWithoutDefaultThrows() {
    QueryContext queryContext = distinctQuery(STRING_EXTRACT, SAME_PATH_IS_NULL_FILTER);

    JsonIndexReader jsonIndexReader = mock(JsonIndexReader.class);
    when(jsonIndexReader.getMatchingFlattenedDocsMap("$.instance", SAME_PATH_IS_NULL_FILTER)).thenReturn(
        new HashMap<>());

    RuntimeException exception = expectThrows(RuntimeException.class,
        () -> buildOperator(queryContext, jsonIndexReader, bufferBitmap(2), 3).nextBlock());

    assertTrue(exception.getMessage().contains("Illegal Json Path"));
  }

  private static QueryContext distinctQuery(String expression, String filterJsonString) {
    return QueryContextConverterUtils.getQueryContext(
        "SELECT DISTINCT " + expression + " AS tag_value FROM myTable WHERE JSON_MATCH(tags, '"
            + filterJsonString.replace("'", "''") + "')");
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
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getJsonIndex()).thenReturn(jsonIndexReader);

    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getDataSourceNullable("tags")).thenReturn(dataSource);
    return indexSegment;
  }

  private static JsonIndexDistinctOperator buildOperator(QueryContext queryContext, JsonIndexReader jsonIndexReader,
      MutableRoaringBitmap filterBitmap, int numDocs) {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(numDocs);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getJsonIndex()).thenReturn(jsonIndexReader);

    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(indexSegment.getSegmentName()).thenReturn("testSegment");
    when(indexSegment.getDataSource(eq("tags"), any())).thenReturn(dataSource);
    when(indexSegment.getDataSourceNullable("tags")).thenReturn(dataSource);

    return new JsonIndexDistinctOperator(indexSegment, new SegmentContext(indexSegment), queryContext,
        new StaticBitmapFilterOperator(numDocs, filterBitmap));
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

  private static Set<String> extractValues(DistinctResultsBlock resultsBlock) {
    List<Object[]> rows = resultsBlock.getRows();
    return rows.stream().map(row -> (String) row[0]).collect(Collectors.toSet());
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
