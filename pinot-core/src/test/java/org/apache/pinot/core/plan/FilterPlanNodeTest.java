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
package org.apache.pinot.core.plan;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.IsNullPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityRadiusPredicate;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.BaseDictIdBasedRegexpLikePredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.upsert.UpsertUtils;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class FilterPlanNodeTest {

  @Test
  public void testConsistentSnapshot()
      throws Exception {
    IndexSegment segment = mock(IndexSegment.class);
    SegmentMetadata meta = mock(SegmentMetadata.class);
    when(segment.getSegmentMetadata()).thenReturn(meta);
    ThreadSafeMutableRoaringBitmap bitmap = new ThreadSafeMutableRoaringBitmap();
    when(segment.getValidDocIds()).thenReturn(bitmap);
    AtomicInteger numDocs = new AtomicInteger(0);
    when(meta.getTotalDocs()).then((Answer<Integer>) invocationOnMock -> numDocs.get());
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getFilter()).thenReturn(null);

    numDocs.set(3);
    bitmap.add(0);
    bitmap.add(1);
    bitmap.add(2);

    // Continuously update the last value by moving it one doc id forward
    // Follow the order of MutableIndexSegmentImpl: first add the row, update the doc count and then change the
    // validDocId bitmap
    Thread updater = new Thread(() -> {
      for (int i = 3; i < 10_000_000; i++) {
        numDocs.incrementAndGet();
        bitmap.replace(i - 2, i);
      }
    });
    updater.start();

    // Result should be invariant - always exactly 3 docs
    for (int i = 0; i < 10_000; i++) {
      SegmentContext segmentContext = new SegmentContext(segment);
      segmentContext.setDocIdsSnapshot(UpsertUtils.getQueryableDocIdsSnapshotFromSegment(segment));
      assertEquals(getNumberOfFilteredDocs(segmentContext, queryContext), 3);
    }

    updater.join();
  }

  @Test
  public void testMutableVectorFallbackReasonForUnsupportedBackend()
      throws Exception {
    Method method = FilterPlanNode.class.getDeclaredMethod("getVectorFallbackReason", VectorIndexConfig.class,
        boolean.class);
    method.setAccessible(true);

    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_FLAT", 4, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, java.util.Map.of("nlist", "2"));

    String reason = (String) method.invoke(null, config, true);
    assertEquals(reason, "ivf_flat_mutable_segment_unavailable");
  }

  @Test
  public void testMutableIvfPqVectorFallbackReasonForUnsupportedBackend()
      throws Exception {
    Method method = FilterPlanNode.class.getDeclaredMethod("getVectorFallbackReason", VectorIndexConfig.class,
        boolean.class);
    method.setAccessible(true);

    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_PQ", 4, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        java.util.Map.of("nlist", "2", "pqM", "2", "pqNbits", "8"));

    String reason = (String) method.invoke(null, config, true);
    assertEquals(reason, "ivf_pq_mutable_segment_unavailable");
  }

  @Test
  public void testMutableVectorFallbackReasonForMissingIndex()
      throws Exception {
    Method method = FilterPlanNode.class.getDeclaredMethod("getVectorFallbackReason", VectorIndexConfig.class,
        boolean.class);
    method.setAccessible(true);

    String reason = (String) method.invoke(null, null, true);
    assertEquals(reason, "vector_index_missing_on_mutable_segment");
  }

  private int getNumberOfFilteredDocs(SegmentContext segmentContext, QueryContext queryContext) {
    FilterPlanNode node = new FilterPlanNode(segmentContext, queryContext);
    BaseFilterOperator op = node.run();
    int numDocsFiltered = 0;
    FilterBlock block = op.nextBlock();
    BlockDocIdSet blockIds = block.getBlockDocIdSet();
    BlockDocIdIterator it = blockIds.iterator();
    while (it.next() != Constants.EOF) {
      numDocsFiltered++;
    }
    return numDocsFiltered;
  }

  // -----------------------------------------------------------------------
  // Upsert doc-ids snapshot as a required vector candidate filter
  // -----------------------------------------------------------------------

  private static final String VECTOR_COLUMN = "embedding";
  private static final float[] QUERY_VECTOR = {1.0f, 0.0f};

  private static QueryContext mockQueryContext(FilterContext filter) {
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getFilter()).thenReturn(filter);
    return queryContext;
  }

  private static FilterContext vectorFilter(int topK) {
    return FilterContext.forPredicate(
        new VectorSimilarityPredicate(ExpressionContext.forIdentifier(VECTOR_COLUMN), QUERY_VECTOR, topK));
  }

  private static DataSource mockVectorDataSource(@Nullable VectorIndexReader vectorIndex,
      @Nullable ForwardIndexReader<?> forwardIndex) {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getVectorIndex()).thenReturn(vectorIndex);
    Mockito.doReturn(forwardIndex).when(dataSource).getForwardIndex();
    return dataSource;
  }

  private static IndexSegment mockVectorSegment(int numDocs, DataSource vectorDataSource) {
    IndexSegment segment = mock(IndexSegment.class);
    SegmentMetadata metadata = mock(SegmentMetadata.class);
    when(metadata.getTotalDocs()).thenReturn(numDocs);
    when(segment.getSegmentMetadata()).thenReturn(metadata);
    when(segment.getDataSource(Mockito.eq(VECTOR_COLUMN), Mockito.any())).thenReturn(vectorDataSource);
    return segment;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ForwardIndexReader<?> mockVectorForwardIndex(float[][] vectors) {
    ForwardIndexReader mockReader = mock(ForwardIndexReader.class);
    ForwardIndexReaderContext mockContext = mock(ForwardIndexReaderContext.class);
    when(mockReader.createContext()).thenReturn(mockContext);
    for (int i = 0; i < vectors.length; i++) {
      when(mockReader.getFloatMV(Mockito.eq(i), Mockito.any())).thenReturn(vectors[i]);
    }
    return mockReader;
  }

  private static List<Integer> collectDocIds(BaseFilterOperator op) {
    List<Integer> docIds = new ArrayList<>();
    BlockDocIdIterator it = op.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = it.next()) != Constants.EOF) {
      docIds.add(docId);
    }
    return docIds;
  }

  @Test
  public void testUpsertSnapshotRoutesVectorPredicateToFilteredAnn() {
    // FULL-upsert scenario: obsolete docs {0, 1} are physically nearest, valid snapshot is {2, 3}.
    // The filtered ANN search must receive the snapshot and the unfiltered search must never run,
    // even without any metadata predicate.
    FilterAwareVectorIndexReader vectorIndex = mock(FilterAwareVectorIndexReader.class);
    when(vectorIndex.supportsPreFilter()).thenReturn(true);
    when(vectorIndex.getDocIds(Mockito.eq(QUERY_VECTOR), Mockito.eq(2), Mockito.any(ImmutableRoaringBitmap.class)))
        .thenReturn(MutableRoaringBitmap.bitmapOf(2, 3));
    when(vectorIndex.getDocIds(QUERY_VECTOR, 2)).thenReturn(MutableRoaringBitmap.bitmapOf(0, 1));

    IndexSegment segment = mockVectorSegment(100, mockVectorDataSource(vectorIndex, null));
    SegmentContext segmentContext = new SegmentContext(segment);
    MutableRoaringBitmap snapshot = MutableRoaringBitmap.bitmapOf(2, 3);
    segmentContext.setDocIdsSnapshot(snapshot);

    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(vectorFilter(2)));
    BaseFilterOperator op = planNode.run();
    // Mutating the query-scoped snapshot after planning must not leak into the vector search: the
    // planner must hand the operator a defensive copy
    snapshot.add(50);

    assertEquals(collectDocIds(op), List.of(2, 3));

    ArgumentCaptor<ImmutableRoaringBitmap> bitmapCaptor = ArgumentCaptor.forClass(ImmutableRoaringBitmap.class);
    verify(vectorIndex).getDocIds(Mockito.eq(QUERY_VECTOR), Mockito.eq(2), bitmapCaptor.capture());
    assertEquals(bitmapCaptor.getValue(), ImmutableRoaringBitmap.bitmapOf(2, 3));
    assertNotSame(bitmapCaptor.getValue(), snapshot,
        "The vector operator must receive a defensive copy of the query-scoped snapshot");
    verify(vectorIndex, never()).getDocIds(QUERY_VECTOR, 2);
  }

  @Test
  public void testEmptyUpsertSnapshotShortCircuitsWithoutVectorSearch() {
    FilterAwareVectorIndexReader vectorIndex = mock(FilterAwareVectorIndexReader.class);
    IndexSegment segment = mockVectorSegment(100, mockVectorDataSource(vectorIndex, null));
    SegmentContext segmentContext = new SegmentContext(segment);
    segmentContext.setDocIdsSnapshot(new MutableRoaringBitmap());

    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(vectorFilter(2)));
    BaseFilterOperator op = planNode.run();

    assertTrue(op instanceof EmptyFilterOperator, "Empty snapshot should plan an EmptyFilterOperator");
    assertTrue(collectDocIds(op).isEmpty());
    verifyNoInteractions(vectorIndex);
  }

  @Test
  public void testNullUpsertSnapshotPreservesUnfilteredAnn() {
    FilterAwareVectorIndexReader vectorIndex = mock(FilterAwareVectorIndexReader.class);
    when(vectorIndex.getDocIds(QUERY_VECTOR, 2)).thenReturn(MutableRoaringBitmap.bitmapOf(0, 1));

    IndexSegment segment = mockVectorSegment(100, mockVectorDataSource(vectorIndex, null));
    SegmentContext segmentContext = new SegmentContext(segment);

    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(vectorFilter(2)));
    assertEquals(collectDocIds(planNode.run()), List.of(0, 1));

    verify(vectorIndex).getDocIds(QUERY_VECTOR, 2);
    verify(vectorIndex, never())
        .getDocIds(Mockito.any(float[].class), Mockito.anyInt(), Mockito.any(ImmutableRoaringBitmap.class));
  }

  @Test
  public void testUpsertSnapshotWithNonFilterAwareIndexFallsBackToExactScan() {
    // The mutable HNSW index (and any other non-filter-aware reader) cannot honor the required
    // snapshot, so the planner must route to the exact allowed-doc scan over the forward index --
    // never to unfiltered ANN.
    VectorIndexReader vectorIndex = mock(VectorIndexReader.class);
    float[][] vectors = {
        {1.0f, 0.0f},   // doc 0 - obsolete, nearest
        {1.0f, 0.0f},   // doc 1 - obsolete, nearest
        {0.0f, 1.0f},   // doc 2 - valid
        {0.0f, -1.0f},  // doc 3 - valid
    };

    IndexSegment segment = mockVectorSegment(4, mockVectorDataSource(vectorIndex, mockVectorForwardIndex(vectors)));
    SegmentContext segmentContext = new SegmentContext(segment);
    segmentContext.setDocIdsSnapshot(MutableRoaringBitmap.bitmapOf(2, 3));

    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(vectorFilter(2)));
    assertEquals(collectDocIds(planNode.run()), List.of(2, 3));

    verifyNoInteractions(vectorIndex);
  }

  @Test
  public void testUpsertSnapshotClampedToPlannedDocRange() {
    // Under ConsistencyMode.NONE the snapshot can contain a doc id at/beyond the planned numDocs whose row
    // data is still being written -- the planner must clamp the required bitmap so the vector search (and
    // its rerank/threshold refinement) can never read past the planned doc range
    FilterAwareVectorIndexReader vectorIndex = mock(FilterAwareVectorIndexReader.class);
    when(vectorIndex.supportsPreFilter()).thenReturn(true);
    when(vectorIndex.getDocIds(Mockito.eq(QUERY_VECTOR), Mockito.eq(2), Mockito.any(ImmutableRoaringBitmap.class)))
        .thenReturn(MutableRoaringBitmap.bitmapOf(2, 3));

    IndexSegment segment = mockVectorSegment(4, mockVectorDataSource(vectorIndex, null));
    SegmentContext segmentContext = new SegmentContext(segment);
    segmentContext.setDocIdsSnapshot(MutableRoaringBitmap.bitmapOf(2, 3, 4, 100));

    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(vectorFilter(2)));
    assertEquals(collectDocIds(planNode.run()), List.of(2, 3));

    ArgumentCaptor<ImmutableRoaringBitmap> bitmapCaptor = ArgumentCaptor.forClass(ImmutableRoaringBitmap.class);
    verify(vectorIndex).getDocIds(Mockito.eq(QUERY_VECTOR), Mockito.eq(2), bitmapCaptor.capture());
    assertEquals(bitmapCaptor.getValue(), ImmutableRoaringBitmap.bitmapOf(2, 3),
        "Doc ids at/beyond the planned numDocs must be clamped out of the required bitmap");
  }

  @Test
  public void testMutableSegmentDoesNotWireOptionalMetadataPreFilter() {
    // On mutable segments the optional metadata pre-filter stays disabled (the strategy is conservative
    // because the mutable filtered search opens a near-real-time reader per query); without an upsert
    // snapshot the search must remain unfiltered even though the reader is filter-aware
    int numDocs = 200_000;
    FilterAwareVectorIndexReader vectorIndex = mock(FilterAwareVectorIndexReader.class);
    when(vectorIndex.supportsPreFilter()).thenReturn(true);
    when(vectorIndex.getDocIds(QUERY_VECTOR, 2)).thenReturn(MutableRoaringBitmap.bitmapOf(0, 1));

    IndexSegment segment = mockVectorSegment(numDocs, mockVectorDataSource(vectorIndex, null));
    when(segment.getSegmentMetadata().isMutableSegment()).thenReturn(true);

    String metadataColumn = "metadataCol";
    MutableRoaringBitmap nullBitmap = MutableRoaringBitmap.bitmapOf(0, 1);
    nullBitmap.add(1000L, 2497L);
    NullValueVectorReader nullValueVector = mock(NullValueVectorReader.class);
    when(nullValueVector.getNullBitmap()).thenReturn(nullBitmap);
    DataSource metadataDataSource = mock(DataSource.class);
    when(metadataDataSource.getNullValueVector()).thenReturn(nullValueVector);
    when(segment.getDataSource(Mockito.eq(metadataColumn), Mockito.any())).thenReturn(metadataDataSource);

    SegmentContext segmentContext = new SegmentContext(segment);
    FilterContext filter = FilterContext.forAnd(List.of(vectorFilter(2),
        FilterContext.forPredicate(new IsNullPredicate(ExpressionContext.forIdentifier(metadataColumn)))));
    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(filter));
    assertEquals(collectDocIds(planNode.run()), List.of(0, 1));

    verify(vectorIndex).getDocIds(QUERY_VECTOR, 2);
    verify(vectorIndex, never())
        .getDocIds(Mockito.any(float[].class), Mockito.anyInt(), Mockito.any(ImmutableRoaringBitmap.class));
  }

  private static FilterContext radiusFilter(float threshold) {
    return FilterContext.forPredicate(new VectorSimilarityRadiusPredicate(
        ExpressionContext.forIdentifier(VECTOR_COLUMN), QUERY_VECTOR, threshold));
  }

  @Test
  public void testUpsertSnapshotAppliedToRadiusPredicate() {
    // The radius operator must receive the snapshot and use the filtered candidate retrieval
    FilterAwareVectorIndexReader vectorIndex = mock(FilterAwareVectorIndexReader.class);
    when(vectorIndex.supportsPreFilter()).thenReturn(true);
    when(vectorIndex.getDocIds(Mockito.eq(QUERY_VECTOR), Mockito.anyInt(), Mockito.any(ImmutableRoaringBitmap.class)))
        .thenReturn(MutableRoaringBitmap.bitmapOf(2, 3));
    float[][] vectors = {
        {1.0f, 0.0f},   // doc 0 - obsolete, distance 0
        {1.0f, 0.0f},   // doc 1 - obsolete, distance 0
        {0.0f, 1.0f},   // doc 2 - valid, distance 2
        {0.0f, -1.0f},  // doc 3 - valid, distance 2
    };

    IndexSegment segment = mockVectorSegment(4, mockVectorDataSource(vectorIndex, mockVectorForwardIndex(vectors)));
    SegmentContext segmentContext = new SegmentContext(segment);
    segmentContext.setDocIdsSnapshot(MutableRoaringBitmap.bitmapOf(2, 3));

    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(radiusFilter(2.5f)));
    assertEquals(collectDocIds(planNode.run()), List.of(2, 3));

    ArgumentCaptor<ImmutableRoaringBitmap> bitmapCaptor = ArgumentCaptor.forClass(ImmutableRoaringBitmap.class);
    verify(vectorIndex).getDocIds(Mockito.eq(QUERY_VECTOR), Mockito.anyInt(), bitmapCaptor.capture());
    assertEquals(bitmapCaptor.getValue(), ImmutableRoaringBitmap.bitmapOf(2, 3));
    verify(vectorIndex, never()).getDocIds(Mockito.any(float[].class), Mockito.anyInt());
  }

  @Test
  public void testEmptyUpsertSnapshotShortCircuitsRadiusPredicate() {
    FilterAwareVectorIndexReader vectorIndex = mock(FilterAwareVectorIndexReader.class);
    IndexSegment segment = mockVectorSegment(100, mockVectorDataSource(vectorIndex, null));
    SegmentContext segmentContext = new SegmentContext(segment);
    segmentContext.setDocIdsSnapshot(new MutableRoaringBitmap());

    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(radiusFilter(2.5f)));
    BaseFilterOperator op = planNode.run();
    assertTrue(op instanceof EmptyFilterOperator);
    verifyNoInteractions(vectorIndex);
  }

  @Test
  public void testUpsertSnapshotWithNonFilterAwareIndexAndNoForwardIndexFails() {
    // A required snapshot with a vector index that cannot honor it and no forward index for the exact-scan
    // fallback must fail the query loudly -- never silently run unfiltered ANN
    VectorIndexReader vectorIndex = mock(VectorIndexReader.class);
    IndexSegment segment = mockVectorSegment(4, mockVectorDataSource(vectorIndex, null));
    SegmentContext segmentContext = new SegmentContext(segment);
    segmentContext.setDocIdsSnapshot(MutableRoaringBitmap.bitmapOf(2, 3));

    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(vectorFilter(2)));
    expectThrows(IllegalStateException.class, planNode::run);
    verifyNoInteractions(vectorIndex);
  }

  @Test
  public void testUpsertSnapshotIntersectedWithMetadataPreFilter() {
    // AND(vector, IS_NULL(metadataCol)) with a metadata bitmap selective enough for the strategy to
    // choose FILTER_THEN_ANN: the reader must receive the intersection of the required snapshot and
    // the optional metadata bitmap.
    int numDocs = 200_000;
    FilterAwareVectorIndexReader vectorIndex = mock(FilterAwareVectorIndexReader.class);
    when(vectorIndex.supportsPreFilter()).thenReturn(true);
    when(vectorIndex.getDocIds(Mockito.eq(QUERY_VECTOR), Mockito.eq(2), Mockito.any(ImmutableRoaringBitmap.class)))
        .thenReturn(MutableRoaringBitmap.bitmapOf(2));

    IndexSegment segment = mockVectorSegment(numDocs, mockVectorDataSource(vectorIndex, null));

    // Metadata sibling: IS_NULL over a column whose null bitmap is {2} plus {1000..2496}
    // (cardinality 1498 => selectivity 0.00749, within the FILTER_THEN_ANN range of the strategy)
    String metadataColumn = "metadataCol";
    MutableRoaringBitmap nullBitmap = MutableRoaringBitmap.bitmapOf(2);
    nullBitmap.add(1000L, 2497L);
    NullValueVectorReader nullValueVector = mock(NullValueVectorReader.class);
    when(nullValueVector.getNullBitmap()).thenReturn(nullBitmap);
    DataSource metadataDataSource = mock(DataSource.class);
    when(metadataDataSource.getNullValueVector()).thenReturn(nullValueVector);
    when(segment.getDataSource(Mockito.eq(metadataColumn), Mockito.any())).thenReturn(metadataDataSource);

    SegmentContext segmentContext = new SegmentContext(segment);
    segmentContext.setDocIdsSnapshot(MutableRoaringBitmap.bitmapOf(2, 3));

    FilterContext filter = FilterContext.forAnd(List.of(vectorFilter(2),
        FilterContext.forPredicate(new IsNullPredicate(ExpressionContext.forIdentifier(metadataColumn)))));
    FilterPlanNode planNode = new FilterPlanNode(segmentContext, mockQueryContext(filter));
    assertEquals(collectDocIds(planNode.run()), List.of(2));

    ArgumentCaptor<ImmutableRoaringBitmap> bitmapCaptor = ArgumentCaptor.forClass(ImmutableRoaringBitmap.class);
    verify(vectorIndex).getDocIds(Mockito.eq(QUERY_VECTOR), Mockito.eq(2), bitmapCaptor.capture());
    assertEquals(bitmapCaptor.getValue(), ImmutableRoaringBitmap.bitmapOf(2),
        "Reader must receive the intersection of the upsert snapshot and the metadata pre-filter");
    verify(vectorIndex, never()).getDocIds(QUERY_VECTOR, 2);
  }

  @Test
  public void regexpLikeUsesIFSTEvaluatorWhenIFSTAndInvertedAvailable()
      throws Exception {
    PredicateEvaluator evaluator = runRegexpLikeAndGetEvaluator(
        true, true, false, true, false, true);
    assertTrue(evaluator.isDictionaryBased());
    assertTrue(evaluator instanceof BaseDictIdBasedRegexpLikePredicateEvaluator);
  }

  @Test
  public void regexpLikeFallsBackToRawWhenIFSTPresentButNoDictConsumer()
      throws Exception {
    PredicateEvaluator evaluator = runRegexpLikeAndGetEvaluator(
        true, true, false, true, false, false);
    assertFalse(evaluator.isDictionaryBased());
  }

  @Test
  public void regexpLikeUsesFSTEvaluatorWhenFSTAndInvertedAvailable()
      throws Exception {
    PredicateEvaluator evaluator = runRegexpLikeAndGetEvaluator(
        false, false, true, true, false, true);
    assertTrue(evaluator.isDictionaryBased());
    assertTrue(evaluator instanceof BaseDictIdBasedRegexpLikePredicateEvaluator);
  }

  @Test
  public void regexpLikeFallsBackToRawWhenFSTPresentButNoDictConsumer()
      throws Exception {
    PredicateEvaluator evaluator = runRegexpLikeAndGetEvaluator(
        false, false, true, true, false, false);
    assertFalse(evaluator.isDictionaryBased());
  }

  @Test
  public void regexpLikeUsesIFSTEvaluatorWhenIFSTAndDictEncodedForward()
      throws Exception {
    PredicateEvaluator evaluator = runRegexpLikeAndGetEvaluator(
        true, true, false, true, true, false);
    assertTrue(evaluator.isDictionaryBased());
  }

  private PredicateEvaluator runRegexpLikeAndGetEvaluator(boolean caseInsensitive, boolean hasIFST, boolean hasFST,
      boolean hasDictionary, boolean forwardDictEncoded, boolean hasInverted)
      throws Exception {
    String column = "col";
    DataSource dataSource =
        mockStringDataSource(column, hasIFST, hasFST, hasDictionary, forwardDictEncoded, hasInverted);
    RegexpLikePredicate predicate = caseInsensitive
        ? new RegexpLikePredicate(ExpressionContext.forIdentifier(column), "pat", "i")
        : new RegexpLikePredicate(ExpressionContext.forIdentifier(column), "pat");
    FilterContext filterContext = FilterContext.forPredicate(predicate);

    IndexSegment segment = mock(IndexSegment.class);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getTotalDocs()).thenReturn(1);
    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segment.getDataSource(Mockito.eq(column), Mockito.any())).thenReturn(dataSource);

    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getFilter()).thenReturn(filterContext);
    when(queryContext.isIndexUseAllowed(Mockito.any(DataSource.class), Mockito.any(FieldConfig.IndexType.class)))
        .thenReturn(true);

    SegmentContext segmentContext = new SegmentContext(segment);

    FilterPlanNode planNode = new FilterPlanNode(segmentContext, queryContext);
    try {
      planNode.run();
    } catch (Exception ignored) {
    }

    Pair<Predicate, PredicateEvaluator> pair = planNode.getPredicateEvaluators().get(0);
    return pair.getRight();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static DataSource mockStringDataSource(String column, boolean hasIFST, boolean hasFST,
      boolean hasDictionary, boolean forwardDictEncoded, boolean hasInverted) {
    DataSource dataSource = Mockito.mock(DataSource.class);
    DataSourceMetadata metadata = Mockito.mock(DataSourceMetadata.class);
    when(metadata.getDataType()).thenReturn(DataType.STRING);
    when(metadata.isSorted()).thenReturn(false);
    when(metadata.getFieldSpec()).thenReturn(new DimensionFieldSpec(column, DataType.STRING, true));
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    when(dataSource.getColumnName()).thenReturn(column);

    ForwardIndexReader forwardIndex = Mockito.mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(forwardDictEncoded);
    when(forwardIndex.getStoredType()).thenReturn(DataType.STRING);
    when(dataSource.getForwardIndex()).thenReturn(forwardIndex);

    if (hasDictionary) {
      Dictionary dictionary = Mockito.mock(Dictionary.class);
      when(dictionary.length()).thenReturn(0);
      when(dataSource.getDictionary()).thenReturn(dictionary);
    } else {
      when(dataSource.getDictionary()).thenReturn(null);
    }

    InvertedIndexReader invertedReader = hasInverted ? Mockito.mock(InvertedIndexReader.class) : null;
    TextIndexReader ifstReader = hasIFST ? mockTextIndexReader() : null;
    TextIndexReader fstReader = hasFST ? mockTextIndexReader() : null;
    when(dataSource.getInvertedIndex()).thenReturn(invertedReader);
    when(dataSource.getRangeIndex()).thenReturn(null);
    when(dataSource.getIFSTIndex()).thenReturn(ifstReader);
    when(dataSource.getFSTIndex()).thenReturn(fstReader);

    return dataSource;
  }

  private static TextIndexReader mockTextIndexReader() {
    TextIndexReader reader = Mockito.mock(TextIndexReader.class);
    ImmutableRoaringBitmap emptyBitmap = ImmutableRoaringBitmap.bitmapOf();
    when(reader.getDictIds(Mockito.anyString())).thenReturn(emptyBitmap);
    return reader;
  }
}
