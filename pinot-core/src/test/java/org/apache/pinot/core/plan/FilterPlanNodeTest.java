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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.IsNullPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
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
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class FilterPlanNodeTest {

  @DataProvider(name = "nestedVectorSubtreeShapes")
  public Object[][] nestedVectorSubtreeShapes() {
    return new Object[][]{{"AND"}, {"OR"}, {"NOT_OR"}};
  }

  @Test(dataProvider = "nestedVectorSubtreeShapes")
  public void testNestedVectorSimilaritySubtreeIsNeverMaterializedAsMetadata(String shape) {
    FilterAwareVectorIndexReader directVectorReader = mock(FilterAwareVectorIndexReader.class);
    FilterAwareVectorIndexReader nestedVectorReader = mock(FilterAwareVectorIndexReader.class);
    when(directVectorReader.supportsPreFilter()).thenReturn(true);
    when(nestedVectorReader.supportsPreFilter()).thenReturn(true);
    when(nestedVectorReader.getDocIds(Mockito.any(float[].class), Mockito.anyInt(),
        any(ImmutableRoaringBitmap.class))).thenReturn(bitmapOf(1));
    DataSource directVectorDataSource = mock(DataSource.class);
    DataSource nestedVectorDataSource = mock(DataSource.class);
    when(directVectorDataSource.getVectorIndex()).thenReturn(directVectorReader);
    when(nestedVectorDataSource.getVectorIndex()).thenReturn(nestedVectorReader);
    DataSource metadataDataSource = mock(DataSource.class);
    NullValueVectorReader nullValueVectorReader = mock(NullValueVectorReader.class);
    when(nullValueVectorReader.getNullBitmap()).thenReturn(bitmapOf(1));
    when(metadataDataSource.getNullValueVector()).thenReturn(nullValueVectorReader);

    IndexSegment segment = mock(IndexSegment.class);
    SegmentMetadata metadata = mock(SegmentMetadata.class);
    when(metadata.getTotalDocs()).thenReturn(1_000);
    when(segment.getSegmentMetadata()).thenReturn(metadata);
    when(segment.getDataSource("embeddingA", null)).thenReturn(directVectorDataSource);
    when(segment.getDataSource("embeddingB", null)).thenReturn(nestedVectorDataSource);
    when(segment.getDataSource("tenant", null)).thenReturn(metadataDataSource);

    FilterContext directVector = FilterContext.forPredicate(new VectorSimilarityPredicate(
        ExpressionContext.forIdentifier("embeddingA"), new float[]{1.0f, 0.0f}, 2));
    FilterContext nestedVector = FilterContext.forPredicate(new VectorSimilarityPredicate(
        ExpressionContext.forIdentifier("embeddingB"), new float[]{1.0f, 0.0f}, 2));
    FilterContext metadataFilter = FilterContext.forPredicate(
        new IsNullPredicate(ExpressionContext.forIdentifier("tenant")));
    FilterContext nestedSubtree;
    switch (shape) {
      case "AND":
        nestedSubtree = FilterContext.forAnd(List.of(nestedVector, metadataFilter));
        break;
      case "OR":
        nestedSubtree = FilterContext.forOr(List.of(nestedVector, metadataFilter));
        break;
      case "NOT_OR":
        nestedSubtree = FilterContext.forNot(FilterContext.forOr(List.of(nestedVector, metadataFilter)));
        break;
      default:
        throw new IllegalStateException("Unexpected shape: " + shape);
    }
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getFilter()).thenReturn(FilterContext.forAnd(List.of(directVector, nestedSubtree)));

    SegmentContext segmentContext = new SegmentContext(segment);
    segmentContext.setDocIdsSnapshot(bitmapOf(0, 1, 2, 3));
    new FilterPlanNode(segmentContext, queryContext).run();

    verify(nestedVectorReader, never()).getDocIds(Mockito.any(float[].class), Mockito.anyInt());
    verify(nestedVectorReader, never()).getDocIds(Mockito.any(float[].class), Mockito.anyInt(),
        any(ImmutableRoaringBitmap.class));
    verify(directVectorReader, never()).getDocIds(Mockito.any(float[].class), Mockito.anyInt(),
        any(ImmutableRoaringBitmap.class));
  }

  @Test
  public void testNonRestrictedMetadataScopePreservesAdaptivePostFilterBehavior() {
    float[] queryVector = {1.0f, 0.0f};
    FilterAwareVectorIndexReader vectorReader = mock(FilterAwareVectorIndexReader.class);
    when(vectorReader.supportsPreFilter()).thenReturn(true);
    when(vectorReader.getDocIds(queryVector, 2)).thenReturn(bitmapOf(0, 1));

    DataSource vectorDataSource = mock(DataSource.class);
    when(vectorDataSource.getVectorIndex()).thenReturn(vectorReader);
    DataSource metadataDataSource = mock(DataSource.class);
    NullValueVectorReader nullValueVectorReader = mock(NullValueVectorReader.class);
    when(nullValueVectorReader.getNullBitmap()).thenReturn(bitmapOf(2, 3));
    when(metadataDataSource.getNullValueVector()).thenReturn(nullValueVectorReader);

    IndexSegment segment = mock(IndexSegment.class);
    SegmentMetadata metadata = mock(SegmentMetadata.class);
    when(metadata.getTotalDocs()).thenReturn(4);
    when(segment.getSegmentMetadata()).thenReturn(metadata);
    when(segment.getDataSource("embedding", null)).thenReturn(vectorDataSource);
    when(segment.getDataSource("tenant", null)).thenReturn(metadataDataSource);

    FilterContext vectorFilter = FilterContext.forPredicate(new VectorSimilarityPredicate(
        ExpressionContext.forIdentifier("embedding"), queryVector, 2));
    FilterContext metadataFilter = FilterContext.forPredicate(
        new IsNullPredicate(ExpressionContext.forIdentifier("tenant")));
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getFilter()).thenReturn(FilterContext.forAnd(List.of(vectorFilter, metadataFilter)));

    Assert.assertTrue(getFilteredDocIds(new FilterPlanNode(new SegmentContext(segment), queryContext).run()).isEmpty());
    verify(vectorReader).getDocIds(queryVector, 2);
    verify(vectorReader, never()).getDocIds(eq(queryVector), eq(2), any(ImmutableRoaringBitmap.class));
  }

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

  private static MutableRoaringBitmap getFilteredDocIds(BaseFilterOperator operator) {
    MutableRoaringBitmap docIds = new MutableRoaringBitmap();
    BlockDocIdIterator iterator = operator.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      docIds.add(docId);
    }
    return docIds;
  }

  private static MutableRoaringBitmap bitmapOf(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    return bitmap;
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
