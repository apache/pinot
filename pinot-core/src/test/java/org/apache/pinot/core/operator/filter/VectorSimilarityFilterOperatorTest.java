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
package org.apache.pinot.core.operator.filter;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NprobeAware;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mockito;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link VectorSimilarityFilterOperator} with multi-backend support.
 */
public class VectorSimilarityFilterOperatorTest {

  @Test
  public void testBackwardCompatibleSearch() {
    // Original 3-arg constructor, no query options
    MutableRoaringBitmap expectedResult = new MutableRoaringBitmap();
    expectedResult.add(1);
    expectedResult.add(3);
    expectedResult.add(5);

    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 2.0f, 3.0f};
    when(mockReader.getDocIds(queryVector, 3)).thenReturn(expectedResult);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 3);

    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate, 100);

    Assert.assertEquals(operator.getNumMatchingDocs(), 3);
    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertTrue(result.contains(1));
    Assert.assertTrue(result.contains(3));
    Assert.assertTrue(result.contains(5));
  }

  @Test
  public void testNprobeDispatchToIvfReader() {
    // Create a mock reader that implements both VectorIndexReader and NprobeAware
    NprobeAwareVectorReader mockReader = mock(NprobeAwareVectorReader.class);
    float[] queryVector = {1.0f, 2.0f};
    MutableRoaringBitmap expectedResult = new MutableRoaringBitmap();
    expectedResult.add(0);
    expectedResult.add(2);
    when(mockReader.getDocIds(queryVector, 2)).thenReturn(expectedResult);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    VectorSearchParams params = new VectorSearchParams(16, false, null);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate,
        100, params, null);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    // Verify nprobe was set before search
    verify(mockReader).setNprobe(16);
  }

  @Test
  public void testNprobeNotCalledOnHnswReader() {
    // Plain VectorIndexReader (HNSW-like) should not get setNprobe called
    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 2.0f};
    MutableRoaringBitmap expectedResult = new MutableRoaringBitmap();
    expectedResult.add(0);
    when(mockReader.getDocIds(queryVector, 1)).thenReturn(expectedResult);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 1);

    VectorSearchParams params = new VectorSearchParams(16, false, null);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate,
        100, params, null);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 1);
    // Reader is NOT NprobeAware, so no setNprobe should be called
  }

  @Test
  public void testExactRerankRefinesResults() {
    // ANN returns 4 candidates, rerank should refine to top-2
    MutableRoaringBitmap annResult = new MutableRoaringBitmap();
    annResult.add(0); // near
    annResult.add(1); // far
    annResult.add(2); // farthest
    annResult.add(3); // very near

    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    // When rerank is enabled, searchCount = maxCandidates(20) since topK*10 > topK
    when(mockReader.getDocIds(Mockito.eq(queryVector), Mockito.anyInt())).thenReturn(annResult);

    // Forward index provides actual vectors for reranking
    float[][] vectors = {
        {0.9f, 0.1f},  // doc 0 - distance = 0.02
        {0.0f, 1.0f},  // doc 1 - distance = 2.0
        {-1.0f, 0.0f}, // doc 2 - distance = 4.0
        {1.0f, 0.0f},  // doc 3 - distance = 0.0
    };

    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    VectorSearchParams params = new VectorSearchParams(null, true, null);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate,
        10, params, mockForward);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    Assert.assertEquals(result.getCardinality(), 2);
    // Should contain doc 3 (distance=0.0) and doc 0 (distance=0.02), NOT doc 1 or doc 2
    Assert.assertTrue(result.contains(3), "doc 3 (distance=0.0) should be in results");
    Assert.assertTrue(result.contains(0), "doc 0 (distance=0.02) should be in results");
    Assert.assertFalse(result.contains(1), "doc 1 (distance=2.0) should not be in top-2");
    Assert.assertFalse(result.contains(2), "doc 2 (distance=4.0) should not be in top-2");
  }

  @Test
  public void testRerankWithMaxCandidates() {
    // Verify that maxCandidates affects the number of ANN candidates requested
    MutableRoaringBitmap annResult = new MutableRoaringBitmap();
    annResult.add(0);

    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    // maxCandidates=50, topK=5 -> should search for 50
    when(mockReader.getDocIds(queryVector, 50)).thenReturn(annResult);

    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(new float[][]{{1.0f, 0.0f}});

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 5);

    VectorSearchParams params = new VectorSearchParams(null, true, 50);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate,
        100, params, mockForward);

    operator.getBitmaps();
    verify(mockReader).getDocIds(queryVector, 50);
  }

  @Test
  public void testRerankDisabledSearchesForTopK() {
    MutableRoaringBitmap annResult = new MutableRoaringBitmap();
    annResult.add(0);
    annResult.add(1);

    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    // Without rerank, should search for exactly topK
    when(mockReader.getDocIds(queryVector, 2)).thenReturn(annResult);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    VectorSearchParams params = new VectorSearchParams(null, false, null);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate,
        100, params, null);

    operator.getBitmaps();
    verify(mockReader).getDocIds(queryVector, 2);
  }

  @Test
  public void testRerankWithoutForwardIndexSkipsRerank() {
    // If rerank is enabled but no forward index reader, skip rerank
    MutableRoaringBitmap annResult = new MutableRoaringBitmap();
    annResult.add(0);
    annResult.add(1);

    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    float[] queryVector = {1.0f, 0.0f};
    when(mockReader.getDocIds(Mockito.eq(queryVector), Mockito.anyInt())).thenReturn(annResult);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 2);

    VectorSearchParams params = new VectorSearchParams(null, true, null);
    // Note: forwardIndexReader is null -- rerank should be skipped gracefully
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate,
        100, params, null);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    // Should still return the ANN results without reranking
    Assert.assertEquals(result.getCardinality(), 2);
  }

  @Test
  public void testCanProduceBitmaps() {
    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, new float[]{1.0f}, 1);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate, 1);
    Assert.assertTrue(operator.canProduceBitmaps());
  }

  @Test
  public void testExplainString() {
    VectorIndexReader mockReader = mock(VectorIndexReader.class);
    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, new float[]{1.0f, 2.0f}, 5);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate, 100);
    String explain = operator.toExplainString();
    Assert.assertTrue(explain.contains("VECTOR_SIMILARITY_INDEX"));
    Assert.assertTrue(explain.contains("embedding"));
    Assert.assertTrue(explain.contains("5"));
  }

  @Test
  public void testNprobeAndRerankCombined() {
    // Test that both nprobe and rerank work together
    NprobeAwareVectorReader mockReader = mock(NprobeAwareVectorReader.class);
    float[] queryVector = {1.0f, 0.0f};
    MutableRoaringBitmap annResult = new MutableRoaringBitmap();
    annResult.add(0);
    annResult.add(1);
    when(mockReader.getDocIds(Mockito.eq(queryVector), Mockito.anyInt())).thenReturn(annResult);

    float[][] vectors = {
        {1.0f, 0.0f},  // doc 0 - distance = 0.0
        {0.0f, 1.0f},  // doc 1 - distance = 2.0
    };
    ForwardIndexReader<?> mockForward = createMockForwardIndexReader(vectors);

    ExpressionContext lhs = ExpressionContext.forIdentifier("embedding");
    VectorSimilarityPredicate predicate = new VectorSimilarityPredicate(lhs, queryVector, 1);

    VectorSearchParams params = new VectorSearchParams(8, true, 20);
    VectorSimilarityFilterOperator operator = new VectorSimilarityFilterOperator(mockReader, predicate,
        100, params, mockForward);

    ImmutableRoaringBitmap result = operator.getBitmaps().reduce();
    // After rerank with topK=1, only the closest doc should remain
    Assert.assertEquals(result.getCardinality(), 1);
    Assert.assertTrue(result.contains(0));
    // Verify nprobe was set
    verify(mockReader).setNprobe(8);
  }

  /**
   * Interface combining VectorIndexReader and NprobeAware for mocking IVF_FLAT readers.
   */
  interface NprobeAwareVectorReader extends VectorIndexReader, NprobeAware {
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private ForwardIndexReader<?> createMockForwardIndexReader(float[][] vectors) {
    ForwardIndexReader mockReader = mock(ForwardIndexReader.class);
    ForwardIndexReaderContext mockContext = mock(ForwardIndexReaderContext.class);
    try {
      when(mockReader.createContext()).thenReturn(mockContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    when(mockReader.isSingleValue()).thenReturn(false);
    when(mockReader.isDictionaryEncoded()).thenReturn(false);
    when(mockReader.getStoredType()).thenReturn(DataType.FLOAT);

    for (int i = 0; i < vectors.length; i++) {
      when(mockReader.getFloatMV(Mockito.eq(i), Mockito.any())).thenReturn(vectors[i]);
    }
    return mockReader;
  }
}
