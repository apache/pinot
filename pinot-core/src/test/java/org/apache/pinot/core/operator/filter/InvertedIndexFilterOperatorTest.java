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

import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/**
 * Targeted tests for {@link InvertedIndexFilterOperator#getNumMatchingDocs()}. Five tests cover every
 * distinct code branch (SV loop, MV switch arms {0, 2, default}, and exclusive arithmetic)
 */
public class InvertedIndexFilterOperatorTest {
  private static final int NUM_DOCS = 1000;

  // SV path: union cardinality equals sum of per-bitmap cardinalities under the disjoint invariant
  @Test
  public void testSvSumCardinalities() {
    ImmutableRoaringBitmap b0 = bitmap(0, 1, 2, 3);
    ImmutableRoaringBitmap b1 = bitmap(4, 5, 6);
    ImmutableRoaringBitmap b2 = bitmap(7, 8, 9, 10, 11);
    InvertedIndexFilterOperator operator =
        newOperator(true, false, new int[]{0, 1, 2}, new ImmutableRoaringBitmap[]{b0, b1, b2});
    assertEquals(operator.getNumMatchingDocs(), 12);
  }

  // SV exclusive path: numDocs minus sum of non-matching cardinalities; also exercises getNonMatchingDictIds
  @Test
  public void testSvExclusive() {
    ImmutableRoaringBitmap b0 = bitmap(0, 1, 2, 3);
    ImmutableRoaringBitmap b1 = bitmap(4, 5, 6);
    InvertedIndexFilterOperator operator =
        newOperator(true, true, new int[]{0, 1}, new ImmutableRoaringBitmap[]{b0, b1});
    assertEquals(operator.getNumMatchingDocs(), NUM_DOCS - 7);
  }

  // MV {@code case 2} arm: pairwise {@code ImmutableRoaringBitmap.orCardinality} with overlap
  @Test
  public void testMvCase2Pairwise() {
    ImmutableRoaringBitmap b0 = bitmap(0, 1, 2, 3, 4);
    ImmutableRoaringBitmap b1 = bitmap(3, 4, 5, 6);
    InvertedIndexFilterOperator operator =
        newOperator(false, false, new int[]{0, 1}, new ImmutableRoaringBitmap[]{b0, b1});
    // Union {0,1,2,3,4,5,6} -> 7. Sum-of-cardinalities would give 9 (wrong for MV).
    assertEquals(operator.getNumMatchingDocs(), 7);
  }

  // MV default arm: regression guard — must return UNION cardinality, never sum
  @Test
  public void testMvUnionWithOverlap() {
    ImmutableRoaringBitmap b0 = bitmap(0, 1, 2, 3, 4);
    ImmutableRoaringBitmap b1 = bitmap(3, 4, 5, 6);
    ImmutableRoaringBitmap b2 = bitmap(5, 6, 7);
    InvertedIndexFilterOperator operator =
        newOperator(false, false, new int[]{0, 1, 2}, new ImmutableRoaringBitmap[]{b0, b1, b2});
    // Union {0..7} -> 8. Sum-of-cardinalities would give 12.
    assertEquals(operator.getNumMatchingDocs(), 8);
  }

  // Empty dictIds + exclusive: an easy-to-miss edge — must return numDocs, not 0. Also exercises MV case 0
  @Test
  public void testMvEmptyDictIdsExclusive() {
    InvertedIndexFilterOperator operator =
        newOperator(false, true, new int[0], new ImmutableRoaringBitmap[0]);
    assertEquals(operator.getNumMatchingDocs(), NUM_DOCS);
  }

  // ----- helpers -----

  private static ImmutableRoaringBitmap bitmap(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    for (int docId : docIds) {
      bitmap.add(docId);
    }
    return bitmap;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static InvertedIndexFilterOperator newOperator(boolean singleValue, boolean exclusive, int[] dictIds,
      ImmutableRoaringBitmap[] perDictIdBitmaps) {
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.isNullHandlingEnabled()).thenReturn(false);

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.isSingleValue()).thenReturn(singleValue);

    InvertedIndexReader reader = mock(InvertedIndexReader.class);
    for (int i = 0; i < dictIds.length; i++) {
      when(reader.getDocIds(dictIds[i])).thenReturn(perDictIdBitmaps[i]);
    }

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    when(dataSource.getInvertedIndex()).thenReturn(reader);

    PredicateEvaluator predicateEvaluator = mock(PredicateEvaluator.class);
    when(predicateEvaluator.isExclusive()).thenReturn(exclusive);
    if (exclusive) {
      when(predicateEvaluator.getNonMatchingDictIds()).thenReturn(dictIds);
    } else {
      when(predicateEvaluator.getMatchingDictIds()).thenReturn(dictIds);
    }

    return new InvertedIndexFilterOperator(queryContext, predicateEvaluator, dataSource, NUM_DOCS);
  }
}
