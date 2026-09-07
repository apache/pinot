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
package org.apache.pinot.segment.local.segment.index.readers.vector;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


/// Tests the shared Lucene constant-score scaffolding and identity contract for Pinot doc-id bitmap filters.
public class BaseFilterQueryTest {

  @Test
  public void testConstantScoreScaffoldingAndCacheOptOut()
      throws Exception {
    TestQuery query = new TestQuery(MutableRoaringBitmap.bitmapOf(2, 4), new Object(),
        DocIdSetIterator.range(1, 3));
    Weight weight = query.createWeight(null, ScoreMode.COMPLETE, 2.5F);

    assertFalse(weight.isCacheable(null));
    Scorer scorer = weight.scorer(null);
    assertNotNull(scorer);
    assertSame(scorer.iterator(), query._iterator);
    assertEquals(scorer.docID(), -1);
    assertEquals(scorer.iterator().nextDoc(), 1);
    assertEquals(scorer.docID(), 1);
    assertEquals(scorer.score(), 2.5F);
    assertEquals(scorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS), 2.5F);
  }

  @Test
  public void testLeafWithoutMatchesReturnsNoScorer()
      throws Exception {
    TestQuery query = new TestQuery(MutableRoaringBitmap.bitmapOf(1), new Object(), null);
    assertNull(query.createWeight(null, ScoreMode.COMPLETE_NO_SCORES, 1.0F).scorer(null));
  }

  @Test
  public void testIdentityEqualityIncludesBitmapClassAndDocIdSource() {
    MutableRoaringBitmap docIds = MutableRoaringBitmap.bitmapOf(1, 3);
    Object source = new Object();
    TestQuery query = new TestQuery(docIds, source, DocIdSetIterator.empty());
    TestQuery equivalent = new TestQuery(docIds, source, DocIdSetIterator.empty());

    assertEquals(query, query);
    assertEquals(query, equivalent);
    assertEquals(query.hashCode(), equivalent.hashCode());
    assertNotEquals(query, new TestQuery(docIds, new Object(), DocIdSetIterator.empty()));
    assertNotEquals(query, new TestQuery(docIds.clone(), source, DocIdSetIterator.empty()));
    assertNotEquals(query, new OtherQuery(docIds));
    assertNotEquals(query, null);
    assertEquals(query.toString("ignored"), "TestQuery(cardinality=2)");
  }

  @Test
  public void testVisitDelegatesToLeafVisitor() {
    TestQuery query = new TestQuery(MutableRoaringBitmap.bitmapOf(1), new Object(), DocIdSetIterator.empty());
    AtomicReference<Query> visited = new AtomicReference<>();
    query.visit(new QueryVisitor() {
      @Override
      public void visitLeaf(Query leafQuery) {
        visited.set(leafQuery);
      }
    });
    assertSame(visited.get(), query);
  }

  private static class TestQuery extends BaseFilterQuery {
    private final Object _source;
    private final DocIdSetIterator _iterator;

    TestQuery(ImmutableRoaringBitmap docIds, Object source, DocIdSetIterator iterator) {
      super(docIds);
      _source = source;
      _iterator = iterator;
    }

    @Override
    protected DocIdSetIterator createLeafIterator(LeafReaderContext context) {
      return _iterator;
    }

    @Override
    protected boolean equalsDocIdSource(BaseFilterQuery other) {
      return _source == ((TestQuery) other)._source;
    }
  }

  private static class OtherQuery extends BaseFilterQuery {
    OtherQuery(ImmutableRoaringBitmap docIds) {
      super(docIds);
    }

    @Override
    protected DocIdSetIterator createLeafIterator(LeafReaderContext context) {
      return DocIdSetIterator.empty();
    }
  }
}
