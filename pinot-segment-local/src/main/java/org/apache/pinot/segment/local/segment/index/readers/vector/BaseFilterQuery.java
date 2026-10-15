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

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Base class for Lucene [Query] implementations that accept only documents whose Pinot doc id is present
/// in a [ImmutableRoaringBitmap]. Used to implement pre-filter ANN search by restricting HNSW graph
/// traversal to the filtered document set.
///
/// Because Lucene uses its own internal doc ids (which differ from Pinot doc ids), subclasses supply the
/// per-leaf iterator that maps Lucene doc ids to Pinot doc ids before testing membership in the bitmap
/// (via a doc-id translator, doc values, etc.). This class owns the constant-score weight/scorer
/// scaffolding, identity-based equality, and cache opt-out, so filter-correctness fixes apply to every
/// implementation at once.
///
/// Instances are single-use per search and must never be cached by Lucene ([Weight#isCacheable] returns
/// false), since the accepted docs depend on the bitmap instance.
///
/// **Bitmap ownership.** The bitmap is retained by reference, not copied. [ImmutableRoaringBitmap] only
/// promises that *this* type exposes no mutators -- a caller may pass a `MutableRoaringBitmap`, which is a
/// subtype -- so the caller must not modify it once it has been handed over. Mutating it during a search
/// changes which documents are accepted midway through traversal and yields results matching neither the old
/// nor the new set. Callers that cannot promise that must pass a detached copy.
///
/// Given that, instances are safe to share across the threads of a single search: the bitmap is only read,
/// and each leaf gets its own iterator.
public abstract class BaseFilterQuery extends Query {
  protected final ImmutableRoaringBitmap _docIds;

  protected BaseFilterQuery(ImmutableRoaringBitmap docIds) {
    _docIds = docIds;
  }

  /// Returns an iterator over the Lucene doc ids of the given leaf whose corresponding Pinot doc ids are
  /// in the bitmap, in increasing Lucene doc id order; or null when the leaf cannot match any document.
  @Nullable
  protected abstract DocIdSetIterator createLeafIterator(LeafReaderContext context)
      throws IOException;

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new ConstantScoreWeight(this, boost) {
      @Override
      @Nullable
      public Scorer scorer(LeafReaderContext context)
          throws IOException {
        DocIdSetIterator iterator = createLeafIterator(context);
        if (iterator == null) {
          return null;
        }
        float constScore = score();
        return new Scorer(this) {
          @Override
          public DocIdSetIterator iterator() {
            return iterator;
          }

          @Override
          public float getMaxScore(int upTo) {
            return constScore;
          }

          @Override
          public float score() {
            return constScore;
          }

          @Override
          public int docID() {
            return iterator.docID();
          }
        };
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }

  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + "(cardinality=" + _docIds.getCardinality() + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    // Identity semantics: the accepted docs depend on the bitmap instance, and these queries are
    // single-use per search (never cached), so structural equality is neither needed nor meaningful
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    BaseFilterQuery that = (BaseFilterQuery) other;
    return _docIds == that._docIds && equalsDocIdSource(that);
  }

  /// Whether `other` resolves Pinot doc ids the same way this query does. Subclasses that read doc ids through
  /// a collaborator must compare it here: two queries over one bitmap that resolve doc ids differently accept
  /// different documents, and Lucene requires unequal queries in that case.
  ///
  /// Called only from [#equals] after the concrete classes have been confirmed identical, so `other` may be
  /// cast to the subclass type without an `instanceof` check.
  protected boolean equalsDocIdSource(BaseFilterQuery other) {
    return true;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() * 31 + System.identityHashCode(_docIds);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }
}
