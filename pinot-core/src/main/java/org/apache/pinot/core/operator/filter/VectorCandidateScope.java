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

import com.google.common.base.Preconditions;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Immutable set of document IDs a vector predicate is allowed to consider as candidates.
///
/// Vector top-K is not monotonic: unlike an ordinary predicate, restricting the corpus changes which documents win
/// the top-K contest, so a document set that defines what the query may see must be applied *before* candidate
/// generation rather than intersected with the result afterwards. That contract is the entire meaning of this type.
/// It deliberately says nothing about where the restriction came from -- callers such as [FilterPlanNode] decide
/// that, today from the segment's queryable-document snapshot.
///
/// Contrast with the optimizer-selected metadata bitmap passed to
/// [VectorSimilarityFilterOperator#setPreFilterBitmap]: that one is optional, because post-filtering an ordinary
/// predicate is already correct and pre-filtering only improves recall.
///
/// The factory owns the single detached copy of the supplied bitmap, so operators can share a scope freely and only
/// allocate again when they intersect it with an independent optional filter.
public final class VectorCandidateScope {
  private final ImmutableRoaringBitmap _requiredDocIds;

  private VectorCandidateScope(ImmutableRoaringBitmap requiredDocIds) {
    // Detach from the caller's bitmap so candidate generation and the outer valid-document AND observe the same
    // document set for the whole query. Copying into array-backed containers keeps per-document `contains` checks
    // cheap, which matters because filtered graph traversal probes this bitmap once per visited node.
    _requiredDocIds = requiredDocIds.toMutableRoaringBitmap().toImmutableRoaringBitmap();
  }

  /// Creates a scope from the document IDs the query is allowed to consider.
  public static VectorCandidateScope of(ImmutableRoaringBitmap requiredDocIds) {
    return new VectorCandidateScope(
        Preconditions.checkNotNull(requiredDocIds, "Required doc IDs must not be null"));
  }

  public ImmutableRoaringBitmap getRequiredDocIds() {
    return _requiredDocIds;
  }

  public int getCardinality() {
    return _requiredDocIds.getCardinality();
  }

  public boolean isEmpty() {
    return _requiredDocIds.isEmpty();
  }
}
