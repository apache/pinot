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
package org.apache.pinot.segment.spi.index.creator;


/**
 * Enumerates the explicit execution modes for vector queries.
 *
 * <p>Each mode describes a specific strategy for combining ANN search, metadata filtering,
 * threshold predicates, and exact refinement. The runtime selects a mode based on backend
 * capabilities, query shape, and filter selectivity, then reports it in explain/debug output.</p>
 *
 * <p>Execution modes are ordered from cheapest/most approximate to most expensive/exact:</p>
 * <ol>
 *   <li>{@link #ANN_TOP_K} - pure ANN, no filter, no rerank</li>
 *   <li>{@link #ANN_TOP_K_WITH_RERANK} - ANN + exact distance rerank</li>
 *   <li>{@link #ANN_THEN_FILTER} - ANN first, then apply metadata filter</li>
 *   <li>{@link #ANN_THEN_FILTER_THEN_RERANK} - ANN, filter, then rerank survivors</li>
 *   <li>{@link #FILTER_THEN_ANN} - apply filter first, then ANN on filtered subset</li>
 *   <li>{@link #ANN_THRESHOLD_SCAN} - ANN candidates + exact threshold refinement</li>
 *   <li>{@link #ANN_THRESHOLD_THEN_FILTER} - ANN candidates + threshold + filter</li>
 *   <li>{@link #EXACT_SCAN} - brute-force scan of all vectors</li>
 * </ol>
 */
public enum VectorExecutionMode {

  /**
   * Pure ANN top-K search with no metadata filter and no exact rerank.
   * This is the default mode for simple VECTOR_SIMILARITY queries.
   */
  ANN_TOP_K("ANN top-K search"),

  /**
   * ANN top-K search followed by exact distance reranking from the forward index.
   * Used when vectorExactRerank=true (or backend default) and no metadata filter is present.
   */
  ANN_TOP_K_WITH_RERANK("ANN top-K with exact rerank"),

  /**
   * ANN search first, then metadata filter applied to ANN results.
   * Used when a metadata filter is present but filter selectivity is not highly restrictive.
   * Some valid results may be lost if the filter removes too many ANN candidates.
   */
  ANN_THEN_FILTER("ANN search, then metadata filter"),

  /**
   * ANN search, then metadata filter, then exact rerank of surviving candidates.
   * Combines filtered ANN with exact refinement for better accuracy.
   */
  ANN_THEN_FILTER_THEN_RERANK("ANN search, then metadata filter, then exact rerank"),

  /**
   * Metadata filter applied first to produce a candidate set, then vector search on the
   * filtered subset.
   */
  FILTER_THEN_ANN("Metadata filter, then ANN search on filtered subset"),

  /**
   * ANN candidate generation followed by exact distance threshold refinement.
   * Used for threshold/radius queries when no backend supports native radius search.
   */
  ANN_THRESHOLD_SCAN("ANN candidates, then exact threshold refinement"),

  /**
   * ANN candidate generation, exact threshold refinement, then metadata filter.
   * Used for combined threshold + filter queries.
   */
  ANN_THRESHOLD_THEN_FILTER("ANN candidates, then threshold refinement, then metadata filter"),

  /**
   * Brute-force exact scan of all vectors in the segment.
   * Used as a fallback when no ANN index is available, or when the planner determines
   * that exact scan is more appropriate (e.g., very small segment, highly selective filter).
   */
  EXACT_SCAN("Exact brute-force vector scan");

  private final String _description;

  VectorExecutionMode(String description) {
    _description = description;
  }

  public String getDescription() {
    return _description;
  }

  /**
   * Returns true if this mode involves ANN (approximate) search.
   */
  public boolean isApproximate() {
    return this != EXACT_SCAN;
  }

  /**
   * Returns true if this mode applies a metadata filter.
   */
  public boolean hasMetadataFilter() {
    switch (this) {
      case ANN_THEN_FILTER:
      case ANN_THEN_FILTER_THEN_RERANK:
      case FILTER_THEN_ANN:
      case ANN_THRESHOLD_THEN_FILTER:
        return true;
      default:
        return false;
    }
  }

  /**
   * Returns true if this mode involves exact rerank or threshold refinement.
   */
  public boolean hasExactRefinement() {
    switch (this) {
      case ANN_TOP_K_WITH_RERANK:
      case ANN_THEN_FILTER_THEN_RERANK:
      case ANN_THRESHOLD_SCAN:
      case ANN_THRESHOLD_THEN_FILTER:
      case EXACT_SCAN:
        return true;
      default:
        return false;
    }
  }

  /**
   * Returns true if this mode involves threshold/radius semantics.
   */
  public boolean isThresholdMode() {
    return this == ANN_THRESHOLD_SCAN || this == ANN_THRESHOLD_THEN_FILTER;
  }
}
