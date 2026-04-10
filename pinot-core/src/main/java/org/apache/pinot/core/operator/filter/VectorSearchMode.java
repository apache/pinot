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


/**
 * Describes how the vector ANN search interacts with non-vector filter predicates.
 *
 * <p>This enum is used in explain output and operator-level logging to indicate which
 * execution path was chosen for a given query.</p>
 */
public enum VectorSearchMode {

  /**
   * ANN search runs first without any filter, then results are intersected with the
   * filter bitmap. This is the default mode and the only mode available before
   * filter-aware ANN was introduced.
   */
  POST_FILTER_ANN("ANN search first, then intersect with filter"),

  /**
   * The filter bitmap is computed first and passed into the ANN search so that only
   * documents matching the filter are considered as candidates. This improves recall
   * for selective filters.
   */
  FILTER_THEN_ANN("Apply filter first, then ANN on filtered docs"),

  /**
   * Brute-force scan of all (or filtered) documents using exact distance computation.
   * Used when the index is not available or when the filter is extremely selective.
   */
  EXACT_SCAN("Brute-force scan of all/filtered docs");

  private final String _description;

  VectorSearchMode(String description) {
    _description = description;
  }

  /**
   * Returns a human-readable description of this search mode.
   */
  public String getDescription() {
    return _description;
  }
}
